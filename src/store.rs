//! A durable, keep-until-acked FIFO queue — one logical queue per recipient key.
//!
//! Backed by redb (pure Rust, ACID) on disk, or an in-memory backend for
//! ephemeral use (tests, and the bus with no `--db`). Same code path either way.
//!
//! Values are opaque bytes, so the same store serves the bus (message
//! envelopes) and the agent's outbound queue. Ordering is a global monotonic
//! sequence, so a prefix range scan over a recipient yields FIFO. A message
//! stays until [`Store::ack`]; redelivery after a crash is safe because the
//! receiver dedupes by `msg_id`.
//!
//! redb's API is synchronous; each call runs on a blocking thread so the surface
//! the rest of the async code sees is `async`. When Turso's pure-Rust SDK
//! matures this module is the single seam to swap.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition, TableError};

const MESSAGES: TableDefinition<&str, &[u8]> = TableDefinition::new("messages");
const META: TableDefinition<&str, u64> = TableDefinition::new("meta");
const NEXT_SEQ: &str = "next_seq";

#[derive(Clone)]
pub struct Store {
    db: Arc<Database>,
}

/// NUL separates the recipient from the zero-padded seq, so a range over
/// `"{recipient}\0" .. "{recipient}\u{1}"` selects exactly that recipient's
/// messages, in insertion order.
fn msg_key(recipient: &str, seq: u64) -> String {
    format!("{recipient}\u{0}{seq:020}")
}

fn recipient_bounds(recipient: &str) -> (String, String) {
    (format!("{recipient}\u{0}"), format!("{recipient}\u{1}"))
}

impl Store {
    /// On-disk durable store (created if absent).
    pub fn on_disk(path: &Path) -> Result<Self> {
        let db = Database::create(path)
            .with_context(|| format!("opening store at {}", path.display()))?;
        Ok(Self { db: Arc::new(db) })
    }

    /// Ephemeral in-memory store — same API, nothing persists.
    pub fn in_memory() -> Result<Self> {
        let db = Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .context("creating in-memory store")?;
        Ok(Self { db: Arc::new(db) })
    }

    /// Append `value` to `recipient`'s queue; returns the ack key.
    pub async fn enqueue(&self, recipient: String, value: Vec<u8>) -> Result<String> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let wtx = db.begin_write()?;
            let key;
            {
                let mut meta = wtx.open_table(META)?;
                let seq = meta.get(NEXT_SEQ)?.map(|v| v.value()).unwrap_or(0);
                meta.insert(NEXT_SEQ, seq + 1)?;
                key = msg_key(&recipient, seq);
                let mut msgs = wtx.open_table(MESSAGES)?;
                msgs.insert(key.as_str(), value.as_slice())?;
            }
            wtx.commit()?;
            Ok::<_, anyhow::Error>(key)
        })
        .await?
    }

    /// The oldest un-acked `(key, value)` for `recipient`, without removing it.
    pub async fn peek_oldest(&self, recipient: String) -> Result<Option<(String, Vec<u8>)>> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let rtx = db.begin_read()?;
            let msgs = match rtx.open_table(MESSAGES) {
                Ok(t) => t,
                Err(TableError::TableDoesNotExist(_)) => return Ok(None),
                Err(e) => return Err(e.into()),
            };
            let (lo, hi) = recipient_bounds(&recipient);
            match msgs.range(lo.as_str()..hi.as_str())?.next() {
                Some(entry) => {
                    let (k, v) = entry?;
                    Ok(Some((k.value().to_string(), v.value().to_vec())))
                }
                None => Ok(None),
            }
        })
        .await?
    }

    /// Remove an acked message by key. Idempotent (removing an absent key is ok).
    pub async fn ack(&self, key: String) -> Result<()> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let wtx = db.begin_write()?;
            {
                let mut msgs = wtx.open_table(MESSAGES)?;
                msgs.remove(key.as_str())?;
            }
            wtx.commit()?;
            Ok::<_, anyhow::Error>(())
        })
        .await?
    }

    /// Count of un-acked messages for `recipient`.
    pub async fn depth(&self, recipient: String) -> Result<usize> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let rtx = db.begin_read()?;
            let msgs = match rtx.open_table(MESSAGES) {
                Ok(t) => t,
                Err(TableError::TableDoesNotExist(_)) => return Ok(0),
                Err(e) => return Err(e.into()),
            };
            let (lo, hi) = recipient_bounds(&recipient);
            Ok::<_, anyhow::Error>(msgs.range(lo.as_str()..hi.as_str())?.count())
        })
        .await?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    #[tokio::test]
    async fn enqueue_peek_ack_roundtrip() {
        let s = Store::in_memory().unwrap();
        let key = s.enqueue("alice".into(), b("hi")).await.unwrap();
        let (k, v) = s.peek_oldest("alice".into()).await.unwrap().unwrap();
        assert_eq!(k, key);
        assert_eq!(v, b("hi"));
        // peek does not remove
        assert!(s.peek_oldest("alice".into()).await.unwrap().is_some());
        s.ack(key).await.unwrap();
        assert!(s.peek_oldest("alice".into()).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn fifo_order_within_recipient() {
        let s = Store::in_memory().unwrap();
        for m in ["one", "two", "three"] {
            s.enqueue("bob".into(), b(m)).await.unwrap();
        }
        for expected in ["one", "two", "three"] {
            let (k, v) = s.peek_oldest("bob".into()).await.unwrap().unwrap();
            assert_eq!(v, b(expected));
            s.ack(k).await.unwrap();
        }
        assert!(s.peek_oldest("bob".into()).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn recipients_are_isolated() {
        let s = Store::in_memory().unwrap();
        s.enqueue("alice".into(), b("for-alice")).await.unwrap();
        s.enqueue("bob".into(), b("for-bob")).await.unwrap();
        assert_eq!(s.depth("alice".into()).await.unwrap(), 1);
        assert_eq!(s.depth("bob".into()).await.unwrap(), 1);
        assert_eq!(
            s.peek_oldest("bob".into()).await.unwrap().unwrap().1,
            b("for-bob")
        );
    }

    #[tokio::test]
    async fn unacked_message_survives_reopen() {
        // The whole point: a message persists across a bus restart until acked.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.redb");
        let key = {
            let s = Store::on_disk(&path).unwrap();
            s.enqueue("alice".into(), b("durable")).await.unwrap()
        }; // Store (and its Database) dropped — simulates a restart
        let s2 = Store::on_disk(&path).unwrap();
        let (k, v) = s2.peek_oldest("alice".into()).await.unwrap().unwrap();
        assert_eq!(k, key);
        assert_eq!(v, b("durable"));
        // and once acked, it's gone across another reopen
        s2.ack(k).await.unwrap();
        drop(s2);
        let s3 = Store::on_disk(&path).unwrap();
        assert!(s3.peek_oldest("alice".into()).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn ack_is_idempotent() {
        let s = Store::in_memory().unwrap();
        let key = s.enqueue("alice".into(), b("x")).await.unwrap();
        s.ack(key.clone()).await.unwrap();
        s.ack(key).await.unwrap(); // no error the second time
        s.ack("never-existed".into()).await.unwrap();
    }
}
