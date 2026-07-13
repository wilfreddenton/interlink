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
use serde::{Deserialize, Serialize};

const MESSAGES: TableDefinition<&str, &[u8]> = TableDefinition::new("messages");
const META: TableDefinition<&str, u64> = TableDefinition::new("meta");
const NEXT_SEQ: &str = "next_seq";

// The message log lives in the same file as the queue but in its own tables.
// LOG holds one record per msg_id (the current state); LOG_INDEX orders records
// per peer by timestamp so a prefix scan yields a conversation in time order.
const LOG: TableDefinition<&str, &[u8]> = TableDefinition::new("log");
const LOG_INDEX: TableDefinition<&str, &str> = TableDefinition::new("log_index");

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Dir {
    In,
    Out,
}

/// One entry in the local conversation log. `text` may be `None` for records
/// where only the fact of a message (not its body) needs keeping.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogRecord {
    pub msg_id: String,
    pub dir: Dir,
    pub peer: String,
    pub text: Option<String>,
    pub ts: u64,
    pub state: String,
}

fn log_index_key(peer: &str, ts: u64, msg_id: &str) -> String {
    format!("{peer}\u{0}{ts:020}\u{0}{msg_id}")
}

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

    /// Every un-acked `(key, value)` for `recipient`, in FIFO order. Used to
    /// inspect a queue (e.g. list what's still pending in the agent's outbox).
    pub async fn list(&self, recipient: String) -> Result<Vec<(String, Vec<u8>)>> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let rtx = db.begin_read()?;
            let msgs = match rtx.open_table(MESSAGES) {
                Ok(t) => t,
                Err(TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(e) => return Err(e.into()),
            };
            let (lo, hi) = recipient_bounds(&recipient);
            let mut out = Vec::new();
            for entry in msgs.range(lo.as_str()..hi.as_str())? {
                let (k, v) = entry?;
                out.push((k.value().to_string(), v.value().to_vec()));
            }
            Ok::<_, anyhow::Error>(out)
        })
        .await?
    }

    /// Insert or overwrite a log record (keyed by msg_id) and its per-peer index.
    pub async fn log_put(&self, rec: LogRecord) -> Result<()> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let idx_key = log_index_key(&rec.peer, rec.ts, &rec.msg_id);
            let bytes = serde_json::to_vec(&rec)?;
            let wtx = db.begin_write()?;
            {
                let mut log = wtx.open_table(LOG)?;
                log.insert(rec.msg_id.as_str(), bytes.as_slice())?;
                let mut idx = wtx.open_table(LOG_INDEX)?;
                idx.insert(idx_key.as_str(), rec.msg_id.as_str())?;
            }
            wtx.commit()?;
            Ok::<_, anyhow::Error>(())
        })
        .await?
    }

    /// Update the `state` of a logged message. No-op if the msg_id is unknown.
    pub async fn log_set_state(&self, msg_id: String, state: String) -> Result<()> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let wtx = db.begin_write()?;
            {
                let mut log = wtx.open_table(LOG)?;
                let current = log.get(msg_id.as_str())?.map(|g| g.value().to_vec());
                if let Some(cur) = current {
                    let mut rec: LogRecord = serde_json::from_slice(&cur)?;
                    rec.state = state;
                    let bytes = serde_json::to_vec(&rec)?;
                    log.insert(msg_id.as_str(), bytes.as_slice())?;
                }
            }
            wtx.commit()?;
            Ok::<_, anyhow::Error>(())
        })
        .await?
    }

    /// The log record for a single msg_id, if any.
    pub async fn log_get(&self, msg_id: String) -> Result<Option<LogRecord>> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let rtx = db.begin_read()?;
            let log = match rtx.open_table(LOG) {
                Ok(t) => t,
                Err(TableError::TableDoesNotExist(_)) => return Ok(None),
                Err(e) => return Err(e.into()),
            };
            match log.get(msg_id.as_str())? {
                Some(g) => Ok(Some(serde_json::from_slice(g.value())?)),
                None => Ok(None),
            }
        })
        .await?
    }

    /// The most recent `limit` log records for `peer`, in chronological order.
    pub async fn log_by_peer(&self, peer: String, limit: usize) -> Result<Vec<LogRecord>> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let rtx = db.begin_read()?;
            let idx = match rtx.open_table(LOG_INDEX) {
                Ok(t) => t,
                Err(TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(e) => return Err(e.into()),
            };
            let log = rtx.open_table(LOG)?;
            let (lo, hi) = recipient_bounds(&peer);
            let mut ids: Vec<String> = Vec::new();
            for entry in idx.range(lo.as_str()..hi.as_str())? {
                let (_k, v) = entry?;
                ids.push(v.value().to_string());
            }
            if ids.len() > limit {
                ids = ids.split_off(ids.len() - limit);
            }
            let mut out = Vec::with_capacity(ids.len());
            for id in ids {
                if let Some(g) = log.get(id.as_str())? {
                    out.push(serde_json::from_slice(g.value())?);
                }
            }
            Ok::<_, anyhow::Error>(out)
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

    fn rec(msg_id: &str, dir: Dir, peer: &str, text: Option<&str>, ts: u64) -> LogRecord {
        LogRecord {
            msg_id: msg_id.into(),
            dir,
            peer: peer.into(),
            text: text.map(str::to_string),
            ts,
            state: "pending".into(),
        }
    }

    #[tokio::test]
    async fn log_put_get_and_set_state() {
        let s = Store::in_memory().unwrap();
        s.log_put(rec("m1", Dir::Out, "bob", Some("hi bob"), 10))
            .await
            .unwrap();
        let got = s.log_get("m1".into()).await.unwrap().unwrap();
        assert_eq!(got.peer, "bob");
        assert_eq!(got.state, "pending");
        s.log_set_state("m1".into(), "sent".into()).await.unwrap();
        assert_eq!(s.log_get("m1".into()).await.unwrap().unwrap().state, "sent");
        // unknown id is a no-op, not an error
        s.log_set_state("nope".into(), "sent".into()).await.unwrap();
        assert!(s.log_get("nope".into()).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn log_by_peer_is_chronological_and_limited() {
        let s = Store::in_memory().unwrap();
        s.log_put(rec("a", Dir::Out, "bob", Some("1"), 1))
            .await
            .unwrap();
        s.log_put(rec("b", Dir::In, "bob", Some("2"), 2))
            .await
            .unwrap();
        s.log_put(rec("c", Dir::Out, "bob", Some("3"), 3))
            .await
            .unwrap();
        s.log_put(rec("z", Dir::In, "carol", Some("other"), 5))
            .await
            .unwrap();
        let hist = s.log_by_peer("bob".into(), 2).await.unwrap();
        assert_eq!(
            hist.iter().map(|r| r.msg_id.as_str()).collect::<Vec<_>>(),
            vec!["b", "c"],
            "newest 2, in time order"
        );
        // carol's history is isolated from bob's
        assert_eq!(s.log_by_peer("carol".into(), 10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn scoped_body_is_not_persisted() {
        let s = Store::in_memory().unwrap();
        s.log_put(rec("s1", Dir::In, "carol", None, 7))
            .await
            .unwrap();
        let got = s.log_get("s1".into()).await.unwrap().unwrap();
        assert!(got.text.is_none(), "withheld body must stay withheld");
    }

    #[tokio::test]
    async fn list_returns_all_queued_for_recipient() {
        let s = Store::in_memory().unwrap();
        s.enqueue("outbox".into(), b("j1")).await.unwrap();
        s.enqueue("outbox".into(), b("j2")).await.unwrap();
        let all = s.list("outbox".into()).await.unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].1, b("j1"));
        assert_eq!(all[1].1, b("j2"));
    }
}
