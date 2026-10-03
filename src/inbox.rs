//! A restart-safe JSONL inbox with a single reader and atomic cursor updates.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Result;
use fs2::FileExt;
use tokio::time::{Instant, sleep};

use crate::state::{atomic_write, lock};

pub const RENEW_AFTER: Duration = Duration::from_secs(3000);
pub const RENEW_NOTICE: &str = "[interlink listener renewal] No peer message arrived. End this turn without tools or a user-facing reply so the Stop hook can renew the inbox listener.";

pub struct Inbox {
    path: PathBuf,
}

pub struct Batch {
    pub lines: Vec<String>,
    end: u64,
}

pub enum Wake {
    Messages(Batch),
    Renew,
}

impl Inbox {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            path: path.to_owned(),
        })
    }

    pub fn listener_lock(&self) -> Result<Option<File>> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(self.path.with_extension("lock"))?;
        match file.try_lock_exclusive() {
            Ok(()) => Ok(Some(file)),
            Err(e) if e.kind() == ErrorKind::WouldBlock => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn append(&self, record: &str) -> Result<()> {
        let _lock = lock(&self.path.with_extension("io-lock"))?;
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&self.path)?;
        let len = file.metadata()?.len();
        if len > 0 {
            file.seek(SeekFrom::End(-1))?;
            let mut last = [0];
            file.read_exact(&mut last)?;
            if last[0] != b'\n' {
                // A partial append was never acknowledged to the bus. Remove its
                // fragment before a redelivery appends the complete record.
                let mut end = len;
                let mut buffer = [0; 4096];
                loop {
                    let start = end.saturating_sub(buffer.len() as u64);
                    let count = (end - start) as usize;
                    file.seek(SeekFrom::Start(start))?;
                    file.read_exact(&mut buffer[..count])?;
                    if let Some(index) = buffer[..count].iter().rposition(|b| *b == b'\n') {
                        file.set_len(start + index as u64 + 1)?;
                        break;
                    }
                    if start == 0 {
                        file.set_len(0)?;
                        break;
                    }
                    end = start;
                }
            }
        }
        file.write_all(format!("{record}\n").as_bytes())?;
        file.sync_data()?;
        Ok(())
    }

    pub fn read_batch(&self) -> Result<Batch> {
        let _lock = lock(&self.path.with_extension("io-lock"))?;
        let cursor = match std::fs::read_to_string(self.path.with_extension("cursor")) {
            Ok(raw) => raw.trim().parse::<u64>()?,
            Err(e) if e.kind() == ErrorKind::NotFound => 0,
            Err(e) => return Err(e.into()),
        };
        let file = File::open(&self.path)?;
        // Older versions truncated inboxes on startup. Preserve migration behavior.
        let start = if cursor > file.metadata()?.len() {
            0
        } else {
            cursor
        };
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(start))?;
        let mut batch = Batch {
            lines: Vec::new(),
            end: start,
        };
        while batch.lines.len() < 64 && batch.end - start < 256 * 1024 {
            let mut line = String::new();
            let bytes = reader.read_line(&mut line)?;
            // A crash during append can leave a partial record. Never consume it.
            if bytes == 0 || !line.ends_with('\n') {
                break;
            }
            batch.end += bytes as u64;
            batch.lines.push(line);
        }
        Ok(batch)
    }

    pub fn commit(&self, batch: &Batch) -> Result<()> {
        atomic_write(
            &self.path.with_extension("cursor"),
            batch.end.to_string().as_bytes(),
        )
    }

    pub async fn wait(&self, renew_after: Duration) -> Result<Wake> {
        let deadline = Instant::now() + renew_after;
        loop {
            let batch = self.read_batch()?;
            if !batch.lines.is_empty() {
                return Ok(Wake::Messages(batch));
            }
            if Instant::now() >= deadline {
                return Ok(Wake::Renew);
            }
            sleep(Duration::from_millis(400)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restart_preserves_unread_records_and_cursor_tracks_only_delivered_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("inbox.jsonl");
        let first = Inbox::open(&path).unwrap();
        first.append("first").unwrap();
        let restarted = Inbox::open(&path).unwrap();
        let batch = restarted.read_batch().unwrap();
        assert_eq!(batch.lines, ["first\n"]);
        first.append("second").unwrap();
        restarted.commit(&batch).unwrap();
        let batch = Inbox::open(&path).unwrap().read_batch().unwrap();
        assert_eq!(batch.lines, ["second\n"]);
    }

    #[test]
    fn partial_record_is_not_consumed_and_duplicate_listener_is_silent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("inbox.jsonl");
        let inbox = Inbox::open(&path).unwrap();
        let guard = inbox.listener_lock().unwrap().unwrap();
        assert!(inbox.listener_lock().unwrap().is_none());
        std::fs::write(&path, "part").unwrap();
        assert!(inbox.read_batch().unwrap().lines.is_empty());
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"ial\n")
            .unwrap();
        assert_eq!(inbox.read_batch().unwrap().lines, ["partial\n"]);
        drop(guard);
        assert!(inbox.listener_lock().unwrap().is_some());
    }

    #[test]
    fn append_repairs_incomplete_tail_without_losing_complete_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("inbox.jsonl");
        let inbox = Inbox::open(&path).unwrap();
        inbox.append("complete").unwrap();
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"partial")
            .unwrap();
        inbox.append("redelivered").unwrap();
        assert_eq!(
            inbox.read_batch().unwrap().lines,
            ["complete\n", "redelivered\n"]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn long_idle_period_requests_renewal_instead_of_disarming() {
        let dir = tempfile::tempdir().unwrap();
        let inbox = Inbox::open(&dir.path().join("inbox.jsonl")).unwrap();
        assert!(matches!(
            inbox.wait(RENEW_AFTER).await.unwrap(),
            Wake::Renew
        ));
        inbox.append("after renewal").unwrap();
        assert!(matches!(
            inbox.wait(RENEW_AFTER).await.unwrap(),
            Wake::Messages(_)
        ));
    }
}
