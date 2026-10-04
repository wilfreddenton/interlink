//! Recoverable notice diagnostics and legacy full-message delivery failures.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use crate::identity::mint_session_id;
use crate::state::{atomic_write, lock};

#[derive(Clone, Serialize, Deserialize)]
pub struct FailedDelivery {
    pub id: String,
    pub msg_id: String,
    pub sender: String,
    pub text: String,
    pub reason: String,
    #[serde(default)]
    pub notice: bool,
}

pub struct FailedDeliveries {
    path: PathBuf,
}

impl FailedDeliveries {
    pub fn new(path: &Path) -> Self {
        Self {
            path: path.to_owned(),
        }
    }

    pub fn list(&self) -> Result<Vec<FailedDelivery>> {
        match std::fs::read(&self.path) {
            Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(e.into()),
        }
    }

    pub fn retain(&self, msg_id: &str, sender: &str, text: &str, reason: &str) -> Result<()> {
        self.retain_entry(msg_id, sender, text, reason, false)
    }

    pub fn retain_notice(&self, msg_id: &str, text: &str, reason: &str) -> Result<()> {
        self.retain_entry(msg_id, "Interlink", text, reason, true)
    }

    pub fn clear_notices(&self) -> Result<()> {
        let _lock = lock(&self.path.with_extension("lock"))?;
        let mut records = self.list()?;
        records.retain(|r| !r.notice);
        atomic_write(&self.path, &serde_json::to_vec(&records)?)
    }

    fn retain_entry(
        &self,
        msg_id: &str,
        sender: &str,
        text: &str,
        reason: &str,
        notice: bool,
    ) -> Result<()> {
        let _lock = lock(&self.path.with_extension("lock"))?;
        let mut records = self.list()?;
        // Repeated wake-up failures need one diagnostic slot, not an unbounded
        // sequence of stale notices. Legacy full messages must remain recoverable.
        if notice {
            records.retain(|r| !r.notice);
        }
        if records
            .iter()
            .any(|r| r.msg_id == msg_id && r.sender == sender)
        {
            return Ok(());
        }
        if records.len() >= 64 {
            bail!("failed-delivery storage is full; read and discard recovered entries");
        }
        records.push(FailedDelivery {
            id: mint_session_id()?,
            msg_id: msg_id.into(),
            sender: sender.into(),
            text: text.into(),
            reason: reason.into(),
            notice,
        });
        atomic_write(&self.path, &serde_json::to_vec(&records)?)
    }

    pub fn remove(&self, id: &str) -> Result<()> {
        let _lock = lock(&self.path.with_extension("lock"))?;
        let mut records = self.list()?;
        records.retain(|r| r.id != id);
        atomic_write(&self.path, &serde_json::to_vec(&records)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notice_retries_use_one_slot_and_preserve_legacy_messages() {
        let dir = tempfile::tempdir().unwrap();
        let store = FailedDeliveries::new(&dir.path().join("failed.json"));
        store
            .retain("legacy", "peer", "full body", "offline")
            .unwrap();
        for index in 0..70 {
            store
                .retain_notice(&index.to_string(), "fetch mailbox", "offline")
                .unwrap();
        }
        let records = store.list().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].text, "full body");
        assert_eq!(records[1].msg_id, "69");
        store.clear_notices().unwrap();
        assert_eq!(store.list().unwrap().len(), 1);
    }
}
