//! Recoverable host-delivery failures, persisted before releasing a bus message.

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
        let _lock = lock(&self.path.with_extension("lock"))?;
        let mut records = self.list()?;
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
