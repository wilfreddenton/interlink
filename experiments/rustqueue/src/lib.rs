//! Isolated evaluation of rustqueue as Interlink's notification scheduler.

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use rustqueue::engine::error::RustQueueError;
use rustqueue::engine::models::{BackoffStrategy, Job, JobId};
use rustqueue::engine::queue::{JobOptions, QueueManager};
use rustqueue::storage::RedbStorage;
use serde_json::json;

pub const WAKE_QUEUE: &str = "interlink-wakes";

pub struct Notifications {
    engine: QueueManager,
    stall_ms: u64,
    retry_ms: u64,
}

impl Notifications {
    pub fn open(path: &Path, stall_ms: u64, retry_ms: u64) -> Result<Self> {
        Ok(Self {
            engine: QueueManager::new(Arc::new(RedbStorage::new(path)?)),
            stall_ms,
            retry_ms,
        })
    }

    /// The mailbox must already be durable before requesting a wake-up.
    pub async fn ensure_wake(&self, session: &str) -> Result<bool> {
        match self
            .engine
            .push(
                WAKE_QUEUE,
                "fetch-mailbox",
                json!({ "session": session }),
                Some(JobOptions {
                    unique_key: Some(session.into()),
                    // Finite even at this value. A production integration would
                    // still need reconciliation after retry exhaustion.
                    max_attempts: Some(u32::MAX),
                    backoff: Some(BackoffStrategy::Fixed),
                    backoff_delay_ms: Some(self.retry_ms),
                    remove_on_complete: Some(true),
                    ..Default::default()
                }),
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(RustQueueError::DuplicateKey(_)) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    /// A successful host handoff leaves the job active until receiver acknowledgement.
    pub async fn next_wake(&self) -> Result<Option<Job>> {
        self.engine.detect_stalls(self.stall_ms).await?;
        self.engine.promote_delayed_jobs().await?;
        Ok(self.engine.pull(WAKE_QUEUE, 1).await?.pop())
    }

    pub async fn host_failed(&self, id: JobId) -> Result<()> {
        self.engine.fail(id, "host rejected notification").await?;
        Ok(())
    }

    /// This delegates the upstream semantics unchanged so probes can expose gaps.
    pub async fn receiver_acknowledged(&self, id: JobId) -> Result<()> {
        self.engine.ack(id, None).await?;
        Ok(())
    }
}
