use std::env;
use std::path::Path;
use std::process::{Command, exit};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::{TimeDelta, Utc};
use interlink_rustqueue_prototype::{Notifications, WAKE_QUEUE};
use rustqueue::engine::error::RustQueueError;
use rustqueue::engine::models::{BackoffStrategy, Job, JobState};
use rustqueue::engine::queue::{JobOptions, QueueManager};
use rustqueue::storage::{RedbStorage, StorageBackend};
use serde_json::json;
use tempfile::tempdir;
use tokio::time::{sleep, timeout};

fn engine(path: &Path) -> Result<(Arc<RedbStorage>, QueueManager)> {
    let storage = Arc::new(RedbStorage::new(path)?);
    let engine = QueueManager::new(storage.clone());
    Ok((storage, engine))
}

fn options(key: &str) -> JobOptions {
    JobOptions {
        unique_key: Some(key.into()),
        backoff: Some(BackoffStrategy::Fixed),
        backoff_delay_ms: Some(0),
        ..Default::default()
    }
}

async fn expire(storage: &RedbStorage, job: &Job) -> Result<()> {
    // Only the test clock is changed. Recovery still uses the real queue APIs.
    let mut old = storage.get_job(job.id).await?.unwrap();
    old.started_at = Some(Utc::now() - TimeDelta::seconds(60));
    old.last_heartbeat = None;
    storage.update_job(&old).await
}

async fn eventually_next(queue: &Notifications) -> Result<Job> {
    timeout(Duration::from_secs(5), async {
        loop {
            if let Some(job) = queue.next_wake().await? {
                return Ok(job);
            }
            sleep(Duration::from_millis(2)).await;
        }
    })
    .await?
}

#[tokio::test]
async fn lost_host_notice_and_dropped_fetch_response_recover_after_reopen() -> Result<()> {
    // Host names label identical simulated handoffs, not real CLI integrations.
    for host in ["claude-channel", "claude-stop", "codex-queue"] {
        let dir = tempdir()?;
        let path = dir.path().join("queue.redb");
        let queue = Notifications::open(&path, 5, 0)?;
        assert!(queue.ensure_wake(host).await?);
        assert!(!queue.ensure_wake(host).await?);
        let sent = eventually_next(&queue).await?;
        drop(queue);

        let queue = Notifications::open(&path, 5, 0)?;
        let resent = eventually_next(&queue).await?;
        assert_eq!(resent.id, sent.id);
        assert!(resent.attempt > sent.attempt);

        // Fetch response disappears: no acknowledgement was submitted.
        let after_dropped_response = eventually_next(&queue).await?;
        assert_eq!(after_dropped_response.id, sent.id);
        queue.receiver_acknowledged(sent.id).await?;
        assert!(queue.next_wake().await?.is_none());
        drop(queue);
        assert!(
            Notifications::open(&path, 5, 0)?
                .next_wake()
                .await?
                .is_none()
        );
    }
    Ok(())
}

#[tokio::test]
async fn host_failure_retries_and_new_session_can_progress() -> Result<()> {
    let dir = tempdir()?;
    let queue = Notifications::open(&dir.path().join("queue.redb"), 60_000, 0)?;
    queue.ensure_wake("one").await?;
    let first = queue.next_wake().await?.unwrap();
    queue.host_failed(first.id).await?;
    let retry = queue.next_wake().await?.unwrap();
    assert_eq!(retry.id, first.id);
    queue.ensure_wake("two").await?;
    assert_ne!(queue.next_wake().await?.unwrap().id, first.id);
    Ok(())
}

#[tokio::test]
async fn retry_backoff_delays_redelivery() -> Result<()> {
    let dir = tempdir()?;
    let queue = Notifications::open(&dir.path().join("queue.redb"), 60_000, 60_000)?;
    queue.ensure_wake("one").await?;
    let first = queue.next_wake().await?.unwrap();
    queue.host_failed(first.id).await?;
    assert!(queue.next_wake().await?.is_none());
    Ok(())
}

#[tokio::test]
async fn limitation_history_ack_requires_active_job_and_ack_is_not_idempotent() -> Result<()> {
    let dir = tempdir()?;
    let (_, queue) = engine(&dir.path().join("queue.redb"))?;
    let id = queue.push(WAKE_QUEUE, "message", json!({}), None).await?;
    assert!(matches!(
        queue.ack(id, None).await,
        Err(RustQueueError::InvalidState { .. })
    ));
    queue.pull(WAKE_QUEUE, 1).await?;
    queue.ack(id, None).await?;
    assert!(matches!(
        queue.ack(id, None).await,
        Err(RustQueueError::InvalidState { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn limitation_completed_message_can_be_enqueued_again_with_same_unique_key() -> Result<()> {
    let dir = tempdir()?;
    let (_, queue) = engine(&dir.path().join("queue.redb"))?;
    let first = queue
        .push(
            WAKE_QUEUE,
            "message",
            json!({}),
            Some(options("sender/message")),
        )
        .await?;
    queue.pull(WAKE_QUEUE, 1).await?;
    queue.ack(first, None).await?;
    let replay = queue
        .push(
            WAKE_QUEUE,
            "message",
            json!({}),
            Some(options("sender/message")),
        )
        .await?;
    assert_ne!(first, replay);
    assert_eq!(queue.pull(WAKE_QUEUE, 1).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn limitation_stale_attempt_can_acknowledge_new_attempt() -> Result<()> {
    let dir = tempdir()?;
    let (storage, queue) = engine(&dir.path().join("queue.redb"))?;
    queue
        .push(WAKE_QUEUE, "wake", json!({}), Some(options("session")))
        .await?;
    let first = queue.pull(WAKE_QUEUE, 1).await?.pop().unwrap();
    expire(&storage, &first).await?;
    assert_eq!(queue.detect_stalls(1).await?, 1);
    let second = queue.pull(WAKE_QUEUE, 1).await?.pop().unwrap();
    assert!(second.attempt > first.attempt);
    queue.ack(first.id, None).await?;
    assert_eq!(
        queue.get_job(second.id).await?.unwrap().state,
        JobState::Completed
    );
    Ok(())
}

#[tokio::test]
async fn limitation_active_progress_cannot_be_cancelled_when_result_supersedes_it() -> Result<()> {
    let dir = tempdir()?;
    let (_, queue) = engine(&dir.path().join("queue.redb"))?;
    let id = queue.push(WAKE_QUEUE, "progress", json!({}), None).await?;
    queue.pull(WAKE_QUEUE, 1).await?;
    assert!(matches!(
        queue.cancel(id).await,
        Err(RustQueueError::InvalidState { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn limitation_default_retry_budget_eventually_stops_waking_unread_mailbox() -> Result<()> {
    let dir = tempdir()?;
    let (storage, queue) = engine(&dir.path().join("queue.redb"))?;
    let id = queue
        .push(WAKE_QUEUE, "wake", json!({}), Some(options("session")))
        .await?;
    for _ in 0..3 {
        let job = queue.pull(WAKE_QUEUE, 1).await?.pop().unwrap();
        expire(&storage, &job).await?;
        assert_eq!(queue.detect_stalls(1).await?, 1);
    }
    assert_eq!(queue.get_job(id).await?.unwrap().state, JobState::Dlq);
    assert!(queue.pull(WAKE_QUEUE, 1).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn limitation_arrival_between_fetch_and_ack_needs_mailbox_reconciliation() -> Result<()> {
    let dir = tempdir()?;
    let queue = Notifications::open(&dir.path().join("queue.redb"), 60_000, 0)?;
    queue.ensure_wake("session").await?;
    let first = queue.next_wake().await?.unwrap();
    // A new message arrives after the receiver fetched the original batch.
    assert!(!queue.ensure_wake("session").await?);
    queue.receiver_acknowledged(first.id).await?;
    assert!(queue.next_wake().await?.is_none());
    // The library cannot know a second message remains unread.
    assert!(queue.ensure_wake("session").await?);
    assert!(queue.next_wake().await?.is_some());
    Ok(())
}

#[test]
fn limitation_database_requires_one_owner() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("queue.redb");
    let first = RedbStorage::new(&path)?;
    assert!(RedbStorage::new(&path).is_err());
    drop(first);
    assert!(RedbStorage::new(&path).is_ok());
    Ok(())
}

#[tokio::test]
async fn crash_child() -> Result<()> {
    let Some(path) = env::var_os("INTERLINK_RUSTQUEUE_CRASH_DB") else {
        return Ok(());
    };
    let queue = Notifications::open(Path::new(&path), 5, 0)?;
    queue.ensure_wake("crashed-session").await?;
    assert!(queue.next_wake().await?.is_some());
    // Skip Rust destructors to exercise durable active-job recovery.
    exit(42);
}

#[tokio::test]
async fn active_job_recovers_after_process_exits_without_destructors() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("queue.redb");
    let output = Command::new(env::current_exe()?)
        .args(["--exact", "crash_child", "--nocapture"])
        .env("INTERLINK_RUSTQUEUE_CRASH_DB", &path)
        .output()?;
    assert_eq!(output.status.code(), Some(42), "{output:?}");
    let queue = Notifications::open(&path, 5, 0)?;
    let recovered = eventually_next(&queue).await?;
    assert_eq!(recovered.data["session"], "crashed-session");
    assert!(recovered.attempt > 0);
    Ok(())
}
