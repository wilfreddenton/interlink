//! Shared consumption and notification state for every host adapter.

use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use anyhow::{Result, bail};
use fs2::FileExt;
use serde::{Deserialize, Serialize};

use crate::agent::MAX_PAST_MS;
use crate::identity::{SignedMessage, TaskStatus, mint_session_id};
use crate::now_ms;
use crate::state::{atomic_write, lock};

const NOTICE_RETRY_MS: u64 = 30_000;
const MAX_NOTICE_RETRY_MS: u64 = 300_000;
const MAX_RECORDS: usize = 4096;
const MAX_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Received {
    pub message: SignedMessage,
    pub peer: String,
    pub state: String,
    pub superseded_by: Option<String>,
}

impl Received {
    pub fn unread(&self) -> bool {
        self.state != "receiver_acknowledged" && self.superseded_by.is_none()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Notice {
    pub id: String,
    pub state: String,
    pub messages: Vec<(String, String)>,
    #[serde(default)]
    pub retry_at: u64,
    #[serde(default)]
    pub attempt: u32,
}

impl Notice {
    pub fn text(&self) -> String {
        format!(
            "[Interlink inbox notice] Call receive_messages(notification_id=\"{}\") to fetch current unread peer messages, then call acknowledge_messages with the returned receipt IDs after reading them. This notice contains no peer request. If nothing remains unread, continue silently without a user-facing reply. Routine progress needs no reply.",
            self.id
        )
    }
}

#[derive(Default, Serialize, Deserialize)]
struct Data {
    records: Vec<Received>,
    notice: Option<Notice>,
}

pub struct Mailbox {
    path: PathBuf,
}

impl Mailbox {
    pub fn new(path: &Path) -> Self {
        Self {
            path: path.to_owned(),
        }
    }

    pub fn notifier_lock(&self) -> Result<Option<File>> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(self.path.with_extension("notifier-lock"))?;
        match file.try_lock_exclusive() {
            Ok(()) => Ok(Some(file)),
            Err(e) if e.kind() == ErrorKind::WouldBlock => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn load(&self) -> Result<Data> {
        match std::fs::read(&self.path) {
            Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(Data::default()),
            Err(e) => Err(e.into()),
        }
    }

    fn save(&self, data: &Data) -> Result<()> {
        let bytes = serde_json::to_vec(data)?;
        atomic_write(&self.path, &bytes)
    }

    pub fn records(&self) -> Result<Vec<Received>> {
        Ok(self.load()?.records)
    }

    pub fn retain(&self, message: &SignedMessage, peer: &str) -> Result<()> {
        let _lock = lock(&self.path.with_extension("lock"))?;
        let mut data = self.load()?;
        if data
            .records
            .iter()
            .any(|r| r.message.from == message.from && r.message.msg_id == message.msg_id)
        {
            return Ok(());
        }
        // Keep replay tombstones throughout the gate's acceptance window. Unread
        // requests are never evicted just to make space for a newer message.
        data.records
            .retain(|r| r.unread() || now_ms().saturating_sub(r.message.ts) <= MAX_PAST_MS);
        let mut incoming = Received {
            message: message.clone(),
            peer: peer.into(),
            state: "receiver_stored".into(),
            superseded_by: None,
        };
        if message.task_id.is_some()
            && message
                .status
                .is_some_and(|s| s == TaskStatus::Update || s.is_terminal())
        {
            for prior in &mut data.records {
                let same_task = prior.message.from == message.from
                    && prior.message.reply_to == message.reply_to
                    && prior.message.task_id == message.task_id;
                if !same_task {
                    continue;
                }
                // A delayed progress message must not resurrect a completed task.
                if message.status == Some(TaskStatus::Update)
                    && (prior.message.status.is_some_and(TaskStatus::is_terminal)
                        || (prior.message.status == Some(TaskStatus::Update)
                            && prior.message.ts > message.ts))
                {
                    incoming.superseded_by = Some(prior.message.msg_id.clone());
                }
                if prior.message.status == Some(TaskStatus::Update)
                    && prior.superseded_by.is_none()
                    && (message.status.is_some_and(TaskStatus::is_terminal)
                        || prior.message.ts <= message.ts)
                {
                    prior.superseded_by = Some(message.msg_id.clone());
                }
            }
        }
        data.records.push(incoming);
        // Later state changes and notification IDs can grow the metadata. Never
        // apply the admission cap to consumption or recovery writes.
        if data.records.len() > MAX_RECORDS || serde_json::to_vec(&data.records)?.len() > MAX_BYTES
        {
            bail!(
                "Interlink mailbox is full; messages remain on the broker until space is available"
            );
        }
        self.save(&data)
    }

    pub fn reserve_notice(&self) -> Result<Option<Notice>> {
        self.reserve_notice_at(now_ms())
    }

    fn reserve_notice_at(&self, now: u64) -> Result<Option<Notice>> {
        let _lock = lock(&self.path.with_extension("lock"))?;
        let mut data = self.load()?;
        let messages: Vec<(String, String)> = data
            .records
            .iter()
            .filter(|r| r.unread() && r.message.status != Some(TaskStatus::Update))
            .map(|r| (r.message.from.clone(), r.message.msg_id.clone()))
            .collect();
        if messages.is_empty() {
            if data.notice.take().is_some() {
                self.save(&data)?;
            }
            return Ok(None);
        }
        let mut attempt = 0;
        if let Some(notice) = &data.notice {
            // A backward clock jump must not leave a reservation stuck indefinitely.
            let remaining = notice.retry_at.saturating_sub(now);
            if remaining > 0 && remaining <= MAX_NOTICE_RETRY_MS {
                return Ok(None);
            }
            attempt = notice.attempt.saturating_add(1).min(4);
        }
        let delay = (NOTICE_RETRY_MS << attempt).min(MAX_NOTICE_RETRY_MS);
        let notice = Notice {
            id: mint_session_id()?,
            state: "preparing".into(),
            messages,
            retry_at: now.saturating_add(delay),
            attempt,
        };
        data.notice = Some(notice.clone());
        self.save(&data)?;
        Ok(Some(notice))
    }

    pub fn finish_notice(&self, id: &str, state: &str) -> Result<()> {
        let _lock = lock(&self.path.with_extension("lock"))?;
        let mut data = self.load()?;
        if let Some(notice) = &mut data.notice
            && notice.id == id
        {
            notice.state = state.into();
            for record in &mut data.records {
                if record.unread()
                    && notice.messages.iter().any(|(sender, id)| {
                        sender == &record.message.from && id == &record.message.msg_id
                    })
                {
                    record.state = state.into();
                }
            }
            self.save(&data)?;
        }
        Ok(())
    }

    pub fn acknowledge(&self, ids: &[(String, String)]) -> Result<usize> {
        let _lock = lock(&self.path.with_extension("lock"))?;
        let mut data = self.load()?;
        let mut acknowledged = 0;
        let ids: HashSet<_> = ids
            .iter()
            .map(|(sender, id)| (sender.as_str(), id.as_str()))
            .collect();
        for record in &mut data.records {
            if record.state != "receiver_acknowledged"
                && ids.contains(&(record.message.from.as_str(), record.message.msg_id.as_str()))
            {
                record.state = "receiver_acknowledged".into();
                acknowledged += 1;
            }
        }
        // Free the reservation only when none of its covered messages still need
        // attention. New arrivals are left unread and can immediately wake the host.
        if data.notice.as_ref().is_some_and(|notice| {
            let covered: HashSet<_> = notice
                .messages
                .iter()
                .map(|(sender, id)| (sender.as_str(), id.as_str()))
                .collect();
            !data.records.iter().any(|record| {
                record.unread()
                    && covered
                        .contains(&(record.message.from.as_str(), record.message.msg_id.as_str()))
            })
        }) {
            data.notice = None;
        }
        self.save(&data)?;
        Ok(acknowledged)
    }

    /// Notification IDs are advisory: stale notices always fetch current unread state.
    pub fn receive(&self, _notification_id: Option<&str>, limit: usize) -> Result<Vec<Received>> {
        let data = self.load()?;
        let mut received: Vec<_> = data.records.into_iter().filter(Received::unread).collect();
        // Stable ordering keeps questions and failures ahead of routine progress.
        received.sort_by_key(|r| r.message.status == Some(TaskStatus::Update));
        received.truncate(limit);
        Ok(received)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{AgentKey, MessageKind};

    fn message(id: &str, task: &str, status: Option<TaskStatus>, ts: u64) -> SignedMessage {
        let key = AgentKey::from_b64("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA").unwrap();
        key.sign_full(
            key.id(),
            id,
            ts,
            id,
            MessageKind::Message,
            Some(task),
            status,
            None,
        )
    }

    fn ids(records: &[Received]) -> Vec<(String, String)> {
        records
            .iter()
            .map(|r| (r.message.from.clone(), r.message.msg_id.clone()))
            .collect()
    }

    #[test]
    fn lost_notices_recover_in_every_state_and_backoff_is_bounded() {
        for state in [
            "preparing",
            "host_queued",
            "inbox_queued",
            "notification_sent",
            "delivery_failed",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("mail.json");
            let mailbox = Mailbox::new(&path);
            mailbox
                .retain(&message("a", "task", None, now_ms()), "peer")
                .unwrap();
            let mut notice = mailbox.reserve_notice_at(1_000).unwrap().unwrap();
            mailbox.finish_notice(&notice.id, state).unwrap();
            let restarted = Mailbox::new(&path);
            for _ in 0..10 {
                assert!(
                    restarted
                        .reserve_notice_at(notice.retry_at - 1)
                        .unwrap()
                        .is_none()
                );
                let next = restarted
                    .reserve_notice_at(notice.retry_at)
                    .unwrap()
                    .unwrap();
                assert_ne!(next.id, notice.id);
                assert!(next.retry_at - notice.retry_at <= MAX_NOTICE_RETRY_MS);
                notice = next;
            }
        }
    }

    #[test]
    fn dropped_fetch_and_partial_ack_leave_remaining_messages_recoverable() {
        let dir = tempfile::tempdir().unwrap();
        let mailbox = Mailbox::new(&dir.path().join("mail.json"));
        for id in ["a", "b"] {
            mailbox
                .retain(&message(id, "task", None, now_ms()), "peer")
                .unwrap();
        }
        let first = mailbox.reserve_notice_at(1_000).unwrap().unwrap();
        let fetched = mailbox.receive(None, 1).unwrap();
        assert_eq!(
            mailbox.receive(None, 1).unwrap()[0].message.msg_id,
            fetched[0].message.msg_id
        );
        mailbox
            .retain(&message("c", "task", None, now_ms()), "peer")
            .unwrap();
        assert_eq!(mailbox.acknowledge(&ids(&fetched)).unwrap(), 1);
        assert_eq!(mailbox.acknowledge(&ids(&fetched)).unwrap(), 0);
        let retry = mailbox.reserve_notice_at(first.retry_at).unwrap().unwrap();
        mailbox.finish_notice(&first.id, "host_queued").unwrap();
        assert_eq!(mailbox.load().unwrap().notice.unwrap().id, retry.id);
        assert_eq!(mailbox.receive(None, 20).unwrap().len(), 2);
        mailbox
            .acknowledge(&ids(&mailbox.records().unwrap()))
            .unwrap();
        assert!(mailbox.reserve_notice_at(retry.retry_at).unwrap().is_none());
    }

    #[test]
    fn history_consumption_without_notice_id_unblocks_future_messages() {
        let dir = tempfile::tempdir().unwrap();
        let mailbox = Mailbox::new(&dir.path().join("mail.json"));
        mailbox
            .retain(&message("a", "task", None, now_ms()), "peer")
            .unwrap();
        let old = mailbox.reserve_notice().unwrap().unwrap();
        mailbox.finish_notice(&old.id, "notification_sent").unwrap();
        mailbox
            .retain(&message("b", "task", None, now_ms()), "peer")
            .unwrap();
        let fetched = mailbox.receive(None, 1).unwrap();
        mailbox.acknowledge(&ids(&fetched)).unwrap();
        let next = mailbox.reserve_notice().unwrap().unwrap();
        assert_ne!(next.id, old.id);
        assert_eq!(
            mailbox.receive(Some(&old.id), 20).unwrap()[0]
                .message
                .msg_id,
            "b"
        );
    }

    #[test]
    fn old_mailbox_notices_and_backward_clock_jumps_do_not_block_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let mailbox = Mailbox::new(&dir.path().join("mail.json"));
        mailbox
            .retain(&message("a", "task", None, now_ms()), "peer")
            .unwrap();
        let first = mailbox.reserve_notice().unwrap().unwrap();
        let mut legacy = serde_json::to_value(mailbox.load().unwrap()).unwrap();
        legacy["notice"].as_object_mut().unwrap().remove("retry_at");
        legacy["notice"].as_object_mut().unwrap().remove("attempt");
        atomic_write(&mailbox.path, &serde_json::to_vec(&legacy).unwrap()).unwrap();
        let restored = mailbox.reserve_notice().unwrap().unwrap();
        assert_ne!(first.id, restored.id);
        assert!(mailbox.reserve_notice_at(0).unwrap().is_some());
    }

    #[test]
    fn history_consumption_survives_restart_and_an_already_queued_notice() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mail.json");
        let mailbox = Mailbox::new(&path);
        let msg = message("one", "task", None, now_ms());
        mailbox.retain(&msg, "peer").unwrap();
        let notice = mailbox.reserve_notice().unwrap().unwrap();
        mailbox.finish_notice(&notice.id, "host_queued").unwrap();
        mailbox
            .acknowledge(&[(msg.from.clone(), "one".into())])
            .unwrap();
        let restarted = Mailbox::new(&path);
        restarted.retain(&msg, "peer").unwrap();
        assert_eq!(restarted.records().unwrap().len(), 1);
        assert!(restarted.reserve_notice().unwrap().is_none());
        assert!(restarted.receive(Some(&notice.id), 20).unwrap().is_empty());
        assert!(restarted.reserve_notice().unwrap().is_none());
        assert_eq!(
            restarted.records().unwrap()[0].state,
            "receiver_acknowledged"
        );
    }

    #[test]
    fn progress_is_quiet_and_superseded_but_questions_and_failures_survive() {
        let dir = tempfile::tempdir().unwrap();
        let mailbox = Mailbox::new(&dir.path().join("mail.json"));
        let ts = now_ms();
        for (id, status, offset) in [
            ("old", TaskStatus::Update, 0),
            ("new", TaskStatus::Update, 2),
            ("delayed", TaskStatus::Update, 1),
        ] {
            mailbox
                .retain(&message(id, "task", Some(status), ts + offset), "peer")
                .unwrap();
        }
        assert!(mailbox.reserve_notice().unwrap().is_none());
        mailbox
            .retain(
                &message("question", "task", Some(TaskStatus::NeedsInput), ts + 3),
                "peer",
            )
            .unwrap();
        mailbox
            .retain(
                &message("failure", "task", Some(TaskStatus::Failed), ts + 4),
                "peer",
            )
            .unwrap();
        mailbox
            .retain(
                &message("after-terminal", "task", Some(TaskStatus::Update), ts + 5),
                "peer",
            )
            .unwrap();
        let notice = mailbox.reserve_notice().unwrap().unwrap();
        let records = mailbox.receive(Some(&notice.id), 20).unwrap();
        assert_eq!(
            records
                .iter()
                .map(|r| r.message.msg_id.as_str())
                .collect::<Vec<_>>(),
            ["question", "failure"]
        );
        assert_eq!(mailbox.records().unwrap().len(), 6);
    }

    #[test]
    fn coalescing_is_scoped_to_sender_session_and_task() {
        let dir = tempfile::tempdir().unwrap();
        let mailbox = Mailbox::new(&dir.path().join("mail.json"));
        let ts = now_ms();
        let mut a = message("a", "task", Some(TaskStatus::Update), ts);
        a.reply_to = Some("session-a".into());
        let mut b = message("b", "task", Some(TaskStatus::Result), ts + 1);
        b.reply_to = Some("session-b".into());
        mailbox.retain(&a, "peer").unwrap();
        mailbox.retain(&b, "peer").unwrap();
        mailbox
            .retain(
                &message("c", "other", Some(TaskStatus::Result), ts + 2),
                "peer",
            )
            .unwrap();
        assert_eq!(mailbox.receive(None, 20).unwrap().len(), 3);
    }

    #[test]
    fn notification_races_do_not_regress_acknowledgement_or_clear_new_wakeups() {
        let dir = tempfile::tempdir().unwrap();
        let mailbox = Mailbox::new(&dir.path().join("mail.json"));
        mailbox
            .retain(&message("a", "task", None, now_ms()), "peer")
            .unwrap();
        let first = mailbox.reserve_notice().unwrap().unwrap();
        let fetched = mailbox.receive(Some(&first.id), 20).unwrap();
        mailbox.acknowledge(&ids(&fetched)).unwrap();
        mailbox
            .retain(&message("b", "task", None, now_ms()), "peer")
            .unwrap();
        let second = mailbox.reserve_notice().unwrap().unwrap();
        mailbox.finish_notice(&first.id, "host_queued").unwrap();
        let fetched = mailbox.receive(Some(&first.id), 20).unwrap();
        assert_eq!(mailbox.load().unwrap().notice.unwrap().id, second.id);
        mailbox.acknowledge(&ids(&fetched)).unwrap();
        assert!(
            mailbox
                .records()
                .unwrap()
                .iter()
                .all(|r| r.state == "receiver_acknowledged")
        );
    }

    #[test]
    fn only_one_notifier_and_acknowledgement_is_idempotent_between_handles() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mail.json");
        let a = Mailbox::new(&path);
        let b = Mailbox::new(&path);
        let guard = a.notifier_lock().unwrap().unwrap();
        assert!(b.notifier_lock().unwrap().is_none());
        a.retain(&message("a", "task", None, now_ms()), "peer")
            .unwrap();
        let workers: Vec<_> = (0..2)
            .map(|_| {
                let path = path.clone();
                std::thread::spawn(move || {
                    let mailbox = Mailbox::new(&path);
                    let records = mailbox.records().unwrap();
                    mailbox.acknowledge(&ids(&records)).unwrap()
                })
            })
            .collect();
        assert_eq!(
            workers
                .into_iter()
                .map(|w| w.join().unwrap())
                .sum::<usize>(),
            1
        );
        drop(guard);
        assert!(b.notifier_lock().unwrap().is_some());
    }
    #[test]
    fn full_mailbox_can_be_consumed_and_expired_tombstones_make_room() {
        let dir = tempfile::tempdir().unwrap();
        let mailbox = Mailbox::new(&dir.path().join("mail.json"));
        let base = message("base", "task", None, now_ms());
        let mut data = Data::default();
        for i in 0..MAX_RECORDS {
            let mut msg = base.clone();
            msg.msg_id = i.to_string();
            data.records.push(Received {
                message: msg,
                peer: "peer".into(),
                state: "receiver_stored".into(),
                superseded_by: None,
            });
        }
        mailbox.save(&data).unwrap();
        let next = message("next", "task", None, now_ms());
        assert!(mailbox.retain(&next, "peer").is_err());
        assert_eq!(mailbox.records().unwrap().len(), MAX_RECORDS);
        let notice = mailbox.reserve_notice().unwrap().unwrap();
        assert_eq!(
            mailbox
                .receive(Some(&notice.id), MAX_RECORDS)
                .unwrap()
                .len(),
            MAX_RECORDS
        );
        mailbox
            .acknowledge(&ids(&mailbox.records().unwrap()))
            .unwrap();
        let mut data = mailbox.load().unwrap();
        for record in &mut data.records {
            record.message.ts = now_ms() - MAX_PAST_MS - 1;
        }
        mailbox.save(&data).unwrap();
        mailbox.retain(&next, "peer").unwrap();
        assert_eq!(mailbox.records().unwrap().len(), 1);
    }

    #[test]
    fn history_acknowledgement_is_scoped_by_identity_and_questions_take_priority() {
        let dir = tempfile::tempdir().unwrap();
        let mailbox = Mailbox::new(&dir.path().join("mail.json"));
        let progress = message("same-id", "task", Some(TaskStatus::Update), now_ms());
        let other = AgentKey::generate().unwrap();
        let question = other.sign_full(
            other.id(),
            "question",
            now_ms(),
            "same-id",
            MessageKind::Message,
            Some("task"),
            Some(TaskStatus::NeedsInput),
            None,
        );
        mailbox.retain(&progress, "peer").unwrap();
        mailbox.retain(&question, "other").unwrap();
        mailbox
            .acknowledge(&[(progress.from, progress.msg_id)])
            .unwrap();
        let received = mailbox.receive(None, 1).unwrap();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].peer, "other");
        mailbox.acknowledge(&ids(&received)).unwrap();
        mailbox
            .retain(
                &message("progress", "other-task", Some(TaskStatus::Update), now_ms()),
                "peer",
            )
            .unwrap();
        mailbox
            .retain(&message("request", "task", None, now_ms()), "peer")
            .unwrap();
        assert_eq!(
            mailbox.receive(None, 1).unwrap()[0].message.msg_id,
            "request"
        );
    }
}
