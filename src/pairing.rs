//! Durable, session-scoped pairing requests and their control-message outbox.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use crate::identity::{AgentId, AgentKey, MessageKind, SignedMessage};
use crate::route::Route;
use crate::state::{atomic_write, lock};

const CAP: usize = 64;

#[derive(Clone, Serialize, Deserialize)]
pub struct Request {
    pub key: String,
    pub name: String,
    pub request_id: String,
    pub reply_to: String,
}

impl Request {
    pub fn inbound(msg: &SignedMessage) -> Result<Self> {
        let route = msg.reply_to.as_deref().map(Route::parse);
        let Some(route) = route.filter(|route| route.key == msg.from && route.session.is_some())
        else {
            bail!("pairing request has no return session for its sender");
        };
        Ok(Self {
            key: msg.from.clone(),
            name: msg.text.clone(),
            request_id: msg.msg_id.clone(),
            reply_to: route.to_string(),
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ControlMessage {
    pub route: String,
    pub msg: SignedMessage,
}

impl ControlMessage {
    pub fn for_delivery(&self, key: &AgentKey, now: u64) -> Result<SignedMessage> {
        if self.msg.verify()? != key.id()
            || !matches!(
                self.msg.kind,
                MessageKind::PairRequest | MessageKind::PairAccept
            )
        {
            bail!("pairing outbox contains an invalid control message");
        }
        // The durable job may outlive the freshness window. Preserve its ID so
        // correlation and receiver deduplication survive a renewed signature.
        let mut msg = key.sign_full(
            AgentId::from_b64(&self.msg.to)?,
            &self.msg.text,
            now,
            &self.msg.msg_id,
            self.msg.kind,
            self.msg.task_id.as_deref(),
            self.msg.status,
            self.msg.in_reply_to.as_deref(),
        );
        msg.reply_to = self.msg.reply_to.clone();
        Ok(msg)
    }
}

#[derive(Default, Serialize, Deserialize)]
struct State {
    inbound: Vec<Request>,
    outbound: Vec<Request>,
    queued: Vec<ControlMessage>,
}

pub enum Acceptance {
    Queued,
    ConfirmationPending(String),
}

pub struct PairingStore {
    path: PathBuf,
}

impl PairingStore {
    pub fn new(path: &Path) -> Self {
        Self {
            path: path.to_owned(),
        }
    }

    fn read(&self) -> Result<State> {
        match std::fs::read(&self.path) {
            Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(State::default()),
            Err(e) => Err(e.into()),
        }
    }

    fn update<T>(&self, change: impl FnOnce(&mut State) -> Result<T>) -> Result<T> {
        let _lock = lock(&self.path.with_extension("lock"))?;
        let mut state = self.read()?;
        let result = change(&mut state)?;
        atomic_write(&self.path, &serde_json::to_vec(&state)?)?;
        Ok(result)
    }

    pub fn inbound(&self) -> Result<Vec<Request>> {
        Ok(self.read()?.inbound)
    }

    pub fn find(&self, fingerprint: &str) -> Result<Option<Request>> {
        let requests = self.inbound()?;
        let mut matches = requests.into_iter().filter(|r| {
            r.key == fingerprint || r.key.chars().take(8).collect::<String>() == fingerprint
        });
        let first = matches.next();
        if matches.next().is_some() {
            bail!("ambiguous fingerprint; use the full key");
        }
        Ok(first)
    }

    pub fn receive(&self, request: Request) -> Result<()> {
        self.update(|s| {
            put(&mut s.inbound, request);
            Ok(())
        })
    }

    pub fn request(
        &self,
        mut request: Request,
        message: impl FnOnce(&Request) -> ControlMessage,
    ) -> Result<()> {
        self.update(|s| {
            // A delayed acceptance must still match after an explicit retry.
            // Select the ID under the lock, before the caller signs the message.
            if let Some(old) = s.outbound.iter().find(|r| r.key == request.key) {
                request.request_id = old.request_id.clone();
            }
            s.queued.retain(|job| job.msg.msg_id != request.request_id);
            enqueue(s, message(&request))?;
            put(&mut s.outbound, request);
            Ok(())
        })
    }

    pub fn accept(
        &self,
        request: &Request,
        message: ControlMessage,
        authorize: impl FnOnce() -> Result<()>,
    ) -> Result<Acceptance> {
        self.accept_with_commit(request, message, authorize, atomic_write)
    }

    fn accept_with_commit(
        &self,
        request: &Request,
        message: ControlMessage,
        authorize: impl FnOnce() -> Result<()>,
        commit: impl FnOnce(&Path, &[u8]) -> Result<()>,
    ) -> Result<Acceptance> {
        let _lock = lock(&self.path.with_extension("lock"))?;
        let mut state = self.read()?;
        if !state
            .inbound
            .iter()
            .any(|r| r.key == request.key && r.request_id == request.request_id)
        {
            bail!("pairing request changed; review the pending requests again");
        }
        // Validate and serialize before changing trust. A full queue or stale
        // request must not authorize a peer as a side effect of a failed call.
        enqueue(&mut state, message)?;
        state.inbound.retain(|r| r.key != request.key);
        let bytes = serde_json::to_vec(&state)?;
        authorize()?;
        match commit(&self.path, &bytes) {
            Ok(()) => Ok(Acceptance::Queued),
            // Policy and pairing use separate files. Keep the request available
            // for an explicit, idempotent retry if the second commit fails.
            Err(e) => Ok(Acceptance::ConfirmationPending(e.to_string())),
        }
    }

    pub fn reject(&self, key: &str) -> Result<()> {
        self.update(|s| {
            s.inbound.retain(|r| r.key != key);
            Ok(())
        })
    }

    pub fn pending_accept(&self, key: &str, in_reply_to: Option<&str>) -> Result<Option<Request>> {
        Ok(self
            .read()?
            .outbound
            .into_iter()
            .find(|r| r.key == key && in_reply_to.is_none_or(|id| id == r.request_id)))
    }

    pub fn complete(&self, request: &Request) -> Result<()> {
        self.update(|s| {
            s.outbound
                .retain(|r| r.key != request.key || r.request_id != request.request_id);
            Ok(())
        })
    }

    pub fn queued(&self) -> Result<Vec<ControlMessage>> {
        Ok(self.read()?.queued)
    }

    pub fn sent(&self, msg_id: &str) -> Result<()> {
        self.update(|s| {
            s.queued.retain(|job| job.msg.msg_id != msg_id);
            Ok(())
        })
    }
}

fn put(requests: &mut Vec<Request>, request: Request) {
    requests.retain(|r| r.key != request.key);
    if requests.len() >= CAP {
        requests.remove(0);
    }
    requests.push(request);
}

fn enqueue(state: &mut State, message: ControlMessage) -> Result<()> {
    if state.queued.len() >= CAP * 2 {
        bail!("pairing outbox is full; retry after the relay recovers");
    }
    state.queued.push(message);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{AgentKey, MessageKind};
    use crate::policy_store::PolicyStore;

    #[test]
    fn renewing_control_messages_preserves_identity_and_metadata() {
        let alice = AgentKey::generate().unwrap();
        let bob = AgentKey::generate().unwrap();
        for kind in [MessageKind::PairRequest, MessageKind::PairAccept] {
            let mut msg = alice.sign_full(
                bob.id(),
                "alice",
                1,
                "original",
                kind,
                None,
                None,
                Some("request"),
            );
            msg.reply_to = Some(format!("{}#session", alice.id().to_b64()));
            let job = ControlMessage {
                route: format!("{}#recipient", bob.id().to_b64()),
                msg,
            };
            let renewed = job.for_delivery(&alice, 200_000_000).unwrap();
            assert_eq!(renewed.verify().unwrap(), alice.id());
            assert_eq!(renewed.ts, 200_000_000);
            assert_ne!(renewed.sig, job.msg.sig);
            let mut fields = serde_json::to_value(&renewed).unwrap();
            fields["ts"] = serde_json::json!(job.msg.ts);
            fields["sig"] = serde_json::json!(job.msg.sig);
            assert_eq!(fields, serde_json::to_value(&job.msg).unwrap());
            assert!(job.for_delivery(&bob, 200_000_000).is_err());
        }
    }

    #[test]
    fn requests_and_acceptances_survive_restart_with_exact_return_route() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pairing.json");
        let alice = AgentKey::generate().unwrap();
        let bob = AgentKey::generate().unwrap();
        let mut msg = alice.sign_as(bob.id(), "alice", 1, "request", MessageKind::PairRequest);
        msg.reply_to = Some(format!("{}#second-session", alice.id().to_b64()));
        let request = Request::inbound(&msg).unwrap();
        let store = PairingStore::new(&path);
        store.receive(request.clone()).unwrap();
        let reopened = PairingStore::new(&path);
        let pending = reopened.find(&alice.id().to_b64()).unwrap().unwrap();
        assert!(pending.reply_to.ends_with("#second-session"));
        let accept = bob.sign_full(
            alice.id(),
            "bob",
            2,
            "accept",
            MessageKind::PairAccept,
            None,
            None,
            Some("request"),
        );
        reopened
            .accept(
                &pending,
                ControlMessage {
                    route: pending.reply_to.clone(),
                    msg: accept,
                },
                || Ok(()),
            )
            .unwrap();
        let reopened = PairingStore::new(&path);
        assert!(reopened.inbound().unwrap().is_empty());
        let queued = reopened.queued().unwrap();
        assert_eq!(queued[0].route, request.reply_to);
        assert_eq!(queued[0].msg.in_reply_to.as_deref(), Some("request"));
        reopened.sent("accept").unwrap();
        assert!(store.queued().unwrap().is_empty());
    }
    #[test]
    fn failed_confirmation_is_explicit_and_can_be_retried() {
        let dir = tempfile::tempdir().unwrap();
        let pairing = PairingStore::new(&dir.path().join("pairing.json"));
        let alice = AgentKey::generate().unwrap();
        let bob = AgentKey::generate().unwrap();
        let policy_path = dir.path().join("peers.json");
        std::fs::write(&policy_path, "{}").unwrap();
        let policy = PolicyStore::open(&policy_path).unwrap();
        let request = Request {
            key: alice.id().to_b64(),
            name: "alice".into(),
            request_id: "request".into(),
            reply_to: format!("{}#session", alice.id().to_b64()),
        };
        pairing.receive(request.clone()).unwrap();
        let message = ControlMessage {
            route: request.reply_to.clone(),
            msg: bob.sign_as(alice.id(), "bob", 1, "accept", MessageKind::PairAccept),
        };
        let outcome = pairing
            .accept_with_commit(
                &request,
                message.clone(),
                || policy.add(&request.name, &request.key),
                |_, _| bail!("simulated storage failure after authorization"),
            )
            .unwrap();
        assert!(matches!(outcome, Acceptance::ConfirmationPending(_)));
        assert!(policy.read().unwrap().resolve("alice").is_ok());
        assert!(pairing.find(&request.key).unwrap().is_some());
        assert!(pairing.queued().unwrap().is_empty());
        assert!(matches!(
            pairing
                .accept(&request, message, || policy
                    .add(&request.name, &request.key))
                .unwrap(),
            Acceptance::Queued
        ));
        assert!(pairing.inbound().unwrap().is_empty());
        assert_eq!(pairing.queued().unwrap().len(), 1);
    }

    #[test]
    fn full_outbox_never_runs_authorization() {
        let dir = tempfile::tempdir().unwrap();
        let pairing = PairingStore::new(&dir.path().join("pairing.json"));
        let alice = AgentKey::generate().unwrap();
        let bob = AgentKey::generate().unwrap();
        let request = Request {
            key: alice.id().to_b64(),
            name: "alice".into(),
            request_id: "request".into(),
            reply_to: format!("{}#session", alice.id().to_b64()),
        };
        let message = ControlMessage {
            route: request.reply_to.clone(),
            msg: bob.sign_as(alice.id(), "bob", 1, "accept", MessageKind::PairAccept),
        };
        pairing.receive(request.clone()).unwrap();
        pairing
            .update(|s| {
                s.queued = vec![message.clone(); CAP * 2];
                Ok(())
            })
            .unwrap();
        let mut authorized = false;
        assert!(
            pairing
                .accept(&request, message, || {
                    authorized = true;
                    Ok(())
                })
                .is_err()
        );
        assert!(!authorized);
        assert!(pairing.find(&request.key).unwrap().is_some());
    }
}
