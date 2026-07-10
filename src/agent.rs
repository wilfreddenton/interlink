//! The per-agent channel server and its decision logic.
//!
//! Inbound flow, for each message drained from the bus:
//!   verify signature → sender on the allowlist? → addressed to me? → fresh? →
//!   not a replay? → dispatch by the peer's grant.
//!
//! A `*` peer is handled **inline**: the message is pushed straight into the
//! session as a `<channel>` event. A scoped peer is (will be) handled in a
//! quarantined subagent — for now its body is withheld and only metadata is
//! pushed, pending the `PreToolUse` enforcement layer.

use std::collections::VecDeque;

use crate::identity::{AgentId, SignedMessage, check_freshness};
use crate::policy::{Grant, Policy};

/// How far a message's timestamp may be from local time. Bounds the replay
/// window that the dedupe set must remember.
pub const MAX_SKEW_MS: u64 = 60_000;

/// What to do with a verified, authorized message.
#[derive(Debug, PartialEq, Eq)]
pub enum Dispatch {
    /// Trusted peer: push the full content into the session now.
    Inline { petname: String, text: String },
    /// Scoped peer: quarantine the body, push only metadata. The main agent
    /// spawns a subagent named `capability` (whose `tools:` frontmatter limits
    /// it) to fetch and act on the body.
    Scoped { petname: String, capability: String },
}

/// Why a message was dropped. All of these are logged, none reach the model.
#[derive(Debug, PartialEq, Eq)]
pub enum Reject {
    BadSignature,
    NotAllowlisted,
    WrongRecipient,
    Stale,
    Replay,
}

pub type Verdict = Result<Dispatch, Reject>;

/// The full inbound gate. `me` is this agent's own id; the bus routes by key,
/// but we re-check so a misrouted or spoofed `to` can't slip through.
pub fn decide(
    msg: &SignedMessage,
    me: AgentId,
    policy: &Policy,
    now: u64,
    seen: &mut Dedupe,
) -> Verdict {
    // 1. Authenticate the sender from the signature — never from the `from`
    //    string, which is attacker-controlled until verified.
    let from = msg.verify().map_err(|_| Reject::BadSignature)?;

    // 2. Is this authenticated key on the allowlist?
    let peer = policy.peer(from).ok_or(Reject::NotAllowlisted)?;

    // 3. Is it actually addressed to us?
    let to = AgentId::from_b64(&msg.to).map_err(|_| Reject::WrongRecipient)?;
    if to != me {
        return Err(Reject::WrongRecipient);
    }

    // 4. Fresh enough to bound replays.
    check_freshness(msg.ts, now, MAX_SKEW_MS).map_err(|_| Reject::Stale)?;

    // 5. Not already seen. Do this last so a replayed *valid* message is still
    //    recorded only once, and invalid ones never consume a dedupe slot.
    if !seen.insert(&msg.msg_id) {
        return Err(Reject::Replay);
    }

    Ok(match &peer.grant {
        Grant::All => Dispatch::Inline {
            petname: peer.petname.clone(),
            text: msg.text.clone(),
        },
        Grant::Scoped(capability) => Dispatch::Scoped {
            petname: peer.petname.clone(),
            capability: capability.clone(),
        },
    })
}

/// A scoped request whose untrusted body is held back from the main context
/// until a capability subagent fetches it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldRequest {
    /// The authenticated sender's petname.
    pub from: String,
    /// The untrusted request body. Only ever handed to a capability subagent.
    pub text: String,
}

/// Withheld scoped bodies, keyed by `msg_id`. Bounded (drop-oldest) so a scoped
/// peer can't grow it without limit by never having its requests fetched.
pub struct Quarantine {
    order: VecDeque<String>,
    held: std::collections::HashMap<String, HeldRequest>,
    cap: usize,
}

impl Quarantine {
    pub fn new(cap: usize) -> Self {
        Self {
            order: VecDeque::new(),
            held: std::collections::HashMap::new(),
            cap: cap.max(1),
        }
    }

    /// Hold a body under `msg_id`. Evicts the oldest if at capacity.
    pub fn hold(&mut self, msg_id: String, req: HeldRequest) {
        if self.held.contains_key(&msg_id) {
            return; // dedupe upstream should prevent this; ignore duplicates
        }
        if self.order.len() >= self.cap
            && let Some(old) = self.order.pop_front()
        {
            self.held.remove(&old);
        }
        self.order.push_back(msg_id.clone());
        self.held.insert(msg_id, req);
    }

    /// Take the body for `msg_id` — **one-shot**: a body can't be re-read after
    /// it's handled. `None` if unknown or already taken.
    pub fn take(&mut self, msg_id: &str) -> Option<HeldRequest> {
        let req = self.held.remove(msg_id)?;
        self.order.retain(|id| id != msg_id);
        Some(req)
    }

    pub fn len(&self) -> usize {
        self.held.len()
    }

    pub fn is_empty(&self) -> bool {
        self.held.is_empty()
    }
}

/// A bounded set of recently-seen `msg_id`s. Bounded because we only need to
/// reject replays inside the freshness window; anything older is already
/// rejected by [`check_freshness`], so unbounded memory would be pointless.
pub struct Dedupe {
    order: VecDeque<String>,
    seen: std::collections::HashSet<String>,
    cap: usize,
}

impl Dedupe {
    pub fn new(cap: usize) -> Self {
        Self {
            order: VecDeque::with_capacity(cap),
            seen: std::collections::HashSet::with_capacity(cap),
            cap: cap.max(1),
        }
    }

    /// Record `id`. Returns `false` if it was already present (a replay).
    pub fn insert(&mut self, id: &str) -> bool {
        if self.seen.contains(id) {
            return false;
        }
        if self.order.len() >= self.cap
            && let Some(old) = self.order.pop_front()
        {
            self.seen.remove(&old);
        }
        self.order.push_back(id.to_string());
        self.seen.insert(id.to_string());
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::AgentKey;
    use crate::policy::Policy;

    fn policy_for(key: &AgentKey, may: &str) -> Policy {
        // `may` is a JSON string literal, e.g. "\"*\"" or "\"run-tests\"".
        let raw = format!(
            r#"{{ "alice": {{ "key": "{}", "may": {} }} }}"#,
            key.id().to_b64(),
            may
        );
        Policy::parse(&raw).unwrap()
    }

    #[test]
    fn wildcard_peer_is_dispatched_inline() {
        let (alice, me) = (AgentKey::generate().unwrap(), AgentKey::generate().unwrap());
        let policy = policy_for(&alice, "\"*\"");
        let msg = alice.sign(me.id(), "run the deploy", 1_000, "m1");
        let mut seen = Dedupe::new(16);
        assert_eq!(
            decide(&msg, me.id(), &policy, 1_000, &mut seen),
            Ok(Dispatch::Inline {
                petname: "alice".into(),
                text: "run the deploy".into()
            })
        );
    }

    #[test]
    fn scoped_peer_withholds_the_body() {
        let (alice, me) = (AgentKey::generate().unwrap(), AgentKey::generate().unwrap());
        let policy = policy_for(&alice, "\"run-tests\"");
        let msg = alice.sign(me.id(), "secret prose", 1_000, "m1");
        let mut seen = Dedupe::new(16);
        // The dispatch names the capability agent but carries no text — the body
        // stays quarantined until the subagent fetches it.
        assert_eq!(
            decide(&msg, me.id(), &policy, 1_000, &mut seen),
            Ok(Dispatch::Scoped {
                petname: "alice".into(),
                capability: "run-tests".into()
            })
        );
    }

    #[test]
    fn stranger_is_rejected_even_with_valid_signature() {
        let (stranger, me) = (AgentKey::generate().unwrap(), AgentKey::generate().unwrap());
        let policy = policy_for(&AgentKey::generate().unwrap(), "\"*\""); // allowlists someone else
        let msg = stranger.sign(me.id(), "hi", 1_000, "m1");
        let mut seen = Dedupe::new(16);
        assert_eq!(
            decide(&msg, me.id(), &policy, 1_000, &mut seen),
            Err(Reject::NotAllowlisted)
        );
    }

    #[test]
    fn forged_sender_is_bad_signature() {
        let (alice, me) = (AgentKey::generate().unwrap(), AgentKey::generate().unwrap());
        let policy = policy_for(&alice, "\"*\"");
        // Eve signs but stamps alice's key as `from`.
        let eve = AgentKey::generate().unwrap();
        let mut msg = eve.sign(me.id(), "hi", 1_000, "m1");
        msg.from = alice.id().to_b64();
        let mut seen = Dedupe::new(16);
        assert_eq!(
            decide(&msg, me.id(), &policy, 1_000, &mut seen),
            Err(Reject::BadSignature)
        );
    }

    #[test]
    fn message_for_someone_else_is_rejected() {
        let (alice, me) = (AgentKey::generate().unwrap(), AgentKey::generate().unwrap());
        let other = AgentKey::generate().unwrap();
        let policy = policy_for(&alice, "\"*\"");
        let msg = alice.sign(other.id(), "hi", 1_000, "m1"); // addressed to `other`
        let mut seen = Dedupe::new(16);
        assert_eq!(
            decide(&msg, me.id(), &policy, 1_000, &mut seen),
            Err(Reject::WrongRecipient)
        );
    }

    #[test]
    fn stale_message_is_rejected() {
        let (alice, me) = (AgentKey::generate().unwrap(), AgentKey::generate().unwrap());
        let policy = policy_for(&alice, "\"*\"");
        let msg = alice.sign(me.id(), "hi", 1_000, "m1");
        let mut seen = Dedupe::new(16);
        let far_future = 1_000 + MAX_SKEW_MS + 1;
        assert_eq!(
            decide(&msg, me.id(), &policy, far_future, &mut seen),
            Err(Reject::Stale)
        );
    }

    #[test]
    fn replay_is_rejected_the_second_time() {
        let (alice, me) = (AgentKey::generate().unwrap(), AgentKey::generate().unwrap());
        let policy = policy_for(&alice, "\"*\"");
        let msg = alice.sign(me.id(), "hi", 1_000, "m1");
        let mut seen = Dedupe::new(16);
        assert!(decide(&msg, me.id(), &policy, 1_000, &mut seen).is_ok());
        assert_eq!(
            decide(&msg, me.id(), &policy, 1_000, &mut seen),
            Err(Reject::Replay)
        );
    }

    #[test]
    fn dedupe_forgets_oldest_beyond_cap() {
        let mut d = Dedupe::new(2);
        assert!(d.insert("a"));
        assert!(d.insert("b"));
        assert!(d.insert("c")); // evicts "a"
        assert!(d.insert("a"), "a was evicted, so it is fresh again");
        assert!(!d.insert("c"), "c is still within the window");
    }

    fn held(text: &str) -> HeldRequest {
        HeldRequest {
            from: "alice".into(),
            text: text.into(),
        }
    }

    #[test]
    fn quarantine_take_is_one_shot() {
        let mut q = Quarantine::new(8);
        q.hold("m1".into(), held("do the thing"));
        assert_eq!(q.take("m1"), Some(held("do the thing")));
        // Second fetch must fail — a body cannot be re-read after handling.
        assert_eq!(q.take("m1"), None);
        assert!(q.is_empty());
    }

    #[test]
    fn quarantine_take_of_unknown_is_none() {
        let mut q = Quarantine::new(8);
        assert_eq!(q.take("nope"), None);
    }

    #[test]
    fn quarantine_is_bounded_drop_oldest() {
        let mut q = Quarantine::new(2);
        q.hold("m1".into(), held("one"));
        q.hold("m2".into(), held("two"));
        q.hold("m3".into(), held("three")); // evicts m1
        assert_eq!(q.len(), 2);
        assert_eq!(q.take("m1"), None, "oldest was evicted");
        assert_eq!(q.take("m2"), Some(held("two")));
        assert_eq!(q.take("m3"), Some(held("three")));
    }

    #[test]
    fn quarantine_take_keeps_order_intact_for_eviction() {
        // Taking a middle entry must not leave a stale id that later evicts the
        // wrong thing.
        let mut q = Quarantine::new(2);
        q.hold("m1".into(), held("one"));
        q.hold("m2".into(), held("two"));
        assert_eq!(q.take("m1"), Some(held("one")));
        q.hold("m3".into(), held("three")); // m2, m3 fit; nothing wrongly evicted
        assert_eq!(q.take("m2"), Some(held("two")));
        assert_eq!(q.take("m3"), Some(held("three")));
    }
}
