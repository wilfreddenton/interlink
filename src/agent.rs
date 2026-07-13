//! The per-agent channel server and its decision logic.
//!
//! Inbound flow, for each message drained from the bus:
//!   verify signature → sender on the allowlist? → addressed to me? → fresh? →
//!   not a replay? → dispatch.
//!
//! An admitted peer is handled **inline**: the message is pushed straight into
//! the session as a `<channel>` event. A non-peer may only *knock* to pair;
//! anything else from a non-peer is dropped at the gate.

use std::collections::{HashMap, VecDeque};

use crate::identity::{AgentId, MessageKind, SignedMessage, check_freshness};
use crate::policy::Policy;

/// How far a message's timestamp may be from local time. Bounds the replay
/// window that the dedupe set must remember.
pub const MAX_SKEW_MS: u64 = 60_000;

/// What to do with a verified, authorized message.
#[derive(Debug, PartialEq, Eq)]
pub enum Dispatch {
    /// Trusted peer: push the full content into the session now.
    Inline { petname: String, text: String },
    /// A non-peer *knocked*: it wants to pair. Carries only its key and a
    /// self-claimed name — never actionable text. Surfaced for human accept/reject.
    PairRequest { from_key: String, name: String },
    /// A non-peer replied that it accepted our earlier knock. The handler adds it
    /// only if we actually have an outstanding request to that key.
    PairAccept { from_key: String, name: String },
}

/// Why a message was dropped. All of these are logged, none reach the model.
#[derive(Debug, PartialEq, Eq)]
pub enum Reject {
    BadSignature,
    NotAllowlisted,
    WrongRecipient,
    Stale,
    Replay,
    /// A pairing kind from someone already a peer, or an otherwise nonsensical
    /// (peer, kind) combination — ignored.
    Unexpected,
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

    // 2. Is it actually addressed to us?
    let to = AgentId::from_b64(&msg.to).map_err(|_| Reject::WrongRecipient)?;
    if to != me {
        return Err(Reject::WrongRecipient);
    }

    // 3. Allowlist. A non-peer may deliver *only* a pairing knock; a plain
    //    message from one is dropped here, before it can even consume a dedupe
    //    slot (so non-peers can't flood the replay set).
    let peer = policy.peer(from);
    if peer.is_none() && msg.kind == MessageKind::Message {
        return Err(Reject::NotAllowlisted);
    }

    // 4. Fresh enough to bound replays.
    check_freshness(msg.ts, now, MAX_SKEW_MS).map_err(|_| Reject::Stale)?;

    // 5. Not already seen. Do this after the cheap rejects so a replayed *valid*
    //    message is recorded only once and invalid ones never consume a slot.
    if !seen.insert(&msg.msg_id) {
        return Err(Reject::Replay);
    }

    match (peer, msg.kind) {
        (Some(peer), MessageKind::Message) => Ok(Dispatch::Inline {
            petname: peer.petname.clone(),
            text: msg.text.clone(),
        }),
        // A non-peer knock: identity + self-claimed name only.
        (None, MessageKind::PairRequest) => Ok(Dispatch::PairRequest {
            from_key: from.to_b64(),
            name: msg.text.clone(),
        }),
        (None, MessageKind::PairAccept) => Ok(Dispatch::PairAccept {
            from_key: from.to_b64(),
            name: msg.text.clone(),
        }),
        // A pairing kind from an existing peer, or any other combination.
        _ => Err(Reject::Unexpected),
    }
}

/// A bounded key→value table (drop-oldest), for pending pairing state: inbound
/// knocks (sender key → claimed name) and outbound requests (target key → the
/// grant we'll assign them on accept). Bounded so a knock flood can't grow it.
pub struct PairTable {
    order: VecDeque<String>,
    map: HashMap<String, String>,
    cap: usize,
}

impl PairTable {
    pub fn new(cap: usize) -> Self {
        Self {
            order: VecDeque::new(),
            map: HashMap::new(),
            cap: cap.max(1),
        }
    }

    /// Insert or update `key`; evicts the oldest at capacity.
    pub fn put(&mut self, key: String, value: String) {
        if !self.map.contains_key(&key) {
            if self.order.len() >= self.cap
                && let Some(old) = self.order.pop_front()
            {
                self.map.remove(&old);
            }
            self.order.push_back(key.clone());
        }
        self.map.insert(key, value);
    }

    /// Remove and return `key`'s value.
    pub fn take(&mut self, key: &str) -> Option<String> {
        let v = self.map.remove(key)?;
        self.order.retain(|k| k != key);
        Some(v)
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.map.get(key)
    }

    /// Resolve a full key or an exact 8-char fingerprint to `(key, value)`.
    pub fn find(&self, key_or_fp: &str) -> Option<(String, String)> {
        self.map
            .iter()
            .find(|(k, _)| {
                k.as_str() == key_or_fp || k.chars().take(8).collect::<String>() == key_or_fp
            })
            .map(|(k, v)| (k.clone(), v.clone()))
    }

    pub fn entries(&self) -> impl Iterator<Item = (&String, &String)> {
        self.map.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
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

    fn policy_for(key: &AgentKey) -> Policy {
        let raw = format!(r#"{{ "alice": {{ "key": "{}" }} }}"#, key.id().to_b64());
        Policy::parse(&raw).unwrap()
    }

    #[test]
    fn admitted_peer_is_dispatched_inline() {
        let (alice, me) = (AgentKey::generate().unwrap(), AgentKey::generate().unwrap());
        let policy = policy_for(&alice);
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
    fn stranger_is_rejected_even_with_valid_signature() {
        let (stranger, me) = (AgentKey::generate().unwrap(), AgentKey::generate().unwrap());
        let policy = policy_for(&AgentKey::generate().unwrap()); // allowlists someone else
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
        let policy = policy_for(&alice);
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
        let policy = policy_for(&alice);
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
        let policy = policy_for(&alice);
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
        let policy = policy_for(&alice);
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

    #[test]
    fn non_peer_knock_is_surfaced_not_dropped() {
        let (stranger, me) = (AgentKey::generate().unwrap(), AgentKey::generate().unwrap());
        let policy = Policy::default(); // nobody is a peer
        let msg = stranger.sign_as(
            me.id(),
            "stranger-laptop",
            1_000,
            "k1",
            MessageKind::PairRequest,
        );
        let mut seen = Dedupe::new(16);
        assert_eq!(
            decide(&msg, me.id(), &policy, 1_000, &mut seen),
            Ok(Dispatch::PairRequest {
                from_key: stranger.id().to_b64(),
                name: "stranger-laptop".into()
            })
        );
    }

    #[test]
    fn non_peer_plain_message_is_still_denied() {
        let (stranger, me) = (AgentKey::generate().unwrap(), AgentKey::generate().unwrap());
        let policy = Policy::default();
        let msg = stranger.sign(me.id(), "hi", 1_000, "m1");
        let mut seen = Dedupe::new(16);
        assert_eq!(
            decide(&msg, me.id(), &policy, 1_000, &mut seen),
            Err(Reject::NotAllowlisted)
        );
    }

    #[test]
    fn pairing_kind_from_existing_peer_is_unexpected() {
        let (alice, me) = (AgentKey::generate().unwrap(), AgentKey::generate().unwrap());
        let policy = policy_for(&alice);
        let msg = alice.sign_as(me.id(), "x", 1_000, "k1", MessageKind::PairRequest);
        let mut seen = Dedupe::new(16);
        assert_eq!(
            decide(&msg, me.id(), &policy, 1_000, &mut seen),
            Err(Reject::Unexpected)
        );
    }

    #[test]
    fn pair_table_put_take_and_find() {
        let mut t = PairTable::new(8);
        t.put("aaaabbbbcccc".into(), "desktop".into());
        assert_eq!(t.get("aaaabbbbcccc"), Some(&"desktop".to_string()));
        assert_eq!(
            t.find("aaaabbbb"), // exact 8-char fingerprint
            Some(("aaaabbbbcccc".into(), "desktop".into()))
        );
        assert_eq!(t.take("aaaabbbbcccc"), Some("desktop".into()));
        assert!(t.is_empty());
    }
}
