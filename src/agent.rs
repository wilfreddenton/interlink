//! The per-agent channel server and its decision logic.
//!
//! Inbound flow, for each message drained from the bus:
//!   verify signature → addressed to me? → sender on the allowlist? → fresh? →
//!   not a replay? → dispatch.
//!
//! An admitted peer is handled **inline**: the message is pushed straight into
//! the session as a `<channel>` event. A non-peer may only *knock* to pair;
//! anything else from a non-peer is dropped at the gate.

use std::collections::{HashMap, VecDeque};

use crate::identity::{AgentId, MessageKind, SignedMessage, TaskStatus, check_freshness};
use crate::policy::Policy;

/// How far ahead of local time a message may be dated — a tight clock-skew bound; a
/// message from the future is a forgery/replay signal.
pub const MAX_FUTURE_MS: u64 = 60_000;

/// How long after signing a message may still be delivered. Generous, because the
/// bus is a durable keep-until-ack store: an offline or slow peer (or a Claude Code
/// stdio-server restart gap) can leave a legitimate message queued for a long time,
/// and it must not be rejected as stale when it finally lands. The replay this widens
/// is bounded in practice by the dedupe set and the trusted-tailnet threat model.
pub const MAX_PAST_MS: u64 = 86_400_000; // 24h

/// What to do with a verified, authorized message.
#[derive(Debug, PartialEq, Eq)]
pub enum Dispatch {
    /// Trusted peer: push the full content into the session now. Task-tracking
    /// metadata (if any) rides along so the session can branch on it — e.g. a
    /// `NeedsInput` is surfaced to the operator, a terminal status closes the loop.
    Inline {
        petname: String,
        text: String,
        task_id: Option<String>,
        status: Option<TaskStatus>,
        in_reply_to: Option<String>,
    },
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
    //    slot (so non-peers can't flood the replay set). One implicit exception:
    //    a message from our *own* key is another live session on this same node —
    //    same principal, so it's trusted without a `peers.json` entry. Only the
    //    holder of our secret key can produce such a signature, so this grants
    //    nothing to anyone else.
    let peer = policy.peer(from);
    let is_self = from == me;
    if peer.is_none() && !is_self && msg.kind == MessageKind::Message {
        return Err(Reject::NotAllowlisted);
    }

    // 4. Plausible timestamp: tight in the future (skew), generous in the past (the
    //    durable bus may hold a legitimate message for a while before it's delivered).
    check_freshness(msg.ts, now, MAX_FUTURE_MS, MAX_PAST_MS).map_err(|_| Reject::Stale)?;

    // 5. Not already seen. Do this after the cheap rejects so a replayed *valid*
    //    message is recorded only once and invalid ones never consume a slot.
    if !seen.insert(&msg.msg_id) {
        return Err(Reject::Replay);
    }

    match (peer, msg.kind) {
        (Some(peer), MessageKind::Message) => Ok(Dispatch::Inline {
            petname: peer.petname.clone(),
            text: msg.text.clone(),
            task_id: msg.task_id.clone(),
            status: msg.status,
            in_reply_to: msg.in_reply_to.clone(),
        }),
        // Another session under our own identity (see is_self, above).
        (None, MessageKind::Message) if is_self => Ok(Dispatch::Inline {
            petname: "self".to_string(),
            text: msg.text.clone(),
            task_id: msg.task_id.clone(),
            status: msg.status,
            in_reply_to: msg.in_reply_to.clone(),
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

/// A bounded set of recently-seen `msg_id`s. Its main job is collapsing the bus's
/// at-least-once redelivery (the same message seen twice before it's acked), which is
/// a near-term window, so a bounded LRU suffices. It is *not* sized to cover the full
/// [`MAX_PAST_MS`] window: a replay older than this capacity but still within the past
/// bound could slip through — an accepted tradeoff under the signed, trusted-tailnet
/// threat model, in exchange for durable delivery to offline/slow peers.
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
                text: "run the deploy".into(),
                task_id: None,
                status: None,
                in_reply_to: None,
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
    fn own_key_is_trusted_without_a_peers_entry() {
        // Another session on the same node signs with our own key. It's admitted
        // implicitly — no self-entry in peers.json — and shown as "self".
        let me = AgentKey::generate().unwrap();
        let policy = Policy::default(); // we are NOT in our own allowlist
        let msg = me.sign(me.id(), "from my other session", 1_000, "m1");
        let mut seen = Dedupe::new(16);
        assert_eq!(
            decide(&msg, me.id(), &policy, 1_000, &mut seen),
            Ok(Dispatch::Inline {
                petname: "self".into(),
                text: "from my other session".into(),
                task_id: None,
                status: None,
                in_reply_to: None,
            })
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
        // Past the generous past bound → stale.
        let long_after = 1_000 + MAX_PAST_MS + 1;
        assert_eq!(
            decide(&msg, me.id(), &policy, long_after, &mut seen),
            Err(Reject::Stale)
        );
        // A future-dated message is rejected by the tight future bound.
        let future_msg = alice.sign(me.id(), "hi", 1_000 + MAX_FUTURE_MS + 1, "m2");
        assert_eq!(
            decide(&future_msg, me.id(), &policy, 1_000, &mut seen),
            Err(Reject::Stale)
        );
        // A message delivered a few minutes late (well within the past bound) is fine.
        let ok_msg = alice.sign(me.id(), "hi", 1_000, "m3");
        assert!(decide(&ok_msg, me.id(), &policy, 1_000 + 300_000, &mut seen).is_ok());
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
        assert_eq!(
            t.find("aaaabbbbcccc"), // exact full key
            Some(("aaaabbbbcccc".into(), "desktop".into()))
        );
        assert_eq!(
            t.find("aaaabbbb"), // exact 8-char fingerprint
            Some(("aaaabbbbcccc".into(), "desktop".into()))
        );
        assert_eq!(t.take("aaaabbbbcccc"), Some("desktop".into()));
        assert!(t.is_empty());
    }
}
