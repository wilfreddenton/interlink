//! Ed25519 identity. **The public key is the identity**; names are local petnames.
//!
//! Claiming the name "alice" gets you nothing without her key, so the sender
//! gate in the channel server checks a signature rather than a string an agent
//! typed. This is the property the channel docs demand: *"gate on the sender's
//! identity."*
//!
//! Signing covers a domain-separated, length-prefixed canonical encoding, so a
//! signature can never be replayed into a different context and no pair of
//! fields can be shifted across their boundary.

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use ed25519_dalek::{
    PUBLIC_KEY_LENGTH, SECRET_KEY_LENGTH, SIGNATURE_LENGTH, Signature, Signer, SigningKey,
    VerifyingKey,
};
use serde::{Deserialize, Serialize};

/// Bound into every message signature. Bumped v1→v2 when task fields (`status`,
/// `task_id`, `in_reply_to`) entered the canonical encoding; a bump makes old and
/// new signatures mutually unverifiable.
const DOMAIN: &[u8] = b"interlink-v2\0";

/// What a signed message *is*. A plain `Message` is the everyday case; the pairing
/// kinds are the only thing a non-peer may deliver (a knock), gated specially.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageKind {
    #[default]
    Message,
    PairRequest,
    PairAccept,
}

impl MessageKind {
    fn as_str(self) -> &'static str {
        match self {
            MessageKind::Message => "message",
            MessageKind::PairRequest => "pair_request",
            MessageKind::PairAccept => "pair_accept",
        }
    }
}

/// The lifecycle marker on a task message. Absent on a plain chat turn, the
/// opening request, or an answer; present on the executor's replies about a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    /// Progress on a running task.
    Update,
    /// Blocked; needs an answer routed back to the requester's operator.
    NeedsInput,
    /// Terminal: finished successfully.
    Result,
    /// Terminal: finished with an error.
    Failed,
    /// Terminal: aborted by either side.
    Canceled,
}

impl TaskStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            TaskStatus::Update => "update",
            TaskStatus::NeedsInput => "needs_input",
            TaskStatus::Result => "result",
            TaskStatus::Failed => "failed",
            TaskStatus::Canceled => "canceled",
        }
    }

    /// Parse the tool-facing string form; `None` for an unrecognized value.
    pub fn from_tag(s: &str) -> Option<Self> {
        match s {
            "update" => Some(TaskStatus::Update),
            "needs_input" => Some(TaskStatus::NeedsInput),
            "result" => Some(TaskStatus::Result),
            "failed" => Some(TaskStatus::Failed),
            "canceled" => Some(TaskStatus::Canceled),
            _ => None,
        }
    }

    /// Terminal states close a task and cannot restart.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            TaskStatus::Result | TaskStatus::Failed | TaskStatus::Canceled
        )
    }
}

/// An agent's identity: its Ed25519 public key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentId(VerifyingKey);

impl AgentId {
    pub fn from_b64(s: &str) -> Result<Self> {
        let raw = B64.decode(s).context("agent id is not valid base64")?;
        let bytes: [u8; PUBLIC_KEY_LENGTH] = raw
            .try_into()
            .map_err(|_| anyhow!("agent id must be {PUBLIC_KEY_LENGTH} bytes"))?;
        Ok(Self(
            VerifyingKey::from_bytes(&bytes).context("not a valid ed25519 public key")?,
        ))
    }

    pub fn to_b64(self) -> String {
        B64.encode(self.0.as_bytes())
    }

    /// Short form for logs and `<channel>` tags. Not for authentication.
    pub fn fingerprint(self) -> String {
        self.to_b64().chars().take(8).collect()
    }

    pub fn as_verifying_key(&self) -> &VerifyingKey {
        &self.0
    }
}

/// An agent's secret key. Zeroized on drop by `ed25519-dalek`'s default features.
pub struct AgentKey(SigningKey);

impl AgentKey {
    /// Seeded straight from the OS CSPRNG.
    pub fn generate() -> Result<Self> {
        let mut secret = [0u8; SECRET_KEY_LENGTH];
        getrandom::fill(&mut secret).map_err(|e| anyhow!("OS entropy unavailable: {e}"))?;
        Ok(Self(SigningKey::from_bytes(&secret)))
    }

    pub fn from_b64(s: &str) -> Result<Self> {
        let raw = B64
            .decode(s.trim())
            .context("secret key is not valid base64")?;
        let bytes: [u8; SECRET_KEY_LENGTH] = raw
            .try_into()
            .map_err(|_| anyhow!("secret key must be {SECRET_KEY_LENGTH} bytes"))?;
        Ok(Self(SigningKey::from_bytes(&bytes)))
    }

    pub fn to_b64(&self) -> String {
        B64.encode(self.0.to_bytes())
    }

    pub fn id(&self) -> AgentId {
        AgentId(self.0.verifying_key())
    }

    /// Sign a plain message to `to`. `ts` and `msg_id` are covered, giving replay
    /// protection when paired with the receiver's dedupe set.
    pub fn sign(&self, to: AgentId, text: &str, ts: u64, msg_id: &str) -> SignedMessage {
        self.sign_full(to, text, ts, msg_id, MessageKind::Message, None, None, None)
    }

    /// Sign a message of a specific `kind` (plain, or a pairing knock/accept).
    pub fn sign_as(
        &self,
        to: AgentId,
        text: &str,
        ts: u64,
        msg_id: &str,
        kind: MessageKind,
    ) -> SignedMessage {
        self.sign_full(to, text, ts, msg_id, kind, None, None, None)
    }

    /// Sign a message carrying optional task-tracking metadata; all of it —
    /// `status`, `task_id`, `in_reply_to` — is covered by the signature, so a
    /// relay can't forge a `Canceled` or flip a status in flight.
    #[allow(clippy::too_many_arguments)]
    pub fn sign_full(
        &self,
        to: AgentId,
        text: &str,
        ts: u64,
        msg_id: &str,
        kind: MessageKind,
        task_id: Option<&str>,
        status: Option<TaskStatus>,
        in_reply_to: Option<&str>,
    ) -> SignedMessage {
        let bytes = canonical(
            self.id(),
            to,
            ts,
            msg_id,
            text,
            kind,
            task_id,
            status,
            in_reply_to,
        );
        let sig: Signature = self.0.sign(&bytes);
        SignedMessage {
            from: self.id().to_b64(),
            to: to.to_b64(),
            text: text.to_string(),
            ts,
            msg_id: msg_id.to_string(),
            kind,
            task_id: task_id.map(str::to_string),
            status,
            in_reply_to: in_reply_to.map(str::to_string),
            sig: B64.encode(sig.to_bytes()),
        }
    }

    /// Sign a presence announcement: "this key is online, calling itself `name`,
    /// as this live session". The session descriptor is signed too, so a peer can
    /// trust the cwd/repo/summary it picks from as much as the key.
    pub fn announce(&self, name: &str, session: &SessionInfo, ts: u64) -> Announcement {
        let sig: Signature = self
            .0
            .sign(&announce_canonical(self.id(), name, session, ts));
        Announcement {
            pubkey: self.id().to_b64(),
            name: name.to_string(),
            session: session.clone(),
            ts,
            sig: B64.encode(sig.to_bytes()),
        }
    }
}

/// A live session under a node identity: a random per-launch `session_id` plus the
/// descriptor a human recognizes it by. One identity hosts several at once; the id
/// is pure routing (`key#session_id`), the rest is for `discover`'s pick-list.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionInfo {
    pub session_id: String,
    #[serde(default)]
    pub cwd: String,
    #[serde(default)]
    pub git_root: String,
    #[serde(default)]
    pub summary: String,
}

/// A fresh random session id (8 lowercase hex chars). Random — not derived from
/// metadata — so two sessions in the same directory never collide; the id is only
/// a routing key, never shown to humans.
pub fn mint_session_id() -> Result<String> {
    let mut b = [0u8; 4];
    getrandom::fill(&mut b).map_err(|e| anyhow!("OS entropy unavailable: {e}"))?;
    Ok(b.iter().map(|x| format!("{x:02x}")).collect())
}

/// What travels over the bus. The bus treats it as an opaque payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SignedMessage {
    pub from: String,
    pub to: String,
    pub text: String,
    pub ts: u64,
    pub msg_id: String,
    #[serde(default)]
    pub kind: MessageKind,
    /// Task-tracking metadata (all optional; absent on a plain chat turn). See
    /// [`docs/TASKS.md`](../docs/TASKS.md).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<TaskStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<String>,
    pub sig: String,
}

impl SignedMessage {
    /// Verify the signature and return the *authenticated* sender.
    ///
    /// Uses `verify_strict`, which rejects small-order public keys and
    /// non-canonical signature encodings.
    pub fn verify(&self) -> Result<AgentId> {
        let from = AgentId::from_b64(&self.from)?;
        let to = AgentId::from_b64(&self.to)?;

        let raw = B64
            .decode(&self.sig)
            .context("signature is not valid base64")?;
        let sig_bytes: [u8; SIGNATURE_LENGTH] = raw
            .try_into()
            .map_err(|_| anyhow!("signature must be {SIGNATURE_LENGTH} bytes"))?;
        let sig = Signature::from_bytes(&sig_bytes);

        let bytes = canonical(
            from,
            to,
            self.ts,
            &self.msg_id,
            &self.text,
            self.kind,
            self.task_id.as_deref(),
            self.status,
            self.in_reply_to.as_deref(),
        );
        from.as_verifying_key()
            .verify_strict(&bytes, &sig)
            .map_err(|_| {
                anyhow!(
                    "signature does not verify for sender {}",
                    from.fingerprint()
                )
            })?;
        Ok(from)
    }
}

/// Domain-separated, length-prefixed. Length prefixes stop an attacker shifting
/// bytes across the `msg_id`/`text` boundary to forge a different message under
/// the same signature.
#[allow(clippy::too_many_arguments)]
fn canonical(
    from: AgentId,
    to: AgentId,
    ts: u64,
    msg_id: &str,
    text: &str,
    kind: MessageKind,
    task_id: Option<&str>,
    status: Option<TaskStatus>,
    in_reply_to: Option<&str>,
) -> Vec<u8> {
    // Optional fields encode as empty when absent — unambiguous, since a real
    // task_id/status/in_reply_to is never the empty string.
    let k = kind.as_str();
    let st = status.map(TaskStatus::as_str).unwrap_or("");
    let tid = task_id.unwrap_or("");
    let irt = in_reply_to.unwrap_or("");
    let mut b = Vec::with_capacity(DOMAIN.len() + 88 + k.len() + msg_id.len() + text.len());
    b.extend_from_slice(DOMAIN);
    b.extend_from_slice(from.as_verifying_key().as_bytes());
    b.extend_from_slice(to.as_verifying_key().as_bytes());
    b.extend_from_slice(&ts.to_le_bytes());
    // Every variable-length field is length-prefixed and in a fixed order, so no
    // bytes can shift across a field boundary to forge a different message.
    for field in [k, msg_id, text, st, tid, irt] {
        b.extend_from_slice(&(field.len() as u32).to_le_bytes());
        b.extend_from_slice(field.as_bytes());
    }
    b
}

/// Bound into presence announcements. Separate from the message `DOMAIN` so the
/// two version independently — a message-format change need not reissue the
/// announcement format, and vice versa.
const ANNOUNCE_DOMAIN: &[u8] = b"interlink-announce-v1\0";

/// A signed presence announcement, published to the bus roster. The `name` is a
/// self-claim; identity is the key, so a peer [`verify`](Announcement::verify)s
/// before ever trusting the name.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Announcement {
    pub pubkey: String,
    pub name: String,
    #[serde(default)]
    pub session: SessionInfo,
    pub ts: u64,
    pub sig: String,
}

impl Announcement {
    /// Verify the self-signature; returns the authenticated key on success.
    pub fn verify(&self) -> Result<AgentId> {
        let id = AgentId::from_b64(&self.pubkey)?;
        let raw = B64
            .decode(&self.sig)
            .context("announcement signature is not valid base64")?;
        let sig_bytes: [u8; SIGNATURE_LENGTH] = raw
            .try_into()
            .map_err(|_| anyhow!("signature must be {SIGNATURE_LENGTH} bytes"))?;
        let sig = Signature::from_bytes(&sig_bytes);
        id.as_verifying_key()
            .verify_strict(
                &announce_canonical(id, &self.name, &self.session, self.ts),
                &sig,
            )
            .map_err(|_| anyhow!("announcement does not verify for {}", id.fingerprint()))?;
        Ok(id)
    }
}

fn announce_canonical(pubkey: AgentId, name: &str, session: &SessionInfo, ts: u64) -> Vec<u8> {
    let mut b = Vec::with_capacity(ANNOUNCE_DOMAIN.len() + 64 + name.len());
    b.extend_from_slice(ANNOUNCE_DOMAIN);
    b.extend_from_slice(pubkey.as_verifying_key().as_bytes());
    // Length-prefix every variable field so no two distinct (name, session)
    // tuples can share an encoding.
    for field in [
        name,
        &session.session_id,
        &session.cwd,
        &session.git_root,
        &session.summary,
    ] {
        b.extend_from_slice(&(field.len() as u32).to_le_bytes());
        b.extend_from_slice(field.as_bytes());
    }
    b.extend_from_slice(&ts.to_le_bytes());
    b
}

/// Reject messages whose timestamp is too far from now, bounding replay windows.
pub fn check_freshness(ts: u64, now: u64, max_skew_ms: u64) -> Result<()> {
    let delta = now.abs_diff(ts);
    if delta > max_skew_ms {
        bail!("message timestamp is {delta}ms from now (max {max_skew_ms}ms)");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> AgentKey {
        AgentKey::generate().unwrap()
    }

    #[test]
    fn sign_then_verify_returns_the_sender() {
        let (alice, bob) = (key(), key());
        let msg = alice.sign(bob.id(), "hello", 1234, "m1");
        assert_eq!(msg.verify().unwrap(), alice.id());
    }

    #[test]
    fn tampered_text_fails() {
        let (alice, bob) = (key(), key());
        let mut msg = alice.sign(bob.id(), "transfer 1", 1234, "m1");
        msg.text = "transfer 1000".into();
        assert!(msg.verify().is_err());
    }

    #[test]
    fn tampered_recipient_fails() {
        let (alice, bob, eve) = (key(), key(), key());
        let mut msg = alice.sign(bob.id(), "hi", 1, "m1");
        msg.to = eve.id().to_b64();
        assert!(msg.verify().is_err());
    }

    #[test]
    fn forged_sender_fails() {
        // Eve signs, then claims to be Alice.
        let (alice, bob, eve) = (key(), key(), key());
        let mut msg = eve.sign(bob.id(), "hi", 1, "m1");
        msg.from = alice.id().to_b64();
        assert!(msg.verify().is_err());
    }

    #[test]
    fn ts_and_msg_id_are_covered() {
        let (alice, bob) = (key(), key());
        let orig = alice.sign(bob.id(), "hi", 1, "m1");
        for mut m in [orig.clone(), orig.clone()] {
            m.ts = 2;
            assert!(m.verify().is_err(), "ts must be signed");
        }
        let mut m = orig;
        m.msg_id = "m2".into();
        assert!(m.verify().is_err(), "msg_id must be signed");
    }

    #[test]
    fn length_prefixes_stop_boundary_shifting() {
        // ("ab", "c") and ("a", "bc") must not produce the same signed bytes.
        let (alice, bob) = (key(), key());
        let a = canonical(
            alice.id(),
            bob.id(),
            1,
            "ab",
            "c",
            MessageKind::Message,
            None,
            None,
            None,
        );
        let b = canonical(
            alice.id(),
            bob.id(),
            1,
            "a",
            "bc",
            MessageKind::Message,
            None,
            None,
            None,
        );
        assert_ne!(a, b);
    }

    #[test]
    fn domain_separation_is_bound_in() {
        let (alice, bob) = (key(), key());
        let bytes = canonical(
            alice.id(),
            bob.id(),
            1,
            "m1",
            "hi",
            MessageKind::Message,
            None,
            None,
            None,
        );
        assert!(bytes.starts_with(DOMAIN));
    }

    #[test]
    fn id_b64_round_trips() {
        let alice = key();
        let id = alice.id();
        assert_eq!(AgentId::from_b64(&id.to_b64()).unwrap(), id);
        assert_eq!(id.fingerprint().len(), 8);
    }

    #[test]
    fn secret_key_b64_round_trips() {
        let alice = key();
        let restored = AgentKey::from_b64(&alice.to_b64()).unwrap();
        assert_eq!(restored.id(), alice.id());
    }

    #[test]
    fn freshness_bounds_replay_window() {
        assert!(check_freshness(1_000, 1_500, 1_000).is_ok());
        assert!(check_freshness(1_000, 5_000, 1_000).is_err());
        assert!(
            check_freshness(5_000, 1_000, 1_000).is_err(),
            "future ts too"
        );
    }

    #[test]
    fn announcement_round_trips_and_rejects_tampering() {
        let alice = key();
        let session = SessionInfo {
            session_id: "a3f2c1".into(),
            cwd: "/home/alice/eden".into(),
            git_root: "eden".into(),
            summary: "installing deps".into(),
        };
        let a = alice.announce("alice-laptop", &session, 1234);
        assert_eq!(a.verify().unwrap(), alice.id());

        let mut tampered_name = a.clone();
        tampered_name.name = "eve-laptop".into();
        assert!(tampered_name.verify().is_err(), "name is signed");

        let mut tampered_session = a.clone();
        tampered_session.session.summary = "rm -rf /".into();
        assert!(
            tampered_session.verify().is_err(),
            "the session descriptor is signed"
        );

        let mut swapped_id = a.clone();
        swapped_id.session.session_id = "deadbeef".into();
        assert!(swapped_id.verify().is_err(), "session_id is signed");

        let mut forged_key = a;
        forged_key.pubkey = key().id().to_b64();
        assert!(
            forged_key.verify().is_err(),
            "can't reattribute to another key"
        );
    }

    #[test]
    fn task_fields_are_covered_by_signature() {
        let (alice, bob) = (key(), key());
        let mut m = alice.sign_full(
            bob.id(),
            "on it",
            1,
            "m1",
            MessageKind::Message,
            Some("task-1"),
            Some(TaskStatus::Update),
            None,
        );
        assert!(m.verify().is_ok());
        m.status = Some(TaskStatus::Canceled);
        assert!(m.verify().is_err(), "status must be signed");

        let mut m2 = alice.sign_full(
            bob.id(),
            "answer",
            1,
            "m2",
            MessageKind::Message,
            Some("task-1"),
            None,
            Some("q1"),
        );
        assert!(m2.verify().is_ok());
        m2.in_reply_to = Some("q2".into());
        assert!(m2.verify().is_err(), "in_reply_to must be signed");
    }

    #[test]
    fn kind_is_covered_by_signature() {
        let (alice, bob) = (key(), key());
        let mut m = alice.sign_as(bob.id(), "hi", 1, "m1", MessageKind::Message);
        assert!(m.verify().is_ok());
        // A message can't be re-typed into a pairing knock under the same signature.
        m.kind = MessageKind::PairRequest;
        assert!(m.verify().is_err(), "kind must be signed");
    }
}
