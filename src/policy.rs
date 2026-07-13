//! Per-peer authorization policy: `peers.json`.
//!
//! A peer is a **public key** with a local petname. Being on the list means the
//! peer is *admitted*: it is a trusted chat partner, and its messages are handled
//! inline. The **default for an unlisted peer is deny-everything**, so the safe
//! state is the fallback.
//!
//! ```json
//! {
//!   "my-laptop":  { "key": "8Emom3…" },
//!   "my-desktop": { "key": "rq2AzH…" }
//! }
//! ```
//!
//! Admission is all-or-nothing: interlink is a *chat* between mutually trusted
//! agents, not a sandbox for a semi-trusted one. (A legacy `"may"` field from
//! older files is accepted and ignored, so those files still load.)

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::identity::AgentId;

/// The on-disk form of one peer. Extra fields (e.g. a legacy `"may"`) are
/// ignored, so files written by older versions still parse.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RawPeer {
    key: String,
}

/// One resolved peer: its authenticated identity and local petname.
#[derive(Debug, Clone)]
pub struct Peer {
    pub petname: String,
    pub id: AgentId,
}

/// The whole allowlist. Parsing validates every key up front, so a malformed
/// `peers.json` fails loudly at startup rather than silently at message time.
#[derive(Debug, Clone, Default)]
pub struct Policy {
    peers: Vec<Peer>,
}

impl Policy {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading peers file {}", path.display()))?;
        Self::parse(&raw)
    }

    pub fn parse(raw: &str) -> Result<Self> {
        let map: BTreeMap<String, RawPeer> =
            serde_json::from_str(raw).context("peers file is not valid policy JSON")?;
        let mut peers = Vec::with_capacity(map.len());
        for (petname, rp) in map {
            let id = AgentId::from_b64(&rp.key)
                .with_context(|| format!("peer '{petname}' has an invalid key"))?;
            peers.push(Peer { petname, id });
        }
        // Two petnames for one key is almost certainly a mistake and makes the
        // authenticated-sender lookup ambiguous.
        for i in 0..peers.len() {
            for j in (i + 1)..peers.len() {
                if peers[i].id == peers[j].id {
                    bail!(
                        "peers '{}' and '{}' share the same key",
                        peers[i].petname,
                        peers[j].petname
                    );
                }
            }
        }
        Ok(Self { peers })
    }

    /// Look up an *authenticated* sender. `None` ⇒ not on the allowlist ⇒ drop.
    pub fn peer(&self, id: AgentId) -> Option<&Peer> {
        self.peers.iter().find(|p| p.id == id)
    }

    /// Resolve a petname to a key for outbound sends.
    pub fn resolve(&self, petname: &str) -> Result<AgentId> {
        self.peers
            .iter()
            .find(|p| p.petname == petname)
            .map(|p| p.id)
            .with_context(|| format!("unknown peer '{petname}'"))
    }

    pub fn len(&self) -> usize {
        self.peers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }

    /// All peers, for listing.
    pub fn peers(&self) -> &[Peer] {
        &self.peers
    }

    /// Admit a peer (or no-op if the same petname→key pair is already present).
    /// Rejects a petname already bound to a *different* key, or a key already
    /// held under a *different* petname — either would make the authenticated
    /// sender lookup ambiguous, exactly as [`Policy::parse`] guards against.
    pub fn add(&mut self, petname: &str, key_b64: &str) -> Result<()> {
        let id = AgentId::from_b64(key_b64)
            .with_context(|| format!("peer '{petname}' has an invalid key"))?;
        if let Some(other) = self
            .peers
            .iter()
            .find(|p| p.id == id && p.petname != petname)
        {
            bail!("that key is already authorized as '{}'", other.petname);
        }
        match self.peers.iter_mut().find(|p| p.petname == petname) {
            Some(existing) => existing.id = id,
            None => self.peers.push(Peer {
                petname: petname.to_string(),
                id,
            }),
        }
        Ok(())
    }

    /// Remove a peer by petname; returns whether one was removed.
    pub fn remove(&mut self, petname: &str) -> bool {
        let before = self.peers.len();
        self.peers.retain(|p| p.petname != petname);
        self.peers.len() != before
    }

    /// Serialize back to the `peers.json` object form.
    pub fn to_json(&self) -> Result<String> {
        let map: BTreeMap<String, RawPeer> = self
            .peers
            .iter()
            .map(|p| (p.petname.clone(), RawPeer { key: p.id.to_b64() }))
            .collect();
        serde_json::to_string_pretty(&map).context("serializing peers")
    }

    /// Persist the current allowlist to `path`.
    pub fn save(&self, path: &Path) -> Result<()> {
        let mut json = self.to_json()?;
        json.push('\n');
        std::fs::write(path, json).with_context(|| format!("writing {}", path.display()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::AgentKey;

    fn policy_with_key(k: &AgentKey) -> Policy {
        let raw = format!(r#"{{ "alice": {{ "key": "{}" }} }}"#, k.id().to_b64());
        Policy::parse(&raw).unwrap()
    }

    #[test]
    fn listed_key_resolves_to_its_petname() {
        let k = AgentKey::generate().unwrap();
        let p = policy_with_key(&k);
        assert_eq!(p.peer(k.id()).unwrap().petname, "alice");
    }

    #[test]
    fn legacy_may_field_is_ignored() {
        // Files written by older versions carried a `"may"`; they must still load.
        let k = AgentKey::generate().unwrap();
        let raw = format!(
            r#"{{ "alice": {{ "key": "{}", "may": "*" }} }}"#,
            k.id().to_b64()
        );
        let p = Policy::parse(&raw).unwrap();
        assert_eq!(p.peer(k.id()).unwrap().petname, "alice");
    }

    #[test]
    fn unlisted_sender_is_denied() {
        let k = AgentKey::generate().unwrap();
        let p = policy_with_key(&k);
        let stranger = AgentKey::generate().unwrap();
        assert!(
            p.peer(stranger.id()).is_none(),
            "unlisted key must not resolve"
        );
    }

    #[test]
    fn invalid_key_fails_at_parse_time() {
        let raw = r#"{ "alice": { "key": "not-a-real-key" } }"#;
        assert!(Policy::parse(raw).is_err());
    }

    #[test]
    fn duplicate_keys_are_rejected() {
        let k = AgentKey::generate().unwrap();
        let raw = format!(
            r#"{{ "a": {{ "key": "{0}" }}, "b": {{ "key": "{0}" }} }}"#,
            k.id().to_b64()
        );
        assert!(Policy::parse(&raw).is_err());
    }

    #[test]
    fn resolve_maps_petname_to_key() {
        let k = AgentKey::generate().unwrap();
        let p = policy_with_key(&k);
        assert_eq!(p.resolve("alice").unwrap(), k.id());
        assert!(p.resolve("nobody").is_err());
    }

    #[test]
    fn add_then_resolve_and_persist_round_trip() {
        let k = AgentKey::generate().unwrap();
        let mut p = Policy::default();
        p.add("desktop", &k.id().to_b64()).unwrap();
        assert_eq!(p.resolve("desktop").unwrap(), k.id());
        let reparsed = Policy::parse(&p.to_json().unwrap()).unwrap();
        assert_eq!(reparsed.resolve("desktop").unwrap(), k.id());
    }

    #[test]
    fn add_same_petname_is_idempotent() {
        let k = AgentKey::generate().unwrap();
        let mut p = Policy::default();
        p.add("bot", &k.id().to_b64()).unwrap();
        p.add("bot", &k.id().to_b64()).unwrap();
        assert_eq!(p.len(), 1, "same petname does not duplicate");
    }

    #[test]
    fn add_rejects_key_under_a_second_petname() {
        let k = AgentKey::generate().unwrap();
        let mut p = Policy::default();
        p.add("first", &k.id().to_b64()).unwrap();
        assert!(
            p.add("second", &k.id().to_b64()).is_err(),
            "one key must not get two petnames"
        );
    }

    #[test]
    fn add_rejects_invalid_key() {
        let mut p = Policy::default();
        assert!(p.add("x", "not-a-key").is_err());
    }

    #[test]
    fn remove_reports_whether_it_removed() {
        let k = AgentKey::generate().unwrap();
        let mut p = Policy::default();
        p.add("gone", &k.id().to_b64()).unwrap();
        assert!(p.remove("gone"));
        assert!(!p.remove("gone"), "already absent");
        assert!(p.is_empty());
    }
}
