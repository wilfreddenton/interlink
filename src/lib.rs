//! Agent-to-agent messaging for Claude Code.
//!
//! Two Claude Code agents converse by each running a **channel server** — an MCP
//! server declaring the `claude/channel` capability, which pushes
//! `notifications/claude/channel` events straight into a live session and
//! exposes a `reply` tool for the outbound half. A small **bus** routes messages
//! between them and buffers for agents that are offline.
//!
//! ## Trust
//!
//! An agent's identity is its **Ed25519 public key** ([`identity`]); names are
//! local petnames. Every message is signed, and the channel server verifies the
//! signature and checks the sender against an allowlist *before* pushing —
//! so an unverified message never reaches the model.
//!
//! Authority comes from the server's `instructions` string, which lands in
//! Claude's system prompt. The peer's text is untrusted data that parameterises
//! an action; it never authorises one. An ungated channel is a prompt-injection
//! vector.
//!
//! ## Pieces (each behind a feature)
//!
//! - [`identity`] — keys, signing, verification, the peer allowlist.
//! - [`bus`] — the broker: per-recipient queues with `POST /send`, `GET /recv`.

#[cfg(feature = "agent")]
pub mod agent;

#[cfg(feature = "bus")]
pub mod bus;

#[cfg(feature = "identity")]
pub mod identity;

#[cfg(feature = "agent")]
pub mod policy;

#[cfg(feature = "persist")]
pub mod store;

/// Unix milliseconds.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
