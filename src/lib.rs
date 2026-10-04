//! Agent-to-agent messaging for Claude Code and Codex CLI.
//!
//! Claude Code and Codex CLI agents each run an MCP server: outbound is the
//! `send_message` tool. Claude inbound is delivered by default to a local inbox that a
//! background `interlink-mcp wait` listener drains (plain `claude`, no flags), with
//! native `notifications/claude/channel` push as an opt-in enhancement (`interlinked`
//! / `INTERLINK_CHANNELS=1`). A small **bus** routes messages between agents and
//! buffers for agents that are offline.
//! Codex CLI uses the same protocol and trust gate, with local lifecycle hooks
//! binding the MCP instance to a thread and [`codex`] delivering through its queue.
//!
//! ## Trust
//!
//! An agent's identity is its **Ed25519 public key** ([`identity`]); names are
//! local petnames. Every message is signed, and the channel server verifies the
//! signature and checks the sender against an allowlist *before* pushing —
//! so an unverified message never reaches the model.
//!
//! Authority comes from the server's `instructions` string, which lands in
//! the host's MCP context. The peer's text is untrusted data that parameterises
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

#[cfg(feature = "agent")]
pub mod codex;

#[cfg(feature = "agent")]
pub mod delivery;

#[cfg(feature = "identity")]
pub mod identity;

#[cfg(feature = "agent")]
pub mod policy;

#[cfg(feature = "agent")]
pub mod policy_store;

#[cfg(feature = "agent")]
pub mod pairing;

#[cfg(feature = "agent")]
pub mod inbox;

#[cfg(feature = "agent")]
pub mod mailbox;

#[cfg(feature = "agent")]
pub mod state;

#[cfg(feature = "agent")]
pub mod titles;

pub mod route;

#[cfg(feature = "persist")]
pub mod store;

/// Unix milliseconds.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
