# Design notes

## What this is

Two independent Claude Code sessions exchange signed messages through a small
local bus. Each runs a **channel server** (`escapement-agent`) — an MCP server
that declares Claude Code's `claude/channel` capability, so its notifications are
pushed straight into a live session. The interesting problem isn't moving bytes;
it's letting an agent accept work from a peer it doesn't fully trust.

## How we got here (and what it rules out)

The runtime shapes the design, and the docs were wrong or silent on several
points, so the facts below were established by experiment (see
[`experiments/`](experiments)):

- **A Claude Code agent isn't a persistent process** — it exists only during a
  turn. So a peer's message has to be *pushed in* from outside; the agent can't
  sit and listen.
- **Claude Code channels do exactly that push** (`notifications/claude/channel`).
  Before finding them, an earlier version of this project reproduced the
  mechanism by hand with a Stop hook that kept a background long-poll re-armed —
  it worked, but channels are the supported path, so it was retired to
  [`contrib/stop-hook.sh`](contrib/stop-hook.sh) as the fallback for environments
  where channels can't run (Bedrock/Vertex/Foundry, or orgs without them).
- **Channels only arm with an interactive TTY**; headless `claude -p` connects
  the MCP server but never engages the channel subsystem. The integration tests
  drive a real session through a PTY for this reason.
- **`rmcp` can be a channel**: declare `experimental: {"claude/channel": {}}` and
  push `CustomNotification::new("notifications/claude/channel", …)`. Verified on
  the wire, so the project stays pure Rust.

## The three gates

Authorization is enforced in three deterministic places, none of which trusts the
model's judgment about untrusted text.

**1. Signature + allowlist (`identity`, `policy`, `agent::decide`).** Every
message is signed over a domain-separated, length-prefixed encoding
(`"escapement-v1\0" ‖ from ‖ to ‖ ts ‖ len(msg_id) ‖ msg_id ‖ len(text) ‖ text`)
and verified with `verify_strict` (rejects small-order keys and non-canonical
signatures). The authenticated key — never the claimed `from` string — is looked
up in `peers.json`. Unknown key ⇒ dropped. `ts` and `msg_id` are inside the
signature, so a bounded dedupe set plus a freshness window give replay
protection.

**2. `PreToolUse` guard on `fetch_request`
([`contrib/pretooluse-guard.sh`](contrib/pretooluse-guard.sh)).** A scoped peer's
body is withheld from the push; only metadata (a `msg_id` and a capability name)
reaches the main agent. The body is retrievable *only* through the
`fetch_request` MCP tool, and the guard denies that tool unless the call comes
from inside a subagent (`agent_type` is present on the hook's stdin there, and
absent for the main agent — verified). So the main agent physically cannot pull
untrusted text into its own long-lived context.

**3. Agent `tools:` frontmatter is the capability.** A capability is a named
agent definition (`.claude/agents/<name>.md`) whose `tools:` line lists what its
subagent may use. A subagent cannot call a tool it wasn't given — verified: a
`Read`-only subagent asked to run a shell command simply has no shell. So the
per-peer grant in `peers.json` maps to an agent definition, and enforcement is
the runtime's, not a hook's pattern-matching.

## Why sending is a tool but receiving is a channel

Sending is a discrete, model-initiated action carrying free text — a natural MCP
tool (`send_message`), where the payload is a JSON string that never touches a
shell. Receiving must *wake* a session with an event, which only the channel push
can do. That asymmetry is principled, not accidental: send is a tool because the
payload is untrusted text; receive is a channel because nothing else pushes.

## The bus

A dumb broker: one bounded FIFO per recipient key, plain HTTP on loopback. It
routes an opaque payload and buffers for offline recipients; it never verifies a
signature and holds no keys. Bounded because a peer that never returns would
otherwise grow its queue without limit — drop-oldest, logged. Pop-on-read (no
lease/ack): the only crash window is a session already being torn down.

## No TLS, on purpose

The bus is loopback and the messages are signed, so TLS would add confidentiality
we don't need against a threat (a local root attacker) we don't defend. Dropping
it removes `ring` — the tree's only C dependency — which is what makes the
binaries pure-Rust and statically linkable with nothing but `rustup target add`.
Authenticity moved from the transport (where it was C-shaped) to the message
(pure Rust, and, unlike TLS, intact through an untrusted bus).

## Crate shape

One crate, feature-gated (`bus` / `agent` / `identity`); optional dependencies
mean the identity-only build pulls a fraction of the tree. Binaries use
`required-features`. CI runs `cargo hack --feature-powerset` so a `#[cfg]` typo
can't pass locally and break a user, and asserts no C dependency reappears.

## Prior art

The containment pattern isn't novel in computing, only in this setting:
**systemd socket activation** (a supervisor holds the armed listener and wakes an
inactive service), and object-capability systems (authority is an unforgeable
reference, not an ambient permission — here, a signed key rather than "whoever
can reach the socket"). Within Claude Code, **Agent Teams** independently arrived
at the same trust boundary — a message from another agent is treated as untrusted
input, not operator consent — which is strong corroboration that the boundary is
the right one.
