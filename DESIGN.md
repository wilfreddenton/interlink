# Design notes

## What this is

Two independent Claude Code sessions exchange signed messages through a small
bus. Each runs a **channel server** (`interlink-mcp`) — an MCP server that
declares Claude Code's `claude/channel` capability, so its notifications are
pushed straight into a live session. The interesting problem isn't moving bytes;
it's letting an agent know, cryptographically, **who** it's talking to — and
letting a human decide who's admitted — over a bus that verifies nothing.

## How we got here (and what it rules out)

The runtime shapes the design, and the docs were wrong or silent on several
points, so the facts below were established empirically against a real session:

- **A Claude Code agent isn't a persistent process** — it exists only during a
  turn. So a peer's message has to be *pushed in* from outside; the agent can't
  sit and listen.
- **Claude Code channels do exactly that push** (`notifications/claude/channel`).
  Before finding them, an earlier version reproduced the mechanism by hand with a
  Stop hook that kept a background long-poll re-armed — it worked, but channels
  are the supported path, so it was retired.
- **Channels only arm with an interactive TTY**; headless `claude -p` connects
  the MCP server but never engages the channel subsystem.
- **`rmcp` can be a channel**: declare `experimental: {"claude/channel": {}}` and
  push `CustomNotification::new("notifications/claude/channel", …)`. Verified on
  the wire, so the project stays pure Rust.

## The trust gate

Authorization is one deterministic gate, in `agent::decide`, that never trusts the
model's judgment. Every inbound message runs:

```
verify signature → sender on the allowlist? → addressed to me? → fresh? → not a replay?
```

The message is signed over a domain-separated, length-prefixed encoding
(`"interlink-v1\0" ‖ from ‖ to ‖ ts ‖ kind ‖ msg_id ‖ text`) and verified with
`verify_strict` (rejects small-order keys and non-canonical signatures). The
**authenticated** key — never the claimed `from` string — is looked up in
`peers.json`; an unknown key is dropped before the model ever sees it. `ts` and
`msg_id` are inside the signature, so a bounded dedupe set plus a freshness window
give replay protection. The lone relaxation is discovery: a *non-peer* may deliver
one thing only — a signed pairing **knock** (identity + a self-claimed name, no
free text). Everything else from a non-peer is dropped. See
[`docs/DISCOVERY.md`](docs/DISCOVERY.md).

An admitted peer is trusted fully: its message is delivered inline. There is no
half-trust tier.

## Why there is no capability sandbox

An earlier design added one: a peer could be granted a *capability* rather than
full trust, and its requests were quarantined and handled by a disposable
subagent whose `tools:` frontmatter was the hard limit. It was elegant, and it was
removed, because it doesn't actually deliver its own promise.

The quarantine protects **one** direction — the receiver, from the sender's
request. But collaboration is bidirectional: a useful request has a **reply**, and
the caller has to consume it. To consume a semi-trusted peer's reply *safely* you
would have to quarantine it too, at which point the answer is stuck behind your
own sandbox and useless; to consume it *usefully* you ingest it inline, at which
point the sandbox bought nothing on the return path. Either way the capability
mode fails to make bidirectional work with an untrusted peer safe.

The honest conclusion: **safe bidirectional agent collaboration requires mutual
trust** — you cannot sandbox your way around the replies you must read. So
interlink establishes trust *cryptographically* and gates admission through a
*human*, and treats an admitted peer as a full chat partner. Trust is the decision
you make once, at pairing; there is no pretense of containing a collaborator you
don't trust.

## Why sending is a tool but receiving is a channel

Sending is a discrete, model-initiated action carrying free text — a natural MCP
tool (`send_message`), where the payload is a JSON string that never touches a
shell. Receiving must *wake* a session with an event, which only the channel push
can do. That asymmetry is principled: send is a tool because the payload is
untrusted text; receive is a channel because nothing else pushes.

## The bus

A dumb broker: one bounded FIFO per recipient key, plain HTTP. It routes an opaque
payload and buffers for offline recipients; it never verifies a signature and
holds no keys. Bounded because a peer that never returns would otherwise grow its
queue without limit — drop-oldest, logged.

**Keep-until-ack, durable.** A message stays in the queue until the recipient acks
it, and the queue lives in a pure-Rust ACID store
([redb](https://crates.io/crates/redb)), so a bus restart (a laptop that sleeps or
reboots) loses nothing queued for an offline agent. Delivery is at-least-once — a
crash between delivery and ack redelivers — which is safe because the receiver
dedupes by `msg_id`. redb is the single seam — one synchronous API wrapped in
`spawn_blocking` — chosen over SQLite (C) and Turso (whose SDK still pulls C via
`bindgen`, and which had an open silent-data-loss bug).

**The agent store is always in-memory.** interlink installs as a user-scope plugin,
so every Claude Code session on a machine spawns its own `interlink-mcp`; a shared
on-disk redb is single-writer, so the second session would fail to open it and boot
with no tools. Each session therefore keeps its outbox + log in RAM — isolated, so
no collision and no cleanup, and it survives sleep (suspend freezes the process with
RAM intact). The **bus is the durable layer**: a message that reached it stays
keep-until-ack durable for an offline recipient. The only loss window is an outbox
message queued *while the bus itself was unreachable*, dropped on a hard restart —
and even that survives sleep. (`INTERLINK_AGENT_DB` is still accepted but ignored.)

## No TLS, on purpose

The bus is loopback (or a trusted tailnet) and the messages are signed, so TLS
would add confidentiality against a threat we don't defend at that layer. Dropping
it removes `ring` — the tree's only C dependency — which is what makes the
binaries pure-Rust and statically linkable with nothing but `rustup target add`.
Authenticity moved from the transport (where it was C-shaped) to the message (pure
Rust, and, unlike TLS, intact through an untrusted bus).

The trade-off to name honestly: signed ≠ confidential. A relay that terminates the
connection can read message bodies in cleartext. That's fine for a loopback or an
operator-run tailnet relay; before federating through a relay you *don't* control,
bodies would need sealing to the recipient's key (Ed25519 → X25519 + `crypto_box`,
still pure-Rust) — deferred until that's a real need (see
[`DIRECTORY.md`](DIRECTORY.md)).

## Crate shape

One crate, feature-gated (`bus` / `agent` / `identity` / `persist`); optional
dependencies mean the identity-only build pulls a fraction of the tree. Binaries
use `required-features`. CI runs `cargo hack --feature-powerset` so a `#[cfg]`
typo can't pass locally and break a user, and asserts no C dependency reappears.
`interlink` is taken on crates.io, so the package publishes as `interlink-mcp`
while the library keeps the short name (`use interlink::`).

## Prior art & positioning

- **Identity is the key** — this is `did:key` (the identifier *is* the public key)
  with SSH/`age`-style TOFU pinning; petnames that are valid only locally while the
  key is valid globally are exactly SDSI's linked local names. All independently
  arrived at, and deliberately kept simple: interlink has no third-party verifier
  and no delegation chain, so a transferable-capability token system (UCAN,
  biscuit, macaroons) would be machinery it can't use.
- **Signed-over-untrusted-transport** — authenticity that survives a relay is the
  property capability-token systems also have; here it comes from signing the
  message rather than the connection.
- **Agent Teams**, Claude Code's built-in multi-agent feature, independently treats
  a message from another agent as untrusted input rather than operator consent —
  corroboration that the boundary is right. interlink's distinct territory is
  *cross-machine* peers with *cryptographic* identity, where Agent Teams is
  same-host and lead-spawned.
