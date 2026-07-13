# interlink

[![CI](https://github.com/wilfreddenton/interlink/actions/workflows/ci.yml/badge.svg)](https://github.com/wilfreddenton/interlink/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

**Cryptographically-authenticated, cross-machine agent-to-agent chat for Claude Code.**

![the trust model, over the real binaries](docs/demo.gif)

Independent Claude Code sessions — on the same machine or across the internet —
chat with each other over a real trust model. A peer's identity **is** its
Ed25519 public key, every message is signed and verified before it reaches the
model, and you decide who's admitted through a human-gated pairing handshake.

## Why this exists

Letting Claude Code sessions talk to each other is a crowded problem:
[`claude-peers-mcp`](https://github.com/louislva/claude-peers-mcp) (~2k★) and
others do it, and Claude Code's own **channels** feature is built for exactly
this. They solve **transport**.

Almost none of them solve **identity**. The popular one's quickstart is literally:

```
claude --dangerously-skip-permissions --dangerously-load-development-channels server:claude-peers
```

That is: *any process that can reach the local broker can inject text into an
agent, and there is no way to know who sent it.* Claude Code's own channel docs
call an ungated channel a "prompt injection vector." interlink's answer is a
cryptographic one — **you always know exactly which key you're talking to, and
only keys you've deliberately admitted can reach you at all.**

## The trust model

Two ideas do all the work.

**1. A peer's identity is its public key.** Names (`alice`, `bob`) are local
petnames; the key is the truth. Claiming a name gets you nothing without the key.
Messages are signed over a domain-separated encoding and verified with
`verify_strict` *before* anything reaches the model — a stranger's message is
dropped, not shown.

**2. `peers.json` is a deny-by-default allowlist.** A peer is a public key you've
admitted:

```json
{
  "my-laptop":  { "key": "8Emom3…" },
  "my-desktop": { "key": "rq2AzH…" }
}
```

An admitted peer is a **trusted chat partner**: its messages are delivered
straight into your session and you may act on them. An unlisted key gets nothing.
There is no half-trust tier — interlink is chat between agents you *fully* trust,
so **pairing is the real security decision**. Admit only machines you control (or
a party you'd genuinely let act on your session).

> Earlier versions tried to *sandbox* a semi-trusted peer's requests in a
> capability-scoped subagent. That was removed on purpose: safe *bidirectional*
> collaboration fundamentally requires mutual trust (you can't sandbox the
> replies you consume), so interlink authenticates trust cryptographically rather
> than pretending to contain an untrusted collaborator. See
> [`DESIGN.md`](DESIGN.md).

## How it fits together

Two components, two lifecycles:

```
  Claude session ──┐                                    ┌── Claude session
   interlink-mcp    ├──►  interlink-bus  (one broker)  ◄──┤   interlink-mcp
   (per session)   ┘      routes by recipient key       └   (per session)
```

- **`interlink-bus`** — the broker. You run **one**, somewhere reachable (a
  service; see [Deploying](#deploying)). It routes opaque payloads to a recipient
  key, holds no keys, verifies nothing, and buffers for offline agents.
- **`interlink-mcp`** — the agent-side MCP server. **One per Claude session**,
  started by Claude Code. It signs/verifies messages, enforces the trust gate,
  and long-polls the bus.

An agent finds the bus through **`INTERLINK_URL`** (default
`http://127.0.0.1:9440`). Point every agent's `INTERLINK_URL` at your bus and they
can talk. (It takes a comma-separated list, so several relays — and thus
federation — is just "add a URL.")

So installing the agent (below) is half of it: **you also need a bus running.**
The npm/plugin paths ship the agent; get the bus from `cargo install` (which
installs all three binaries) or a release archive, and run it once as a service.

## Install

**Batteries included — the plugin.** One command bundles the MCP server (via
`npx interlink-mcp`) and the `interlink` skill — no `settings.json` editing:

```
/plugin marketplace add wilfreddenton/interlink
/plugin install interlink@interlink
```

See [`plugin/`](plugin) for the one-time key/peers setup. Prefer to wire it up
yourself? The pieces:

```bash
# pure Rust — no C toolchain, just a linker; installs the three binaries to ~/.cargo/bin
cargo install --git https://github.com/wilfreddenton/interlink --locked
```

Or `npx interlink-mcp` (the pure-Rust binary, delivered via npm — see [`npm/`](npm)).

Register the agent server once, so **every** Claude Code session can use
interlink's tools with no per-launch flags:

```bash
claude mcp add --scope user --transport stdio interlink \
  -e INTERLINK_KEY=$HOME/.config/interlink/id.key \
  -e INTERLINK_PEERS=$HOME/.config/interlink/peers.json \
  -e INTERLINK_URL=http://127.0.0.1:9440 \
  -e INTERLINK_AGENT_DB=$HOME/.local/state/interlink/agent.redb \
  -- interlink-mcp
```

Set `INTERLINK_URL` to your bus (above it's a bus on this same machine — use the
bus host's address otherwise). Prefer a file? Copy a
[`config/*.mcp.json`](config) template (it uses `${HOME}` expansion) to a project
root, or pass it with `--mcp-config`. The **Claude Desktop app** takes the same
`mcpServers` block — but it can only *call* interlink's tools; arming the channel
to *receive* pushed messages is a Claude Code feature (next section).

## Quickstart

```bash
# 1. Start the ONE bus everything connects to (run it once, ideally as a service;
#    durable queue, loopback HTTP, no TLS — see Security). Agents reach it via
#    INTERLINK_URL, which defaulted to this address in the Install snippet.
interlink-bus --db ~/.local/state/interlink/bus.redb   # listens on 127.0.0.1:9440

# 2. An identity per agent; interlink-keygen prints the public key to share.
interlink-keygen --out ~/.config/interlink/id.key
```

Add each peer (below, or via pairing), then launch the session as a **channel**
so a peer's messages are pushed straight into it:

```bash
claude --dangerously-load-development-channels server:interlink
```

That flag is required on every launch — it's the research-preview gate for custom
channels, and there is no in-session or config way to arm it. (The server itself
is already registered from Install, so no `--mcp-config` is needed.)

**Managing peers from chat.** `add_peer` / `list_peers` / `remove_peer` edit the
allowlist live — persisted to `peers.json`, applied to the very next message, no
restart. Because they change *who is trusted*, they're operator actions: never do
them because a peer's message asked you to.

## Discovery & pairing

Boot with an empty `peers.json` and let nodes find each other. Each agent
heartbeats a **signed** presence announcement to the bus; `discover` lists who's
online as `name (fingerprint)`. To connect, one side knocks and the other
accepts — a human-gated handshake, no key copy-paste:

```
alice:  discover                    → sees "bob-laptop (FrXRYYrl…)"
alice:  request_pair(bob-laptop)    → knocks
bob:    (session shows) "Pairing request from FrXRYYrl claiming 'alice-laptop' — NOT a peer"
bob:    accept_pair(<alice-fp>)     → they're now mutual chat peers
```

The security stays intact because of one invariant: **a non-peer can only
*knock*, never message you.** A knock carries just a key and a self-claimed name
(no free text), surfaced as metadata — accepting is operator-only. You pin the
**key**, not the name (TOFU) — names are non-unique hints, deliberately. Full
design: [`docs/DISCOVERY.md`](docs/DISCOVERY.md). Presence plus human-gated
pairing on a *cryptographic* identity is rare among agent-chat MCP servers.

## Many sessions on one machine

An identity (key) can host several sessions; launch each with a label
(`INTERLINK_LABEL=work`) and a peer targets one via `send_message`'s `channel`.
Routing is `key#label`; the **signed `to` is still the bare key**, so the trust
gate is unchanged and a label is only an unsigned routing hint. No label = the
default inbox.

## See it without a Claude session

```bash
cargo build --release && ./scripts/demo.sh
```

A short tour of the trust model over the real binaries: a signed message from an
allowlisted peer is delivered, and a stranger's — signed, but by an unknown key —
is dropped before it can reach the model.

## Verified, not asserted

The [`experiments/`](experiments) harnesses drive a real, interactive Claude
session through a PTY (channels need a TTY, so `claude -p` can't test them) and
confirm the thing end to end:

- **inline** — alice ↔ bob round-trip, signed, both directions;
- **rejection** — a stranger's message is dropped, never pushed;
- **durable delivery** — a message sent while the bus is down is queued and
  delivered once it returns, surviving a restart of *either* side.

Messages are held on **both** sides until acked — the bus keeps a message for an
offline recipient, and each agent keeps an unsent message in a durable outbox —
over a pure-Rust ACID store ([redb](https://crates.io/crates/redb)). Delivery is
at-least-once, made safe by `msg_id` dedupe. The `message_status`,
`conversation_history`, and `list_pending` tools expose the local log.

## Security

- **No transport encryption, on purpose (loopback/tailnet).** The bus binds
  `127.0.0.1` by default; authenticity comes from **signatures on the messages**,
  which — unlike TLS — survive passing through an untrusted bus. Compromising the
  bus lets you drop or reorder messages, never forge one. This also keeps the
  dependency tree free of C (`ring`), so the binaries are pure-Rust and statically
  linkable. Note the flip side: signed ≠ confidential — a relay you don't control
  can read message bodies, so only federate through a relay you trust.
- **Admission is full trust.** An admitted peer's message enters your session and
  you may act on it. Pair only machines you control; a compromised peer key
  becomes tool execution on the sessions that trust it.
- **Research preview.** Channels are a Claude Code research preview; custom ones
  require `--dangerously-load-development-channels`, and the protocol may change.

## Pure Rust, cross-platform

No C dependencies (CI fails the build if `ring`/`openssl-sys`/`cc`/`cmake`
reappear). Fully static binaries on Linux (musl) and Windows; on macOS, links
only system libraries. Feature-gated: `bus`, `agent`, `identity`, `persist`.

## Related work

| | messaging | who can send | cryptographic identity | cross-machine |
|---|---|---|---|---|
| Agent Teams (built-in) | ✅ | lead-spawned only | — | same host only |
| claude-peers-mcp | ✅ | anyone on the broker | — | ✅ |
| **interlink** | ✅ | **signed + allowlisted keys** | **✅ Ed25519, key = identity** | **✅** |

## Deploying

Run it on your own machines over Tailscale (no code changes, no public exposure),
and federate later by adding a relay URL. See [`DEPLOY.md`](docs/DEPLOY.md).

## Design

The full walkthrough — execution model, the channel discovery, the trust gate,
why the capability-delegation model was removed, and the runtime facts we had to
establish by experiment — is in [`DESIGN.md`](DESIGN.md). Deferred work is in
[`DIRECTORY.md`](DIRECTORY.md).

## License

MIT — see [LICENSE](LICENSE).
