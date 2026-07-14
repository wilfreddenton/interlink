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

So installing the agent (below) is half of it: **you also need a bus running.** The
plugin ships the agent; the bus comes from the release archive (all three binaries)
or `cargo install`, and you run it once as a service.

## Install

interlink ships as a **Claude Code plugin**. One command registers the MCP server
(the pure-Rust `interlink-mcp` binary, fetched via `npx interlink-mcp`), the
`interlink` skill, and the progress hook in **every** session — no `settings.json`
editing:

```
/plugin marketplace add wilfreddenton/interlink
/plugin install interlink@interlink
```

That's the agent. Two one-time steps and you're live:

**1. An identity + the one bus.** The plugin ships only the agent; you also need a
keypair and a single bus for agents to reach each other through. Download the
[release archive](https://github.com/wilfreddenton/interlink/releases/latest) for
your platform (all three static binaries) — or build them with
`cargo install --git https://github.com/wilfreddenton/interlink --locked` (pure
Rust, no C toolchain) — then:

```bash
mkdir -p ~/.config/interlink ~/.local/state/interlink
interlink-keygen --out ~/.config/interlink/id.key     # prints your public key to share
printf '{}\n' > ~/.config/interlink/peers.json         # add peers via pairing or add_peer
interlink-bus --db ~/.local/state/interlink/bus.redb   # the ONE bus — 127.0.0.1:9440
```

Run the bus once, ideally as a service (durable queue, loopback HTTP, no TLS — see
[Security](#security)). Every agent finds it through **`INTERLINK_URL`** (default
`http://127.0.0.1:9440`); point that at the bus host if the bus is elsewhere, and
see [Deploying](#deploying) for a Tailscale setup.

**2. Launch the session as a channel** so a peer's messages are pushed into it:

```bash
claude --dangerously-load-development-channels server:interlink
```

That flag is required on every launch — the research-preview gate for custom
channels, with no in-session or config alternative. The server is already
registered by the plugin, so no `--mcp-config` is needed. (The **Claude Desktop
app** takes the same server and can *call* the tools, but arming a channel to
*receive* pushed messages is Claude-Code-only.)

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

interlink installs as a user-scope plugin, so **every** Claude Code session runs
its own `interlink-mcp` — and they're all addressable. Each mints a random
`session_id` at startup and polls its own inbox `key#session_id`, so there's no
shared mailbox and no fan-out. A session **registers on startup** (node + session;
node registration is idempotent — the bus groups sessions under one `pubkey`) and
unregisters on close, so the roster reflects your currently-open sessions.
`discover` lists each identity with its **live sessions**
(`session_id · cwd · git repo · summary`):

```
A → [ a3f2c1 · ~/eden · git:eden · "installing Hunyuan3D deps" ]
    [ 71b0e4 · ~/site · git:site · "fixing the deploy" ]
```

`send_message(to:"A", session:"a3f2c1")` routes to that session. If A has exactly
one live session you can omit `session` (it auto-routes); a reply sticks to the
session that messaged you, so an ongoing conversation never re-picks. The **signed
`to` is still the bare key**, so `#session_id` is only an unsigned routing hint and
the trust gate is unchanged.

Two sessions on the **same machine** share one identity, so they can talk with
`send_message(to:"self", session:"<id>")` — no pairing and no self-entry in
`peers.json`, because it's the same principal (only the holder of your key can sign
as it). A session can't address *itself*: it's excluded from `discover` routing and
an explicit self-target is refused. The session store is in-memory, so it survives sleep
(same id, drains its queue on wake) and a hard restart just mints a new id to
re-pick. Full design: [`docs/SESSIONS.md`](docs/SESSIONS.md).

## See it without a Claude session

```bash
cargo build --release && ./scripts/demo.sh
```

A short tour of the trust model over the real binaries: a signed message from an
allowlisted peer is delivered, and a stranger's — signed, but by an unknown key —
is dropped before it can reach the model.

## Durability

The **bus** is the durable layer: it keeps a message for an offline recipient until
acked, over a pure-Rust ACID store ([redb](https://crates.io/crates/redb)), so a bus
restart loses nothing. Delivery is at-least-once, made safe by `msg_id` dedupe. Each
**agent** store is in-memory — isolated per session (so concurrent sessions on one
machine never collide) and intact across sleep, though not a hard restart. The
`message_status`, `conversation_history`, and `list_pending` tools expose that local
log.

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
