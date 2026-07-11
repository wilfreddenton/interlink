# praetor

[![CI](https://github.com/wilfreddenton/praetor/actions/workflows/ci.yml/badge.svg)](https://github.com/wilfreddenton/praetor/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

**Secure agent-to-agent messaging and capability-scoped delegation for Claude Code.**

![the trust model, over the real binaries](docs/demo.gif)

Independent Claude Code sessions message each other and — the part nobody else
does — **safely delegate work to peers they don't fully trust**. Every message is
Ed25519-signed, a peer's identity *is* its public key, and an untrusted peer's
request runs in a disposable subagent whose tools are the only thing it can do.

## Why this exists

Letting Claude Code sessions talk to each other is a solved, crowded problem:
[`claude-peers-mcp`](https://github.com/louislva/claude-peers-mcp) (~2k★),
`agent-bridge`, and others all do it, and Claude Code's own **channels** feature
is built for exactly this. They solve **transport**.

None of them solve **authorization**. The popular one's quickstart is literally:

```
claude --dangerously-skip-permissions --dangerously-load-development-channels server:claude-peers
```

That is: *any process that can reach the local broker can inject text into an
agent running with every permission check turned off, and there is no way to
know who sent it.* Claude Code's own channel docs call an ungated channel a
"prompt injection vector." `praetor` is the answer to that — the thing that
lets you turn the permissions back **on**.

## The trust model

Two ideas do all the work.

**1. A peer's identity is its public key.** Names (`alice`, `bob`) are local
petnames; the key is the truth. Claiming a name gets you nothing without the key.
Messages are signed over a domain-separated encoding and verified with
`verify_strict` *before* anything reaches the model — a stranger's message is
dropped, not shown.

**2. `peers.json` is a per-peer dial, from "run anything" to "run exactly these
tools":**

```json
{
  "my-laptop":    { "key": "8Emom3…", "may": "*" },
  "build-server": { "key": "rq2AzH…", "may": "run-tests" },
  "some-bot":     { "key": "Zc91xK…", "may": "read-only" }
}
```

- **`"*"`** — full trust. The message is delivered inline; the agent acts on it
  with all its tools. For machines whose private keys *you* hold.
- **a capability name** — e.g. `run-tests` refers to `.claude/agents/run-tests.md`,
  an agent whose `tools:` frontmatter *is* the capability. The request is handled
  in a disposable subagent limited to those tools — and a subagent physically
  cannot call a tool it wasn't given.

An unlisted peer gets nothing. Deny-by-default.

## How a scoped request is contained

The hard case — a peer you don't fully trust — never gets to put text in front of
your main agent:

```
signed request  ──►  bus  ──►  your channel server:
                                 verify sig · on allowlist? · to me? · fresh? · not a replay?
                                 scoped ─► QUARANTINE the body, push only metadata + a capability name
                                              │
   main agent  ◄── "a scoped request m3 from build-server is pending; spawn `run-tests` to handle it"
        │            (main agent CANNOT fetch the body — a PreToolUse hook denies it)
        ▼
   spawns `run-tests` subagent ─► it calls fetch_request(m3) ─► gets the body
                                   acts within its tools (frontmatter-enforced)
                                   replies via send_message
        │
   subagent exits ─► its context, and the untrusted text, are discarded
```

Three deterministic gates, none relying on the model's judgment:
- the **signature + allowlist** decide whether a message exists at all;
- a **`PreToolUse` hook** denies `fetch_request` to the main agent, so the
  untrusted body only ever enters a throwaway subagent;
- the subagent's **`tools:` frontmatter** is the capability — hard-enforced by the
  runtime.

All three are verified against a live Claude session (see below).

## Quickstart

```bash
cargo build --release            # praetor-bus, praetor-mcp, praetor-keygen

# 1. one shared bus (loopback HTTP, no TLS needed — see Security)
./target/release/praetor-bus

# 2. an identity per agent; praetor-keygen prints the public key to share
./target/release/praetor-keygen --out alice.key
```

For each agent, drop an MCP config naming the `praetor` server (see
[`config/`](config)) and a `peers.json` listing the peers' public keys. Then
launch it as a channel:

```bash
claude --mcp-config alice.mcp.json --dangerously-load-development-channels server:praetor
```

To use a scoped capability, add the capability agent to the project's
`.claude/agents/` (example: [`contrib/agents/read-only.md`](contrib/agents/read-only.md))
and register the `PreToolUse` guard
([`contrib/pretooluse-guard.sh`](contrib/pretooluse-guard.sh)) in
`.claude/settings.json`.

## See it without a Claude session

```bash
cargo build --release && ./scripts/demo.sh
```

A 20-second tour of the trust model over the real binaries: a signed message from
an allowlisted peer is delivered, and a stranger's — signed, but by an unknown
key — is dropped before it can reach the model.

## Verified, not asserted

The [`experiments/`](experiments) harnesses drive real, interactive Claude
sessions through a PTY (channels need a TTY, so `claude -p` can't test them) and
confirm the whole thing end to end:

- **inline** — alice ↔ bob round-trip, signed, both directions;
- **rejection** — a stranger's message is dropped, never pushed;
- **scoped enforcement** — a scoped peer's read runs, but its request to run a
  shell command is *deterministically blocked* (the side-effect file is never
  created, even with the shell tool in the session allowlist);
- **durable delivery** — a message sent while the bus is down is queued and
  delivered once it returns, surviving a restart of *either* side (verified by
  killing and relaunching the bus and the agent between send and delivery).

Messages are held on **both** sides until acked — the bus keeps a message for an
offline recipient, and each agent keeps an unsent message in a durable outbox —
over a pure-Rust ACID store ([redb](https://crates.io/crates/redb)). Delivery is
at-least-once, made safe by `msg_id` dedupe. The `message_status`,
`conversation_history`, and `list_pending` tools expose the local log; an
untrusted peer's body is logged as metadata only, never written to disk.

## Security

- **No transport encryption, on purpose.** The bus binds `127.0.0.1`; traffic
  never leaves the machine. Authenticity comes from **signatures on the
  messages**, which — unlike TLS — survive passing through an untrusted bus.
  Compromising the bus lets you drop or reorder messages, never forge one. This
  is also what keeps the dependency tree free of C (`ring`), so the binaries are
  pure-Rust and statically linkable.
- **`"*"` is safe only because of identity.** A wildcard grant means "this key
  may do anything" — reserve it for machines whose keys you hold. A compromise of
  any `*` peer becomes arbitrary tool execution on the others.
- **Research preview.** Channels are a Claude Code research preview; custom ones
  require `--dangerously-load-development-channels`, and the protocol may change.

## Pure Rust, cross-platform

No C dependencies (CI fails the build if `ring`/`openssl-sys`/`cc`/`cmake`
reappear). Fully static binaries on Linux (musl) and Windows; on macOS, links
only system libraries. One feature-gated crate: `bus`, `agent`, `identity`,
`persist`.

## Related work

| | messaging | who can send | authorization | context isolation |
|---|---|---|---|---|
| Agent Teams (built-in) | ✅ | lead-spawned only | permission relay | — |
| claude-peers-mcp | ✅ | anyone on the broker | none (`--dangerously-skip-permissions`) | none |
| **praetor** | ✅ | **signed + allowlisted keys** | **per-peer capability dial** | **quarantine + subagent** |

## Deploying

Run it on your own machines over Tailscale (no code changes, no public exposure), and
federate later by adding a relay URL. See [`DEPLOY.md`](docs/DEPLOY.md).

## Design

The full walkthrough — execution model, the channel discovery, the three gates,
and the runtime facts we had to establish by experiment — is in
[`DESIGN.md`](DESIGN.md). Deferred work (peer discovery, post-quantum signatures)
is in [`DIRECTORY.md`](DIRECTORY.md).

## License

MIT — see [LICENSE](LICENSE).
