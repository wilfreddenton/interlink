# duet

[![CI](https://github.com/wilfreddenton/duet/actions/workflows/ci.yml/badge.svg)](https://github.com/wilfreddenton/duet/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

**Let two Claude Code agents hold a conversation — no human relaying messages by hand.**

`duet` is a small Rust workspace that bridges two (or more) Claude Code instances
over a local message bus. One agent sends with an MCP tool; each agent receives
through a background long-poll that a **Stop hook keeps alive across turns**. The
interesting part isn't the bus — it's the harness-aware liveness trick that lets
an agent stay reactive to external events without a human in the loop.

```
 Claude Code  ──MCP (JSON-RPC / stdio)──►  duet-chat  ──HTTPS──►  duet-bus
   (client)                                 (server)               (queue)
                                            (HTTP client) ────────►
```

## The problem it solves

Two Claude Code sessions can't address each other. If you want them to
collaborate, you end up copy-pasting messages between two terminals. You'd think
you could just have each one "listen" in a loop — but the Claude Code harness has
a hard constraint that breaks the obvious approaches:

> **An agent is only re-invoked when a *background shell task exits*.**
> The model itself doesn't run a loop; it acts on a turn and then goes idle.

So a naive design fails in two ways:

- **A tight in-turn poll** never yields — it burns tokens and never goes idle.
- **An eternal `while true` background listener** never *exits*, so it never wakes
  the agent. Messages pile into a file no one reads.

## The insight

Receiving has to be a **relaunch loop**, and liveness must live in the harness,
not in the model's memory:

1. The agent runs a background long-poll: `curl .../recv?me=alice` — it blocks
   until a message arrives, then **exits**, which wakes the agent.
2. The agent handles the message, replies via the MCP `send_message` tool, and
   relaunches the same long-poll.
3. A **`Stop` hook** ([`duet-liveness`](crates/duet-liveness)) refuses to let the
   agent go idle unless a listener is currently armed on the bus. That's what
   makes the loop durable — it never depends on the model *remembering* to
   relaunch. It's bounded (only blocks while disarmed) and fails open (a down bus
   never traps the model).

Durability lives in the bus: payloads are queued per-recipient, so a missed
relaunch only *delays* delivery — it never drops a message.

## Crates

| Crate | Reusable? | Role |
|---|---|---|
| [`duet-bus`](crates/duet-bus) | ✅ generic | Async per-recipient long-poll queue over HTTPS. Payloads are opaque JSON. `POST /send`, `GET /recv`, `GET /armed`. Lib + binary. |
| [`duet-liveness`](crates/duet-liveness) | ✅ generic | The `Stop` hook. Keeps *any* background listener armed across turns — not tied to this bus. |
| [`duet-mcp`](crates/duet-mcp) | ✅ generic | Helpers for the "MCP tool that proxies to a local HTTP service" pattern: crypto-provider install, CA-trusting client, `BusClient`. |
| [`duet-chat`](crates/duet-chat) | the instance | The concrete MCP server: `send_message` / `poll_messages` tools + the `{from, text}` message shape. ~120 lines on top of the crates above. |

The split is deliberate: `duet-chat` is a thin *instance* of a general pattern.
Swap it for your own tools and payload and you have a different bridge.

## Quickstart

```bash
cargo build --release          # builds all four binaries into target/release

# 1. start the shared bus (generates certs/ on first run)
./target/release/duet-bus

# 2. wire up each instance — for "alice":
cp config/alice.mcp.json       <alice project>/.mcp.json
#   merge config/settings.alice.json into <alice project>/.claude/settings.json
# ...and the same with the "bob" files for the other instance.
```

Restart both Claude Code instances. Tell each one once: *"launch the background
message listener."* After that the Stop hook keeps it armed. Then ask alice to
`send_message` to bob — and watch bob react on its own.

See [`config/`](config) for the drop-in files and the exact `DUET_*` env vars.

## Design

The full walkthrough — the harness execution model, why a barrier vs. relaunch,
the arming race, and the failure modes — is in [`DESIGN.md`](DESIGN.md).

## Limits

- **Not parallel.** Each agent does one thing at a time; an incoming message
  waits until the current turn finishes.
- **Heartbeat wakes.** With no traffic the long-poll times out (~5 min), wakes the
  agent, and re-arms — a periodic no-op.
- **Context growth.** A long-lived instance accumulates context per message.
- **Local + self-signed.** The bus binds `127.0.0.1` with an `rcgen` cert. Not
  built to face a network.

## License

MIT — see [LICENSE](LICENSE).
