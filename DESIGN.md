# Design notes

## The execution model that shapes everything

Claude Code runs a model in an agent loop. Two facts about that loop dictate this
entire design:

1. **The model acts on turns, then goes idle.** It does not run a background
   thread of its own. There is no "while listening" state inside the model.
2. **The harness re-invokes the model on discrete events.** The relevant one here
   is: *a background shell task (launched with `run_in_background`) exits.* When
   it does, its output is delivered and the model runs again.

Everything below follows from working *with* those two facts instead of against
them.

## Why not the obvious designs

**In-turn polling.** "Have the agent loop calling a `poll` tool until a message
arrives." This never yields the turn — it spends tokens continuously and the
agent never returns to idle where the human can talk to it. Rejected.

**One immortal listener.** "Launch a background task that loops forever reading
the bus." A background task only wakes the agent when it *exits*. A task that
never exits never wakes anyone; messages accumulate unseen. Rejected.

**Timer polling (`/loop`, cron).** Genuinely viable: wake every N seconds and
drain the queue. Harness-owned, so liveness is guaranteed. The cost is latency
and a token tick per empty poll. `duet` prefers event-driven delivery, but the
bus's `/recv` supports this style too.

## The chosen design: relaunch loop + Stop-hook guarantee

Receiving is a loop of short-lived listeners:

```
arm ──► curl /recv (blocks) ──► message arrives ──► curl exits ──► agent wakes
 ▲                                                                      │
 └──────────────────── agent handles it, relaunches ◄──────────────────┘
```

The weak point is step "relaunches": it's a *model action*, and model actions
are never guaranteed. So we move the guarantee into the harness with a **`Stop`
hook** ([`duet-liveness`](crates/duet-liveness)):

- On every stop, the hook asks the bus `GET /armed?me=<self>`.
- **Armed** (a `/recv` is in flight) → allow the stop.
- **Not armed** → return `{"decision":"block","reason":"...relaunch..."}`, which
  forces the model to continue and relaunch before it can idle.

Properties that make this safe:

- **Bounded.** It only blocks while disarmed. Once the agent relaunches, the next
  stop sees it armed and lets go — no infinite loop.
- **Fails open.** If the bus is unreachable, the hook allows the stop. A dead bus
  can never trap the agent.
- **No lost messages.** The bus queues per recipient, so the gap between one
  listener exiting and the next arming only *delays* delivery.

## The arming signal

"Armed" is defined as *the count of in-flight `/recv` calls for a recipient is
> 0*. The broker increments an `AtomicUsize` around the `await` in `Broker::recv`
and decrements after. This is more robust than a hook `pgrep`-ing for a curl
process: the bus is the single source of truth about whether a listener is
actually connected, not just whether a process exists.

## Why sending is MCP but receiving is not

Sending is a discrete, model-initiated action → a natural MCP **tool**
(`send_message`). Receiving must *wake* the model from idle, and only a
background shell task can do that → it's a `curl`, outside MCP entirely. This is
why `duet-chat` (the MCP server) is only in the send path; the receive path talks
to the bus directly.

## Two protocols, one adapter

`duet-chat` is a translator between two client/server relationships:

- **Facing Claude:** an MCP server (JSON-RPC over stdio), via `rmcp`.
- **Facing the bus:** an HTTP client (HTTPS), via `reqwest`.

Claude only speaks MCP; the bus only speaks HTTP. Keeping them separate means the
bus is reusable by anything (a shell script, another language) and the MCP layer
carries no queueing logic.

## Transport choices

- **stdio for MCP.** Claude Code launches the server as a subprocess; stdio is the
  standard local transport. No HTTP server involved on the MCP side.
- **HTTPS for the bus,** terminated with `tokio-rustls` and served by
  `hyper-util`'s connection builder driving an axum `Router` — the canonical way
  to serve axum over rustls without the `axum-server` wrapper. ring is the crypto
  provider throughout, so there's no `aws-lc-rs`/`cmake` build dependency.
