# Sessions: multiple live sessions per node (design)

> Status: **implemented in 0.5.0.** Makes N concurrent interlink sessions per
> machine first-class and addressable. The session-scoped announcement changes the
> roster shape (a coordinated, pre-1.0 change), so all nodes must be ≥0.5.0 to see
> each other in `discover`; already-paired peers still chat across the bump.

## The problem

interlink installs as a **user-scope plugin**, so *every* Claude Code session on a
machine spawns its own `interlink-mcp` — and today they're undifferentiated: all
point at one fixed `agent.redb` and all poll the one bare-key inbox. The moment
there's a second session (or an orphan), that gives:

1. **DB collision** — redb is single-writer, so the second server crashes on
   startup with *no tools*.
2. **Inbox fan-out** — every session polls the same inbox, so a message "to fedora"
   is grabbed by a random session (including plain chats that shouldn't intercept
   anything).
3. **Roster spam** — every session announces as the same identity.

We want the opposite: **run several real interlink sessions on one machine, each
its own addressable endpoint**, keeping the node's cryptographic identity and
pair-once semantics.

## Prior art

`louislva/claude-peers-mcp` is a good **UX** reference and a poor **mechanism** one
— it's single-machine (localhost broker, PID-probe liveness). We adopt its UX (a
per-session id; tell sessions apart by **cwd + git repo + a free-text summary**;
pick from a list) and replace the single-machine machinery: identity is our
Ed25519 key, liveness is a **heartbeat TTL** (can't PID-probe a remote box), the
"broker" is our untrusted signed-message bus, and we keep a **node identity +
human-gated pairing** (pair with *A* once, then pick one of *A*'s live sessions).

## The model

Identity is unchanged (a node = an Ed25519 key, paired once). A **live session** is
a new sub-unit under it:

- **Random self-minted `session_id`** at startup. It keys everything: the session
  polls `key#session_id` (its own inbox) and registers under it. Random, so it is
  **always collision-free** — two sessions in the same directory never clash.
  (Deterministic/metadata-derived ids were considered and rejected: no automatic
  metadata is both stable-across-restart *and* unique-per-session, and cwd-derived
  ids re-create the same-dir collision.)
- Humans never see the id — they recognize a session by **cwd + git repo +
  summary**. The id is pure routing.

## Local store: in-memory

Each session uses an **in-memory** outbox + log — **no db file**. This:
- **fixes the crash** (nothing shared to contend for),
- **needs zero cleanup** (no `sessions/*.redb` files to orphan or sweep),
- **survives sleep** (suspend freezes the process with RAM intact; the long-poll
  reconnects on wake).

The trade: the outbox/log are **ephemeral** — lost on a full restart or kill (but
not on sleep). That's fine, because the **bus is the durable layer**: a message
that reached the bus is still keep-until-ack durable for an offline recipient. Only
messages queued *while the bus was unreachable* would be lost on a hard restart —
an edge case that survives sleep.

## Register node + session on start

There is **no `INTERLINK_NODE` flag** and no alias change. A session **announces on
startup** — node and session together — and heartbeats to stay live. **Node
registration is idempotent:** every session under one identity announces the same
`pubkey`, the bus keys the roster by `pubkey#session_id` and groups by `pubkey`, and
a re-announce is an upsert, so N sessions never produce a duplicate node. This is the
plain, intuitive model: if a session is up, it's discoverable; when it closes it
unregisters.

(An earlier design registered lazily — only on the first `send_message` / `set_summary`
— to keep plain, non-interlink chats out of the roster, since the MCP is a user-scope
plugin that every session spawns. It was dropped: it made a fresh session invisible to
itself and created a standoff where two fresh sessions never saw each other. The
trade-off is that every plugin-loaded session now appears; gating that on a real
"is this an interlink session" signal — if one ever becomes detectable — is a separate
call.)

## Polling & presence

Each live session:
- **long-polls only its own inbox** `key#session_id`. There is **no node inbox** —
  every message, *including a pairing knock*, is delivered to a specific live
  session's inbox, so each inbox is polled by exactly one session and there is **no
  shared mailbox and no fan-out anywhere**. (A node has no persistent daemon;
  nothing handles a message unless a session is live, so a node-level inbox would
  only be a store-and-forward queue — which the keep-until-ack bus already provides
  per inbox. It bought nothing over "reach a live session," so it's gone.)
- **heartbeats** a signed announcement ~every 30s (from startup).

## Bus registry & cleanup

The roster is session-scoped. A live session heartbeats a **signed** announcement
`{ pubkey, session_id, cwd, git_root, summary, ts, sig }`; the bus groups by
`pubkey`, so `/roster` returns each identity → its live sessions.

Removal is two things, on two clocks:
- **Presence — short TTL (~90s).** Miss the heartbeat window → dropped from the
  roster (stops showing in `discover`). This is the only liveness signal.
- **Queue — long retention.** The inbox `key#session_id` is **NOT** dropped when
  presence expires — that's the keep-until-ack point, and it's what lets a sleeping
  session get its messages on wake. It's bounded (drop-oldest cap + a long sweep),
  so a truly-abandoned inbox is a small, self-limiting leak.
- **Graceful unregister** on `SIGTERM` (clean Claude-session close) removes presence
  *immediately*, so a peer learns the session is really gone (not just sleeping).

## Discovery & addressing

1. **`discover`** lists each paired online peer → its live sessions:
   `A → [ (a3f2) ~/eden-protocol · git:eden · "installing Hunyuan3D deps" ]`.
2. **`send_message(to:"A", session:"<id>")`** routes to `A_key#<id>`.
   - **Exactly one live session → `session` optional, auto-routes** (zero friction
     for the common case).
   - Several → omit it and the tool returns the list to pick from.
3. **`set_summary("…")`** sets this session's descriptor (and announces it); cwd +
   git_root are filled automatically.
4. **Reply-stickiness:** every message carries `reply_to = my key#session_id`
   (unsigned routing hint), so a reply returns to the exact sending session and,
   within a `task_id`, both sides pin to the session pair. Address once; the
   conversation stays on that desk.

**Pairing folds into this.** To connect to a peer you're not yet paired with,
`discover` still shows it (roster presence is public), and you **knock one of its
live sessions** — the knock is delivered to that session's inbox, its operator
accepts, and the accept is **identity-level** (their key enters `peers.json`, and
yours theirs, for *all* their sessions). Which session you knocked is just the
delivery path. You can only knock a peer that has a **live session** — with no node
daemon, nothing handles a knock otherwise.

## Recovery (the sleep/restart scenarios)

- **Send never hard-errors.** A message to a session that's currently offline is
  **queued** (keep-until-ack); the sender surfaces an informational "session
  offline — queued," not a failure.
- **Sleep → wake: self-heals.** The frozen process wakes as the **same** in-memory
  `session_id`, resumes polling its inbox, **drains the messages queued during the
  sleep**, and re-heartbeats. Seamless; no retry needed.
- **Clean close: immediate.** Graceful unregister tells peers the session is gone,
  so they re-pick right away.
- **Crash / hard restart: re-pick.** A reopened session is a **new** random id (and
  cold anyway), so it does not inherit the old inbox. The sender sees the old id
  never returned and re-picks the peer's new session. (The old inbox's queued
  messages are the bounded leak, swept later.)

## What changes (code)

- **`identity`** — add `session_id`, `cwd`, `git_root`, `summary` to the signed
  announcement (extend the announce encoding).
- **`interlink-mcp`** — mint a random `session_id`; **in-memory store always** (drop
  the `--db`/`INTERLINK_AGENT_DB` durable path for the agent); poll `key#session_id`
  only (no node inbox); knocks are sent to a target session's inbox; announce on
  startup + heartbeat;
  `SIGTERM` graceful-unregister; new `session` arg on `send_message` (auto-route if
  one); new `set_summary` tool; `discover` renders identity→sessions; set/consume
  `reply_to`.
- **`bus`** — roster groups by `pubkey`; `/roster` returns sessions; presence TTL vs.
  queue retention split; `/unregister` for graceful removal.
- **plugin** — no `INTERLINK_NODE`, no alias change; `.mcp.json` drops the agent
  `--db`; README/skill document the model.

## Build order

1. **Random `session_id` + in-memory store + poll `key#session_id` only.**
   *Fixes the crash and the fan-out on its own* — ship first.
2. Session-scoped signed announcement + roster grouping; `discover` renders
   sessions; `set_summary`; register-on-start; graceful unregister.
3. `session` arg + auto-route-if-one; `reply_to` stickiness; the recovery behavior
   (queue-not-error; sleep-heal; re-pick).
