# Auto-progress: a debounced nudge hook (design)

> Status: **implementing** (0.4.1). Additive and **wire-compatible with 0.4.0** —
> no protocol/domain change; a `PostToolUse` hook plus a local state file.

## Why

0.4.0 makes progress updates *structured* and *more likely*, but they're still
model-driven: a heads-down agent can go silent mid-task. We want a reliable floor
without spam. A hook that *sends* updates would be spammy and dumb (a shell script
can't judge what's notable). So the hook **nudges**, and the model **sends**:

- **Cadence** is the hook's job (deterministic, reliable).
- **Content** is the model's job (it writes "deps in, restarting ComfyUI").

## Two layers, one shared timer

- **Prose (primary, unchanged):** the SKILL / server instructions say "update at
  each milestone." Well-timed, semantic — but unreliable.
- **Hook (backstop floor):** fires only when an active task has gone quiet longer
  than an interval. It injects a reminder; the model composes and sends.

They don't double up because **any outgoing update resets the timer** — model- or
hook-prompted. A well-behaved agent updating at milestones keeps resetting the
clock, so the hook never trips; the hook only speaks in the *gaps*. Well-behaved →
hook silent. Silent agent → guaranteed heartbeat.

## Mechanism

Three small pieces, all in the state dir (`~/.local/state/interlink/`):

1. **Current-task marker** (`current-task.json` = `{ task_id, peer, since }`),
   written/cleared by the MCP server so the hook knows whether a task is running
   and who to update. The rule for "I am the executor of this task":
   - An **inbound** message with a `task_id` and **no `status`** is an *opening
     request* → I'm now executing it → write the marker `(task_id, sender)`.
     (Progress/result messages carry a `status`, so they don't (re)arm it — that's
     what distinguishes "someone asked me" from "someone's reporting to me.")
   - Cleared when I send a **terminal** status (`result`/`failed`) for that
     `task_id`, or receive a `canceled` for it.
2. **Last-update timestamp** (`last-update`), reset by the MCP whenever it enqueues
   an **outbound** message with `status = update` (or a terminal) for the active
   task. This is the shared timer.
3. **The `PostToolUse` hook** (Node, shipped as `plugin/scripts/progress-nudge.js`),
   which on each tool event:
   - no marker → exit silently (idle / non-collaboration sessions never fire);
   - marker present **and** `now − last_update > INTERVAL` **and**
     `now − last_nudge > INTERVAL` → emit a reminder to the model
     ("You're executing task '<id>' for <peer> and haven't updated in >Ns — send a
     brief `send_message(status:'update', task_id:'<id>')` on what you just did"),
     and stamp `last-nudge`;
   - else → exit silently.
   - Optional tool gate: only count `Bash`/`Edit`/`Write` events toward the timer,
     so a flurry of `Read`s doesn't trip it.

## Config

`INTERLINK_PROGRESS_INTERVAL` seconds (default **60**); `0` disables the hook. The
operator dials the chattiness.

## What ships (0.4.1)

- **MCP** (`interlink-mcp`): write/clear the marker on the inbound gate; reset
  `last-update` on outbound `update`/terminal. ~30 lines, no wire change.
- **Plugin**: a `hooks/hooks.json` + `scripts/progress-nudge.js`, wired via
  `${CLAUDE_PLUGIN_ROOT}` (this re-introduces a hook to the plugin — it went to
  zero when the capability guards were removed; this one is a *nudge*, not a gate).
- **SKILL / instructions**: unchanged. Prose stays primary; the hook is the floor.

## Deferred

- **Cross-restart persistence.** The marker is best-effort in-session; a session
  restart loses "I was executing X" (overlaps tranche-B durable task state in
  [`docs/TASKS.md`](TASKS.md)). Fine for the floor — worst case the nudge stops
  after a restart until the next task message re-arms it.
- **Semantic filtering** ("only report interesting actions") — deliberately not
  attempted; that's the model's job (it composes the update), the hook only sets
  the beat.
