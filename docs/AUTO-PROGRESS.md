# Automatic progress reminders

Implemented since 0.4.1, this Claude Code plugin hook reminds the model to report
progress on a peer's task. It does not send messages itself or guarantee that the
model will respond. Codex uses the shared task protocol and server instructions,
but this hook is not installed by the Codex adapter.

## Mechanism

The MCP server writes a best-effort marker when an inbound message has `task_id`
and no status. The marker contains `{ task_id, peer, since }`. A status-free reply
with a task ID can also set it; this is a heuristic, not a task scheduler.

State is scoped to the session under
`$XDG_STATE_HOME/interlink/task/<session_id>/`, defaulting to
`~/.local/state/interlink/task/<session_id>/`:

| File | Purpose |
|---|---|
| `current-task.json` | Most recently marked task and peer |
| `last-update` | Timestamp reset on a new marker or an outgoing update/terminal status |
| `last-nudge` | Timestamp of the last reminder |

The plugin's `PostToolUse` hook matches `Bash`, `Edit`, and `Write`. On one of those
events, [progress-nudge.js](../plugin/scripts/progress-nudge.js) checks for a marker
and whether both the last update and last nudge are older than the interval. It
then emits `additionalContext` asking the model to send an attributed progress
update, and stamps `last-nudge`.

The hook derives its session ID from its JSON stdin payload. The server and hook
must use the same state directory. `INTERLINK_PROGRESS_INTERVAL` is measured in
seconds, defaults to 60, and is disabled by zero, negative, or invalid values.

## Clearing and limits

Sending a terminal status (`result`, `failed`, or `canceled`), receiving a
`canceled` message, or using `cancel_task` clears a matching task marker. The
match is by task ID. Outgoing updates and terminal statuses reset the session's
shared timer even if they concern another task.

There is only one marker per session. A later task replaces an earlier one, so
this is a reminder for recent work rather than a complete multi-task tracker.
No matching tool event means no hook invocation; an idle or blocked model does
not receive a periodic timer wake from this hook.

Marker files can survive a restart, but they are not reconciled with the host's
actual task state and can be stale. File writes are best-effort. Reliable durable
blocked-task state, per-task timers, and semantic progress filtering remain
separate work. See [task tracking](TASKS.md).
