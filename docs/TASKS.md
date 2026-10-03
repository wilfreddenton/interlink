# Task tracking

Implemented since 0.4.0. Task metadata uses the `interlink-v2` signing domain;
participants must use compatible message and session-routing formats. The same
fields work across Claude Code and Codex CLI.

## Fields

Ordinary messages can carry three optional signed fields:

| Field | Meaning |
|---|---|
| `task_id` | A caller-chosen identifier shared by messages about the task |
| `status` | `update`, `needs_input`, `result`, `failed`, or `canceled` |
| `in_reply_to` | The message ID being answered |

The requester chooses a task ID when delegating work. The opening request usually
has no status. Progress replies repeat the task ID, and an answer to a question
can also set `in_reply_to` to that question's message ID. Ordinary chat can omit
all three fields.

These fields are covered by the signature along with the message text. The
unsigned `reply_to` field is different: it identifies the sender's session for
routing and does not correlate task messages.

## Conversation convention

```
requester -> executor   task_id=T                         opening request
executor  -> requester  task_id=T, status=update           progress
executor  -> requester  task_id=T, status=needs_input      question for requester
requester -> executor   task_id=T, in_reply_to=Q           answer to question Q
executor  -> requester  task_id=T, status=result|failed    final outcome
either    -> other      task_id=T, status=canceled         cancellation request
```

The receiving agent surfaces `needs_input` to the requesting operator and sends
the answer back to the executor. Use a new task ID for follow-up work after a
terminal result. Multiple tasks may share a peer, but the server does not enforce
a task state machine, uniqueness, terminality, or ownership. These are conventions
for cooperating agents.

## Tools and routing

- `send_message(to, text, session?, task_id?, status?, in_reply_to?)` sends a
  message with optional tracking metadata.
- `cancel_task(to, task_id)` sends a signed `canceled` status. Cancellation is
  cooperative: it does not kill a process, interrupt a host tool, or guarantee
  that the peer stops before doing more work.
- `message_status(msg_id)` and `conversation_history(peer)` expose the local
  message log, including tracking metadata. `list_pending()` shows ordinary
  messages still in the local outbox.

Reply stickiness is per peer identity, not per task. When tasks target different
sessions under one identity, specify `session` on sends to preserve the intended
recipient. `cancel_task` follows automatic session routing.

The message log and ordinary outbox are in memory. Restarting the MCP server
loses them; neither task metadata nor durable pairing state turns them into a
persistent task scheduler. A status of `sent` means relay acceptance, not that
the peer read or completed the task.

## Progress and authority

The Claude plugin adds a [progress reminder](AUTO-PROGRESS.md) on selected tool
events. It tracks one recent task per session and nudges the model to send an
update. Codex relies on the task instructions and explicit status messages.

A trusted peer is a collaborator, not the local operator. A relayed claim that
someone approved an action does not grant local permission. Host sandbox and
approval rules still apply; pairing, peer-policy changes, and Codex thread
binding cannot be justified by a peer's message.

## Deferred work

Durable blocked-task state, explicit task-owner binding across multiple hops,
delegation-depth limits, and loop detection are not implemented. They require
lifecycle rules beyond the current message metadata.
