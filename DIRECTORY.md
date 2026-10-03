# Deferred work

The current project implements signed identity, human-gated pairing, discovery,
per-session routing, task correlation, status messages, and cooperative
cancellation. Codex CLI support and persistence improvements shipped in
[0.9.0](CHANGELOG.md#090---2026-10-03).

## Public-relay hardening

A trusted private network is the current deployment boundary. Before exposing a
broker to untrusted clients, it needs:

- Authenticated receive and acknowledgment operations, so knowing a recipient's
  public key is not enough to drain its queue.
- Authorization for presence changes, including unregistering sessions.
- Rate and resource limits that bound aggregate queues and stored bytes, beyond
  the existing per-recipient message cap and roster cap.
- End-to-end encryption if relay operators must not see message contents.

These require protocol and deployment work, not just an HTTPS reverse proxy.
See [deployment](docs/DEPLOY.md) and the [trust model](DESIGN.md).

## Task orchestration

`task_id`, `status`, and signed `in_reply_to` already correlate messages. They
are conventions for cooperating agents, not a persistent scheduler. Potential
extensions are durable blocked-task state, task-owner tracking across multiple
hops, and delegation-depth or loop limits. See [task tracking](docs/TASKS.md).

`reply_to` already has a different purpose: it is an unsigned session-routing
hint. It must not be confused with signed `in_reply_to` message correlation.

## Delivery and storage

- End-to-end receipts could distinguish relay acceptance from recipient handling
  if one-way delivery confirmation becomes necessary.
- Inbox compaction and cleanup of abandoned session files and broker queues are
  not implemented. Retention needs to preserve unread messages and cursor safety.
- The ordinary agent outbox and conversation log are still in memory. Persisting
  them would need isolation and recovery rules for concurrent sessions.
- A queued message can expire while waiting on the broker. Renewing an unsent
  pairing control message does not extend the validity of copies already sent.

## Host compatibility

The current Codex adapter supports root CLI sessions on the local shared daemon.
Desktop, remote app-server, ephemeral-thread delivery, and subagent support are
separate work. The local host fixtures validate lifecycle behavior without a
production model; interactive UI and production-model acceptance tests remain
outside those fixtures. See [Codex validation](codex/README.md#validation) and
[Claude validation](docs/DELIVERY.md#validation).
