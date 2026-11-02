# Design notes

## Session addressing

Agent-facing sends use a full destination session ID. The MCP server resolves
signed announcements only for its own identity or authorized peers, persists
ownership for offline routing, and rejects conflicting ownership. Pairing and
message verification remain identity-based. The bus still routes opaque
`key#session_id` addresses. Legacy peer-plus-session calls remain compatible.

## Architecture

Independent Claude Code and Codex CLI sessions exchange signed messages through
one or more HTTP brokers. Each Codex MCP instance binds to one thread. Claude
can reuse an MCP process across conversations, with a separate active session
runtime and persistent mailbox for each conversation ID. The broker routes opaque
payloads to `key#session_id`; the agent authenticates messages and applies its
local peer policy before delivering them to the host.

| Host | Delivery | Session identity |
|---|---|---|
| Claude Code, preferred when available | Authenticated local messaging socket | Current conversation ID supplied by local lifecycle hooks |
| Claude Code, legacy fallback or explicit inbox mode | Durable local notice inbox and an `asyncRewake` Stop listener | Bound conversation ID, or startup session ID without hooks |
| Claude Code, optional channels | `notifications/claude/channel` | Bound conversation ID, or startup session ID without hooks |
| Codex CLI | `codex queue` through the local shared daemon | Thread UUID supplied by trusted lifecycle hooks |

Socket delivery disables Claude's legacy Stop listener and its idle maintenance
turns. Socket frames carry the destination conversation ID. Claude's lifecycle
hooks select a fresh mailbox after `/clear`, reopen the existing mailbox on
resume, and stop the old session's workers when switching or ending conversations.
Messages are not moved between mailboxes. Managed Claude and Codex instances do
not announce or receive until bound; initial binding currently requires a prompt.

Discovery identifies sessions by identity, session ID, working directory, project,
and work summary. Binding gives one reminder per session runtime to set a useful
summary; updates are agent-directed, without title synchronization or polling.

See [delivery](docs/DELIVERY.md), [sessions](docs/SESSIONS.md), and
[Codex setup](codex/README.md) for host requirements and configuration.

## The trust gate

[`agent::decide`](src/agent.rs) applies these checks:

```
verify signature -> addressed to me? -> authorized kind? -> fresh? -> not a replay?
```

Messages use a domain-separated, length-prefixed encoding:
`interlink-v2\0`, sender, recipient, timestamp, kind, message ID, text, status,
task ID, and reply correlation. Ed25519 `verify_strict` authenticates the sender.
The `reply_to` session route is an unsigned hint; the receiver uses it only when
its key matches the signed sender. It cannot authorize another identity.

An ordinary message requires a key in `peers.json`, or the receiver's own key
(for another session under the same identity). Unknown keys may send a pairing
request whose claimed name is presented as untrusted metadata. Pair acceptances
are processed only when they match a locally outstanding request. Neither
exception admits arbitrary chat from a stranger. [Pairing](docs/DISCOVERY.md)
describes the handshake and recovery behavior.

The freshness window allows at most 24 hours in the past and 60 seconds in the
future. A bounded, in-memory set remembers 4096 message IDs per session runtime.
It reduces duplicate delivery across retries and relays; it does not provide
exactly-once processing across restarts or after entries leave the set.

Peer-policy updates take a file lock, reload the latest policy, and atomically
replace the file. Readers reload it, so sibling sessions see updates without a
restart. Existing names cannot silently be reassigned to different keys.

## Admission and operator authority

Pairing admits a full chat partner. Its messages enter the model's context and
may drive collaboration, subject to the host's existing permissions and sandbox.
They do not authorize changes to trust or become the local operator's consent.
Pairing, peer-policy changes, and host session binding are local setup or operator
operations, never actions justified solely by a peer message.

An earlier capability-scoped subagent design was removed. It isolated a received
request but could not make the reply safe to consume in the main conversation.
The current design therefore authenticates the collaborator and asks the operator
to decide whom to trust, rather than promising containment of an untrusted peer.

## Sending and receiving

Sending is an MCP tool call. Ordinary outgoing messages are signed and placed in
an in-memory outbox, retried until at least one configured relay accepts them.
The sender attempts every configured relay, but does not keep retrying failed
relays after another has accepted. `message_status` reports local state and relay
acceptance, not a recipient read receipt.

Receiving uses a shared, persistent mailbox scoped by identity and session.
Broker acknowledgement follows the verified body's local commit. Host adapters
send inbox notices; `receive_messages` fetches unread bodies without consuming
them. `acknowledge_messages` explicitly consumes exact message receipts. History
can explicitly consume its returned records. Ordinary inbox notices carry no
peer body, so stale notices do not replay consumed content.

Notification observation is separate from message acknowledgement. A successful
host handoff leaves one outstanding wake-up that coalesces later arrivals, even
when manual reads have consumed its original messages. Accepted notices have no
automatic expiry or timed reminders. A fetch with the matching notification ID
retires the wake-up; stale IDs cannot retire a newer notice. Failed or unfinished
handoffs retry with backoff. An uncertain handoff can still duplicate a notice.

Prompt hooks persist a marker when an exact notice reaches a host turn. Stop hooks can
retire a started notice whose inbox has been drained, or request one recovery
continuation for unread non-progress messages. If a Stop hook already continued
the turn, remaining unread work produces a warning instead of another
continuation. Claude requires confirmed prompt delivery for this recovery; Codex
can also request a manual fetch while preserving a notice still in its queue.
Hooks never acknowledge message bodies. Manual and stale-ID reads expose pending
notice diagnostics, and `reset_notification=true` provides explicit recovery
when a host loses a notice before prompt delivery.

`status="update"` is quiet, replaceable progress and can be superseded unread.
Questions, findings, and corrections that need attention should use ordinary
messages. Progress coalescing and consumption live in the shared mailbox layer.
See [delivery guarantees and limits](docs/DELIVERY.md).

## Persistence and bounds

The bus uses redb. Supplying `--db` makes its queues persistent; without that flag
it is an in-memory broker. Each recipient queue defaults to 1024 messages and
drops the oldest when full. The presence roster is in memory even with `--db`.
Unregistering a session does not delete its queued messages.

Each agent keeps the ordinary outbox, outbound log, gate replay set, and reply
stickiness in memory. They survive process suspension, but not process restart
or ending or switching the active Claude conversation.
A shared agent redb file would prevent multiple sessions from opening the store,
so `--db` / `INTERLINK_AGENT_DB` on the MCP server remain accepted but ignored.

Separate local files persist peer policy, pairing requests and control messages,
shared inbound mailbox bodies, consumption, and notice observation state, legacy
Claude notification records and cursors, and Codex failed deliveries. Socket
pairing notices also use the durable local notice inbox so a socket outage does
not block broker intake. Pairing and failed-delivery files are scoped by identity
and session. Pairing retries preserve the outstanding correlation ID and refresh
the signature when sending a saved job.
This does not renew a message already held by the broker.

These mechanisms provide durable handoffs with limited guarantees. Queue
overflow, expired signatures, unavailable storage, an abandoned session ID, and a
crash during the host handoff remain relevant failure cases.

## Transport boundary

The bus has no authentication or TLS. Use loopback or restrict it to a trusted
private network. Signatures prevent forged sender identity, but do not hide
message contents or prevent a reachable client from reading, acknowledging,
removing, or flooding queues and presence. An untrusted public relay needs
additional authentication and encryption before deployment.

No TLS dependency is linked into the agent or broker. The HTTP client expects
plain HTTP URLs. [Deployment](docs/DEPLOY.md) explains the private-network setup;
[deferred work](DIRECTORY.md) records the public-relay requirements.

## Code organization and checks

One crate exposes the `bus`, `agent`, `identity`, and `persist` features. The
published package is `interlink-mcp`; the library import is `interlink`. The three
binaries are `interlink-mcp`, `interlink-bus`, and `interlink-keygen`.

- `identity`, `policy`, and `agent`: signatures, authorization, dispatch, dedupe.
- `policy_store`, `state`, and `pairing`: shared policy and durable control state.
- `mailbox`: shared consumption, progress coalescing, and notification state.
- `inbox`, `claude`, `codex`, and `delivery`: local notice inbox, socket and queue
  delivery, and saved failures.
- `src/bin/mcp/delivery.rs`: host sinks and message rendering.
- `src/bin/mcp/sessions.rs`: Claude conversation binding and worker lifecycle.
- `src/bin/mcp/transport.rs`: MCP transport and pre-initialization compatibility.
- `bus`, `store`, and `route`: transport queues, storage, and session addresses.

`just ci` runs formatting, Clippy, tests, and the no-C-dependencies checks.
GitHub CI additionally checks the feature powerset and native platform builds.
`just host-test` exercises installed host CLIs with local response fixtures.
`just live-host-test` is an opt-in check using production models and account quota.
See [development and validation](README.md#development-and-validation).
