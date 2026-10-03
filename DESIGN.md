# Design notes

## Architecture

Independent Claude Code and Codex CLI sessions exchange signed messages through
one or more HTTP brokers. Each session runs its own `interlink-mcp` process.
The broker routes opaque payloads to `key#session_id`; the agent authenticates
messages and applies its local peer policy before delivering them to the host.

| Host | Delivery | Session identity |
|---|---|---|
| Claude Code, default | Durable local inbox and an `asyncRewake` Stop listener | Claude's session ID |
| Claude Code, optional channels | `notifications/claude/channel` | Claude's session ID |
| Codex CLI | `codex queue` through the local shared daemon | Thread UUID supplied by trusted lifecycle hooks |

See [delivery](docs/DELIVERY.md), [sessions](docs/SESSIONS.md), and
[Codex setup](codex/README.md). Codex support and the persistence improvements require Interlink 0.9.0 or newer.

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
future. A bounded, in-memory set remembers 4096 message IDs per MCP process.
It reduces duplicate delivery across retries and relays; it does not provide
exactly-once processing across restarts or after entries leave the set.

Peer-policy updates take a file lock, reload the latest policy, and atomically
replace the file. Readers reload it, so sibling sessions see updates without a
restart. Existing names cannot silently be reassigned to different keys.

## Admission and operator authority

Pairing admits a full chat partner. Its messages enter the model's context and
may drive collaboration, subject to the host's existing permissions and sandbox.
They do not authorize changes to trust or become the local operator's consent.
Pairing, peer-policy changes, and Codex thread binding are local setup or operator
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

Receiving needs a host-specific wake mechanism. The Claude listener writes
attributed messages to stderr and exits 2; Codex receives attributed queue input;
native Claude channels receive notifications. A local delivery error prevents
bus acknowledgment until delivery succeeds or, for Codex, the failure is durably
saved for recovery. See [delivery guarantees and limits](docs/DELIVERY.md).

## Persistence and bounds

The bus uses redb. Supplying `--db` makes its queues persistent; without that flag
it is an in-memory broker. Each recipient queue defaults to 1024 messages and
drops the oldest when full. The presence roster is in memory even with `--db`.
Unregistering a session does not delete its queued messages.

Each agent keeps the ordinary outbox, conversation log, replay set, and reply
stickiness in memory. They survive process suspension, but not process restart.
A shared agent redb file would prevent multiple sessions from opening the store,
so `--db` / `INTERLINK_AGENT_DB` on the MCP server remain accepted but ignored.

Separate local files persist peer policy, pairing requests and control messages,
Claude inbox records and cursors, and Codex failed deliveries. Pairing and failed
delivery files are scoped by identity and session. Pairing retries preserve the
outstanding correlation ID and refresh the signature when sending a saved job.
This does not renew a message already held by the broker.

These mechanisms provide durable handoffs with limited guarantees. Queue overflow, expired signatures, unavailable storage, an abandoned
session ID, and a crash during the host handoff remain relevant failure cases.

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
published package is `interlink-mcp`; the library import is `interlink`.

- `identity`, `policy`, and `agent`: signatures, authorization, dispatch, dedupe.
- `policy_store`, `state`, and `pairing`: shared policy and durable control state.
- `inbox`, `codex`, and `delivery`: local inbox, queue delivery, saved failures.
- `src/bin/mcp/delivery.rs`: host sinks and message rendering.
- `bus`, `store`, and `route`: transport queues, storage, and session addresses.

`just ci` runs formatting, Clippy, tests, and the no-C-dependencies checks.
GitHub CI additionally checks the feature powerset and native platform builds.
`just host-test` exercises installed host CLIs with local response fixtures.
See [development and validation](README.md#development-and-validation).
