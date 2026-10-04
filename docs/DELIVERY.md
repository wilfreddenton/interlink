# Message delivery

The shared gate verifies identity, recipient, authorization, freshness, and
replay state before a message reaches a host adapter. Pairing control messages
have their own handler and generate pairing notices. See [the trust model](../DESIGN.md#the-trust-gate).

## Shared inbox and consumption

Verified peer messages are saved to `mailbox/<identity>/<session>.json` before
the broker is acknowledged. This shared path serves Claude's Stop listener,
Claude channels, and Codex. A host receives a small inbox notice containing a
`notification_id`, rather than a copy of each message body. The agent calls
`receive_messages(notification_id="...")` to fetch current unread messages.
Fetching does not consume anything. After reading a response, call
`acknowledge_messages(messages=[{"sender":"<public key>","msg_id":"<id>"}])`
with its exact receipt objects. A lost fetch response remains unread. A stale
notice always reads current state and never replays acknowledged bodies.

An inbox notice requires a tool call even when it is the only event in an idle
conversation. Silence applies only after an empty fetch or acknowledgement; it
does not mean skipping the inbox check. If a turn ends without calling either
tool, messages remain unread and notification retries continue. Unavailable
tools and fetch or acknowledgement errors should be surfaced as blockers.

There is one current notification reservation per session. It expires after
30 seconds, then retries with 60, 120, 240, and at most 300 seconds between
reservations while attention-worthy messages remain unread. Startup and a
one-second reconciliation loop recover lost notices and interrupted sends.
Notifications already queued in a host cannot be retracted, so duplicate wake-ups
are possible. Neither missing notice IDs nor manual/history consumption can
permanently block future notices. Persisted notices from the earlier mailbox
format expire on their next reconciliation.

The default fetch batch is 20 messages, with a maximum of 100; non-progress
messages come first. Acknowledge the returned receipts before fetching another
batch. Acknowledgement is idempotent, scoped to sender key and message ID, and
never consumes a newer arrival. It also releases a reservation when all messages
covered by that reservation have been acknowledged or superseded. Notice IDs are
advisory and are not required for consumption.

`conversation_history(peer, consume=false)` is read-only. Prefer reading it and
acknowledging exact receipts afterward. `consume=true` explicitly consumes exactly
the returned inbound records before the response is sent. This shortcut can lose
the response after consumption; reread history to recover it. All consumption
persists across MCP restarts with the same identity and session.

Progress (`status=update`) does not wake the host. A newer update supersedes
older progress for the same sender key, sender session hint, and task ID. A
terminal result, failure, or cancellation supersedes progress for that task,
including progress that arrives late. Superseded messages remain in history.
Questions, failures, and other non-progress messages are preserved individually.
Use distinct task IDs for new work. The sender session is the existing unsigned
`reply_to` hint, not a separately authenticated session identity.

## Delivery states

| State | What it confirms |
|---|---|
| `pending` | Outbound message is in this MCP process's outbox |
| `bus_accepted` | At least one relay accepted it; remote receiver state is unknown |
| `receiver_stored` | Verified body is committed to the receiving session's mailbox |
| `inbox_queued` | A notice was appended to Claude's local Stop-listener inbox |
| `notification_sent` | A notice was written to Claude's MCP channel |
| `host_queued` | Codex accepted the notice into its queue |
| `delivery_failed` | A host notice failed; bodies remain in the shared mailbox |
| `receiver_acknowledged` | The receiver explicitly acknowledged the message, or consumed it through history |

Inbound states are local to the receiver. No signed remote receipt protocol is
implemented: the sender never infers acknowledgement from relay acceptance.
Acknowledgement records an explicit consumption request, not model comprehension or task completion.
Host notification acceptance is also not a read receipt. Messages joining an
already outstanding notice can remain `receiver_stored` until a later handoff or acknowledgement.

This is not exactly-once delivery. Retries can repeat a notice or an unacknowledged
fetch. Acknowledged bodies are excluded from subsequent unread fetches, while
history remains available. Existing pre-upgrade notices containing full bodies
cannot be retracted. Pairing control notices retain their separate path.

## Claude default: inbox and Stop listener

The Claude plugin registers an async Stop hook:

```json
{
  "type": "command",
  "command": "npx -y interlink-mcp wait",
  "async": true,
  "asyncRewake": true,
  "timeout": 3600
}
```

No development-channel flag or `channelsEnabled` setting is needed for this
path. Hooks and MCP must still be enabled under the host's normal configuration
and organizational policy. Registering the MCP server alone does not install
the Stop hook. See [plugin setup](../plugin/README.md).

The listener waits for complete inbox records, writes inbox notices (or legacy attributed messages) to
stderr, flushes the output, commits the cursor, and exits 2. Claude's documented
`asyncRewake` behavior delivers that output and wakes an idle session. A subsequent
Stop starts another listener. A per-session exclusive OS file lock prevents
multiple active readers; duplicate listeners exit successfully without output.
See the [Claude hook reference](https://code.claude.com/docs/en/hooks#run-hooks-in-the-background).

Claude enforces the timeout for `asyncRewake`. After 50 idle minutes, Interlink
emits a local renewal notice asking the model to end the maintenance turn so Stop
can re-arm the listener before the one-hour deadline. This costs a model turn;
following the notice is host/model behavior, not something the MCP server forces.
`wait --renew-after-secs` accepts 1 through 3000 for a shorter host timeout or tests.

## Claude session rendezvous

The MCP server chooses `--session` / `INTERLINK_SESSION`, otherwise
`CLAUDE_CODE_SESSION_ID`, otherwise a generated ID. The listener chooses its own
`--session` / `INTERLINK_SESSION`, otherwise the Stop payload's `session_id`.
With no usable input, the listener falls back to `main`.

The automatic setup requires a Claude release that supplies the MCP session-ID
environment variable and supports `asyncRewake`. The host fixture was validated
on 2.1.278. A random server fallback does not match a normal hook session ID on
a host that does not supply the variable.
If pinning an ID manually, both processes must use the same value and state root.

## Durable local state

The state root is `$XDG_STATE_HOME/interlink`, or `~/.local/state/interlink` when
unset. Rust uses `USERPROFILE` if `HOME` is absent. The plugin's Node progress hook
uses the OS home directory as its fallback. Keep the host and MCP environment
consistent when overriding paths.

| Path under the state root | Contents |
|---|---|
| `mailbox/<identity>/<session>.json` | Shared inbound bodies, consumption, supersession, and notice state |
| `inbox/<session>.jsonl` | Claude notices, pairing notices, and legacy messages |
| `inbox/<session>.cursor` | Byte offset consumed by the listener |
| `inbox/<session>.lock`, `.io-lock` | Listener and read/write coordination |
| `pairing/<identity>/<session>.json` | Pending requests and unsent control messages |
| `failed/<identity>/<session>.json` | Codex failed deliveries retained for recovery |
| `task/<session>/` | Best-effort progress marker and timestamps |

Mailbox, pairing, and failed-delivery files have sidecar locks; atomic writes use temporary
files in the destination directory. `peers.json` is separate shared configuration,
protected by its own lock and atomic replacement.

Admission to the shared mailbox is bounded at 4,096 records and 32 MiB of
serialized records per identity/session. Notice metadata and acknowledgement
updates may grow it beyond the byte admission cap so a full mailbox can still
be consumed. On arrival, acknowledged and superseded records older than the
24-hour signature acceptance window are pruned. Unread records are not evicted.
If full, corrupt, or unwritable, new messages remain on the broker until storage
can accept them. A notifier lock prevents concurrent MCP processes for the same
session from issuing independent wake-ups. Do not remove live mailbox files.

Startup preserves the Claude inbox and cursor. Writers sync complete JSONL
records before broker acknowledgment. Readers start at the cursor and return up
to 64 records, stopping after a batch reaches 256 KiB (a single record can exceed
that size). A partial final record is not consumed; the next append repairs it.
The cursor commits the exact bytes returned, so concurrent appends are not skipped.

The listener flushes output before committing its cursor, but Claude supplies no
consumption acknowledgment. A crash around that handoff may lose or duplicate
output. Inbox files are not compacted; consumed records remain on disk. Avoid
removing a live session's data or cursor while either writer or listener is running.

## Claude native channels

`interlinked` sets `INTERLINK_CHANNELS=1` and starts Claude with
`--dangerously-load-development-channels plugin:interlink@interlink`. The plugin
forwards the env switch to the MCP server; the listener self-disables in channel
mode. The server cannot infer whether the host actually enabled channels.

This path requires a compatible Claude channel implementation and any applicable
organization settings. Development flags do not override a disabled channel
master switch. Current preview restrictions, allowlists, and protocol limitations
are described in the [official channel documentation](https://code.claude.com/docs/en/channels).
A successful MCP notification send alone cannot prove that Claude accepted it.

## Codex CLI

Codex mode waits for a trusted local lifecycle hook to call `bind_codex_session`
with the current thread UUID before announcing, polling, or sending. An instance
cannot be rebound to another thread. It does not advertise Claude's channel
capability. See [Codex setup and recovery](../codex/README.md).

The adapter invokes `codex queue` with process arguments, no shell, a 30-second
timeout, and a 12 KiB rendered-notice limit. Peer bodies are fetched through MCP,
so large reports do not enter the CLI argument. Nonzero exits get at most three
automatic attempts, with two-second backoff. Permanent failures, exhausted retries,
and timeouts save the notice for recovery. The mailbox retries after the cooldown
as well. Timeouts have an uncertain outcome; any retry can duplicate a notice. Ordinary bodies are already durable and
broker polling continues even if the notifier cannot save its failure.

`failed_deliveries` supports `list`, `read`, `retry`, and `discard`. Its store holds
64 failures per session, with at most one slot for current ordinary notice failures.
New failures replace that slot, and a successful automatic notice handoff clears it.
Legacy failures remain untouched. It also reads pre-upgrade saved full-body failures and
pairing notices. For current ordinary messages, use `receive_messages` or history
to recover the bodies independently of the failed notice. Discard removes the
saved failure only, not unread mailbox messages. Failed notices expire even without
manual recovery. Diagnostic-storage errors do not block the next notification attempt.
The outbound log and ordinary outbox remain in memory.

## Validation

`just test` exercises the shared gate, inbox restart/cursor behavior, concurrent
appends, partial-record repair, default renewal interval with a paused clock,
Codex binding, retry limits, saved failures, and cross-host delivery. Shared
regressions cover busy-host history consumption, restart deduplication, notification
expiry, lost fetch responses, explicit acknowledgement, progress coalescing, and
storage failures across all three adapters. Process integration tests use local
brokers and a recording queue executable.

`just host-test` requires Python 3.11+, installed `codex` and `claude` CLIs, and
localhost socket access. The recorded validation used Codex 0.160.0 and Claude
Code 2.1.278 with local response fixtures, not production models:

- Codex: reviewed hook hashes supplied for that invocation, then lifecycle hooks
  binding two ephemeral threads to separate MCP sessions. Both inbox tools are
  checked in each thread's tool inventory, then a sibling message is fetched and
  acknowledged through Codex's MCP connection. Queued delivery to ephemeral
  threads is unsupported, so that handoff uses a stub.
- Claude: a persistent stream-JSON process, a three-second listener renewal,
  Stop re-arming, and a later fixture message causing another turn.

The fixture does not cover the interactive hook-review UI, interactive Claude
TUI, native-channel acceptance, or production-model behavior. See the
[Codex validation notes](../codex/README.md#validation) for the separate queue wake
smoke test and its limits.
