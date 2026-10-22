# Message delivery

The shared gate verifies identity, recipient, authorization, freshness, and
replay state before a message reaches a host adapter. Pairing control messages
have their own handler and generate pairing notices. See [the trust model](../DESIGN.md#the-trust-gate).

## Shared inbox and consumption

Verified peer messages are saved to `mailbox/<identity>/<session>.json` before
the broker is acknowledged. This shared path serves Claude's local socket, Stop listener,
Claude channels, and Codex. A host receives a small inbox notice containing a
`notification_id`, rather than a copy of each message body. The agent calls
`receive_messages(notification_id="...")` to fetch current unread messages.
Fetching does not consume anything. After reading a response, call
`acknowledge_messages(messages=[{"sender":"<public key>","msg_id":"<id>"}])`
with its exact receipt objects. A lost fetch response remains unread. A stale
notice always reads current state and never replays acknowledged bodies.

The Codex sample also installs an end-of-turn inbox guard. Its prompt hook marks
the current notice as started only when the incoming prompt exactly matches that
notice. This persisted marker distinguishes a dequeued notice from one still
waiting behind a busy turn. The Stop hook requests one fetch/acknowledgement
continuation for unread non-progress messages. It supplies the notice ID only
when prompt delivery was observed; otherwise it leaves the queued wake-up intact.
It may retire a started notice with an empty inbox, but never acknowledges bodies.
If a Stop hook already continued the turn, remaining unread work produces a UI
warning instead of another continuation. No timer or extra queue submission is
used. This requires the updated, trusted hooks and does not recover an idle
thread whose notice was lost before prompt delivery, or restart interrupted work.

An inbox notice requires a tool call even when it is the only event in an idle
conversation. Silence applies only after an empty fetch or acknowledgement; it
does not mean skipping the inbox check. If a turn ends without fetching and
acknowledging, messages remain unread. An accepted notice does not trigger timed
reminders. Unavailable tools and fetch or acknowledgement errors should be
surfaced as blockers.

There is at most one outstanding accepted wake-up per session. Once a host
handoff succeeds, the notice stays outstanding until a fetch supplies its matching
`notification_id`, even if manual reads or history have already consumed all its
messages. New arrivals share that wake-up. Acknowledgement does not cancel a
notice already queued in the host or allow another one to accumulate behind it.

A matching fetch retires the wake-up and covers the current inbox, including
arrivals since handoff. Fetching does not consume bodies. Drain full batches and
acknowledge the exact returned receipts; unread messages covered by that fetch do
not cause reminders. Later arrivals can produce a new wake-up. Stale or missing
notice IDs still fetch current unread state, but cannot retire another notice.
The default batch is 20 messages, with a maximum of 100; non-progress messages
come first. Acknowledgements are idempotent, scoped to sender key and message ID,
and never consume a newer arrival.

Failed or unfinished handoffs retry after 30 seconds, then 60, 120, 240, and at
most 300 seconds between reservations while attention-worthy messages remain
unread. A one-second reconciliation loop resumes this work after restart.
Accepted notices and their observation state also survive MCP restarts. Older
mailboxes without observation metadata are treated as unobserved; an accepted
legacy notice is not resent merely because its old retry deadline expired.

After upgrading, reconnect or restart the receiving Interlink MCP processes in
Claude and Codex so they run the new notification logic and expose the recovery
option. This change requires no broker restart or mailbox reset. Preserve mailbox
files: they contain unread messages and acknowledgement state. Notices already
queued by older processes may still arrive and return an empty inbox.

If a host accepts a notice but loses it, or the fetch response is lost, use
`receive_messages` to recover unread bodies. For a lost or stuck host notice,
`receive_messages(reset_notification=true)` explicitly retires the outstanding
wake-up without its ID and restores notification eligibility for later arrivals.
Use this only for recovery, not routine polling: it cannot retract a notice that
is still queued. Read and acknowledge the returned messages as usual. Previously
queued notices, uncertain handoffs, and a crash between host acceptance and saving
that result can still produce stale wake-ups; notifications are not exactly-once.

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
| `socket_sent` | A notice was written to Claude's local socket; this does not prove host acceptance or reading |
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

## Claude default: local messaging socket

Interlink 0.12.0 prefers the local inbox socket supplied by Claude as
`CLAUDE_CODE_MESSAGING_SOCKET`, authenticating with the owning session's
`CLAUDE_CODE_MESSAGING_TOKEN`. Both are supplied to the MCP process on the tested
Claude Code 2.1.308 host. Interlink opens a fresh connection for each notification,
sends a JSONL auth frame followed by a user-message frame containing an attributed
Interlink notice, and closes its write side. The combined connect/write/shutdown
operation has a three-second timeout. Unix uses a domain socket; Windows uses a
local named pipe. The Windows path has not been host-validated.

The Interlink broker remains independently configurable with `INTERLINK_URL`,
including a broker on a trusted private network. Only delivery from the local
MCP process into Claude uses the socket.

`INTERLINK_CLAUDE_DELIVERY` selects `auto`, `socket`, or `inbox`. Auto uses the
socket when its path is present, otherwise the legacy listener. A socket path
without the host token is an error. Explicit socket mode requires both. Set the
mode before launching Claude so the server and Stop hook receive the same value.
Explicit `INTERLINK_CHANNELS=1` takes precedence and retains native channels.

In socket mode, `interlink-mcp wait` exits successfully immediately, without
reading the inbox or scheduling renewal. Claude therefore has no Interlink
maintenance turns while idle. Connection failures use the existing mailbox retry
and backoff; they never enable the Stop listener as a fallback.

The socket does not provide an acceptance receipt. A successful write is recorded
as `socket_sent` and coalesces further notices until a matching inbox fetch.
Claude can hold or refuse that notice according to its
[inbound controls](https://code.claude.com/docs/en/cross-session-messaging#control-inbound-messages).
A completed socket write must never be interpreted as model delivery. Messages
remain unread in Interlink until explicitly acknowledged. If Claude refuses or
loses a notice, repair the host configuration, then use
`receive_messages(reset_notification=true)` and acknowledge the returned bodies.
This adapter does not change Claude's inbound policy.

For an upgrade from the Stop listener, update both the server and listener binary
and restart Claude. Under the notifier lock, socket mode reissues any unobserved
`inbox_queued` notice; its bodies stay unread. Already fetched notices and
`socket_sent` notices survive MCP restarts without a new wake-up. An old listener
still running can produce one obsolete notice, whose ID cannot retire a newer
notification. Manually changing between other delivery modes may require explicit
notification recovery. Socket support requires Interlink 0.12.0 or newer.

## Claude legacy: inbox and Stop listener

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

For an installed marketplace plugin, opt into channels explicitly:

```bash
INTERLINK_CHANNELS=1 claude --dangerously-load-development-channels plugin:interlink@interlink
```

The plugin forwards the env switch to the MCP server; the listener self-disables
in channel mode. This plugin identifier applies to the marketplace install, not
the local `--plugin-dir` path. The server cannot infer whether the host actually
enabled channels.

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
regressions cover busy-host reads and history consumption without accumulating
wake-ups, restart deduplication, failed-handoff expiry, stale IDs, explicit notice
recovery, partial batches, lost fetch responses, acknowledgement, progress
coalescing, and storage failures. Socket tests cover authenticated framing,
connection failure, fresh connections after socket replacement, and migration
from a pending legacy inbox notice. Process integration
tests use local brokers and a recording queue executable.

`just host-test` requires Python 3.11+, installed `codex` and `claude` CLIs, and
localhost socket access. The recorded validation used Codex 0.160.0 and Claude
Code 2.1.278 with local response fixtures, not production models:

- Codex: reviewed hook hashes supplied for that invocation, then lifecycle hooks
  binding a saved thread and an ephemeral thread to separate MCP sessions. Both inbox tools are
  checked in each thread's tool inventory, then a sibling message is fetched and
  acknowledged through Codex's MCP connection. Queued delivery to ephemeral
  threads is unsupported, so that handoff uses a stub. The fixture also
  verifies registration without titles or metadata subprocess launches.
- Claude: a persistent stream-JSON process, a three-second listener renewal,
  Stop re-arming, and a later fixture message causing another turn. The MCP server
  registers the session without title hooks.

The socket fixture uses Claude 2.1.308 and the real Interlink binaries without a
compatibility shim. It verifies broker-to-socket delivery, MCP fetch and
acknowledgement, refusal without consumption, and an accelerated Stop hook that
stays disabled in socket mode. Model responses come from a local fixture, and the
broker runs in a separate process on the same machine. It does not validate an
overnight idle interval, cross-machine networking, or Windows named pipes.

Newer Claude hosts can send `server/discover` before MCP initialization. Interlink
returns JSON-RPC method-not-found for that probe and keeps the transport open for
the standard `initialize` handshake. It does not claim support for the newer
discovery protocol. Process tests exercise repeated probes with string and
numeric IDs, a pre-initialization ping, and subsequent tool calls.

The fixture does not cover the interactive hook-review UI, interactive Claude
TUI, native-channel acceptance, or production-model behavior. See the
[Codex validation notes](../codex/README.md#validation) for the separate queue wake
smoke test and its limits.

On Codex 0.163.0 the fixture also submits a recorded notice through the host's
queue API to a provider that deliberately never fetches the inbox. It verifies
one Stop continuation, a visible warning after that attempt, retained unread
bodies, no duplicate notification, and a new notification after recovery. Unit
tests cover pending versus started notices, restart persistence, manual
consumption, and prompt/acceptance ordering.

### Live model validation

`just live-host-test` is a separate, opt-in check that makes real model calls and
consumes account quota. It builds and uses this checkout's binaries. Install
`tests/host/requirements-live.txt` into the Python environment used by
`python3`. Both CLIs must already be logged in. The test currently requires Unix
sockets and a Codex `auth.json` login file. It uses the model and reasoning effort
from the user's Codex configuration and Claude's default model.

The test creates a private broker, fresh identity, local Claude plugin copy, and
separate Codex home and shared daemon. It reviews the four binding hooks and trusts
their current hashes only in that test home. Only the test's session lookup,
discovery, send, fetch, and acknowledgement tools are preapproved. The Codex
filesystem sandbox remains read-only. Normal host configuration, peer policy,
and existing conversations are not changed.

It waits for the initial turns to finish, then checks idle wake-up through real
`codex queue` and Claude's local messaging socket,
explicit message acknowledgement, six-message bursts to each host, and replies
in both directions between the models. A controller manually fetches and
acknowledges incoming messages without observing its queued notice; that notice
must remain the only queued notice throughout the exchange. This exercises the
notification backlog regression across actual model turns. After the exchange,
Claude must remain quiet for ten seconds despite a one-second legacy listener
renewal setting. Hook lifecycle events do not count as model turns.

The fixture suite and live checks passed on 2026-10-18 with Codex 0.163.0 and
Claude Code 2.1.301. The live run used an installed development build containing
the Stop guard and version-reporting changes, before its version was bumped from
0.11.1 to 0.11.2.

On 2026-10-22, live checks passed with Codex 0.163.0 and Claude Code 2.1.308
using this checkout's socket-delivery build (still reporting version 0.11.2).
Both hosts were idle before their wake-up messages, fetched and acknowledged
all six burst messages each, and completed a round trip in each direction.
Claude remained quiet for ten seconds with a one-second legacy renewal setting,
and no legacy inbox was created for its session. These were production-model
calls through the local broker, not simulated model responses. This does not
establish overnight idle behavior or remote-broker connectivity.

Diagnostics and a successful-run `summary.json` are saved under
`target/host-validation/live/`. Test processes and the temporary Codex home are
cleaned up on exit. The existing fixture tests remain useful for deterministic
failure and restart coverage. This live check does not cover the interactive
terminal UI, native Claude channels, or cross-machine network failures.
