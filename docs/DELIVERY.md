# Message delivery

The shared gate verifies identity, recipient, authorization, freshness, and
replay state before a message reaches a host adapter. Pairing control messages
have their own handler and generate pairing notices. See [the trust model](../DESIGN.md#the-trust-gate).

## Host paths

| Host | Last hop | Broker acknowledgment follows |
|---|---|---|
| Claude Code, default | Append to local inbox; Stop hook drains it | Successful synced inbox append |
| Claude Code, optional channels | `notifications/claude/channel` | Successful notification send |
| Codex CLI | `codex queue` for the bound thread | Queue acceptance, or durable retention of a failed delivery |

These are transport handoffs, not acknowledgments that the model consumed or
completed the request. The adapters use one path per session. Retries, process
restarts, and a crash during a handoff can still cause duplicate delivery.

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

The listener waits for complete inbox records, writes attributed messages to
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
| `inbox/<session>.jsonl` | Claude messages appended after verification |
| `inbox/<session>.cursor` | Byte offset consumed by the listener |
| `inbox/<session>.lock`, `.io-lock` | Listener and read/write coordination |
| `pairing/<identity>/<session>.json` | Pending requests and unsent control messages |
| `failed/<identity>/<session>.json` | Codex failed deliveries retained for recovery |
| `task/<session>/` | Best-effort progress marker and timestamps |

Pairing and failed-delivery files have sidecar locks; atomic writes use temporary
files in the destination directory. `peers.json` is separate shared configuration,
protected by its own lock and atomic replacement.

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
timeout, and a 12 KiB rendered-message limit. Nonzero exits get at most three
automatic attempts, with two-second backoff. Permanent failures, exhausted retries,
and timeouts are saved locally before acknowledging the broker, allowing later
messages to proceed. Timeouts have an uncertain outcome; manual retry can duplicate
an already accepted message.

`failed_deliveries` supports `list`, `read`, `retry`, and `discard`. Its store holds
64 failures per session. If persistence fails or the store is full, the current
message stays unacknowledged and only persistence is retried. The ordinary
conversation log is still in memory; use the recovery tool after a restart.

## Validation

`just test` exercises the shared gate, inbox restart/cursor behavior, concurrent
appends, partial-record repair, default renewal interval with a paused clock,
Codex binding, retry limits, saved failures, and cross-host delivery. Process
integration tests use local brokers and a recording queue executable.

`just host-test` requires Python 3.11+, installed `codex` and `claude` CLIs, and
localhost socket access. The recorded validation used Codex 0.160.0 and Claude
Code 2.1.278 with local response fixtures, not production models:

- Codex: reviewed hook hashes supplied for that invocation, then lifecycle hooks
  binding two ephemeral threads to separate MCP sessions. This tests binding,
  not queued delivery to ephemeral threads, which is unsupported.
- Claude: a persistent stream-JSON process, a three-second listener renewal,
  Stop re-arming, and a later fixture message causing another turn.

The fixture does not cover the interactive hook-review UI, interactive Claude
TUI, native-channel acceptance, or production-model behavior. See the
[Codex validation notes](../codex/README.md#validation) for the separate queue wake
smoke test and its limits.
