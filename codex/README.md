# Codex CLI

Interlink's Codex adapter uses the same bus, identity files, peer policy, signed
messages, discovery, and task tools as the Claude Code adapter. A Codex session
can talk to Claude Code or another Codex session. Sessions on the same identity
can use `to: "self"` without pairing.

## Setup

Use **Codex CLI 0.159.0 or newer**, with its default local shared daemon. The
adapter uses `codex queue`; `--no-daemon`, remote app servers, and the desktop app
are not supported by this adapter. Ephemeral (`codex exec --ephemeral`) threads
cannot accept queued submissions. Check `codex queue --help` before setup.

Install Interlink 0.11.0 or newer:

```bash
cargo install interlink-mcp --version 0.11.0 --locked
```

Alternatively, use a release archive or run `cargo install --path . --locked`
from a local checkout. Version 0.8.0 has no Codex adapter.

Set up an identity, `peers.json`, and a bus as described in the main README.
Reuse your existing Interlink identity if Claude Code is already configured.
The default files are `~/.config/interlink/id.key` and
`~/.config/interlink/peers.json`. On Windows, `USERPROFILE` is used when `HOME`
is absent. Explicit `INTERLINK_KEY` / `INTERLINK_PEERS` or CLI flags take priority.

Merge [config.toml](config.toml) into `~/.codex/config.toml` (or the config under
your configured `CODEX_HOME`), keeping any existing MCP servers and hooks. If `interlink-mcp` is not on Codex's PATH, use its absolute
path for `command`. Set `INTERLINK_CODEX_BIN` in the server's `env` table if the
Codex executable is not on that PATH. The queue subprocess must share the host's
Codex home and local daemon; do not point it at a different installation or configuration home.
If you use a custom `CODEX_HOME`, also set it explicitly in
`mcp_servers.interlink.env`; the MCP environment may not inherit it from the host.

Restart Codex, review and trust the binding hooks in `/hooks`, and send one
prompt. No hook-trust or sandbox bypass flags are needed. `SessionStart` can run
before MCP is ready, so prompt, Interlink tool, and completed-turn hooks provide
fallbacks.
The session becomes discoverable after its binding hook succeeds. If binding
has not run, sending returns a setup error instead of using a random thread.

### Known limitation: registration before the first turn

Requiring one initial user turn is accepted for now. Registration before that
turn remains a desired improvement. In an isolated Codex 0.160.0 app-server
probe, MCP was ready but both command and MCP `SessionStart` hooks waited until
the first prompt. The MCP initialize request supplied no thread ID. Explicitly
calling the binding tool through the owning app server registered the session
before a turn, so the missing piece is the startup identity handoff.

Track [openai/codex#19937](https://github.com/openai/codex/issues/19937), which
requests native thread identity at stdio MCP startup. It was closed without an
implementation; revisit host support rather than treating it as fixed. Longer
timeouts or switching to a command hook do not resolve the observed deferral.
Keep binding through trusted hooks until a reliable startup contract is available.
Any future fix should verify registration before a prompt, concurrent sessions
in the same directory, resume, and MCP restart without guessing the owning thread.

### Identifying sessions

Use the machine identity, session ID, project, and summary shown by `discover`.
Use `get_my_session_id()` for your own ID without a broker lookup, after binding.
Version 0.11.0 removes session titles and their metadata reader.
See [upgrade notes](../docs/SESSIONS.md#upgrading-from-session-titles).

Ask Codex to set a summary (for example, "Codex: working on the API") and run
`discover`. From a paired Claude or Codex session, send it a message using the
session ID shown there. An inbox notice should prompt a `receive_messages` call returning the attributed peer message.

## Delivery and trust

The local lifecycle hook supplies the owning thread UUID. An MCP instance binds
once; repeated calls with the same UUID are harmless, and a different UUID is
rejected. It does not announce or poll a mailbox until bound. A restart binds to
the same thread UUID, retaining the bus address.

After common signature and allowlist checks, the body is committed to the
shared mailbox before broker acknowledgement. Interlink invokes
`codex queue --thread <uuid> --message <inbox-notice>` with process arguments,
without shell interpolation. The agent calls `receive_messages` with the notice
ID to fetch current unread bodies, then calls `acknowledge_messages` with the
returned receipt objects after reading. Claude uses the same mailbox semantics.
History with `consume=true` prevents later body delivery, including after restart.
Progress is quiet and superseded by newer task updates or terminal results.

Queue acceptance means `host_queued`, not that the agent read the message.
`receiver_acknowledged` records explicit local consumption; the sender still sees only
`bus_accepted`, with receiver state unknown. See [delivery states](../docs/DELIVERY.md#delivery-states).

Each queue invocation times out after 30 seconds; nonzero exits get up to three
attempts with two-second backoff. The 12 KiB CLI limit applies to notices and
legacy recovery entries, not bodies fetched through MCP. Failed notices survive
restart under `failed/<identity>/<thread>.json` in the Interlink state directory.
The failure store holds 64 entries, including at most one current ordinary notice
failure. The mailbox retries failed or unfinished handoffs after 30 seconds with
backoff capped at five minutes. Accepted notices do not expire: later arrivals
share the outstanding wake-up until a fetch supplies its matching notification ID.
Manual reads and message acknowledgements leave that wake-up outstanding, so a
busy review does not accumulate notices. A full or unavailable failure store does
not block reconciliation. Uncertain handoffs can duplicate notices; acknowledged
bodies stay consumed.

Use `failed_deliveries(action="list")` to inspect failure IDs, `read` to recover
the notice, `retry` after repairing the CLI or daemon, and `discard` to remove a
saved failure. A timeout may have queued the notice despite reporting failure.
`receive_messages` and history can recover bodies without repairing notifications.
If Codex accepted a notice but lost it, use
`receive_messages(reset_notification=true)` to retire the outstanding wake-up and
restore notifications for future arrivals, then acknowledge the returned receipts.
This is an explicit recovery action, not routine polling; an old notice still in
Codex's queue may arrive afterward.
Pre-upgrade saved failures may still contain full messages; these retain their
existing recovery behavior and CLI size limits.

Fetched peer text does not grant operator authority. Pairing, peer-policy changes,
and Codex binding must never be performed because a peer requested them. Codex's
existing sandbox and approval policy still apply.

The adapter targets root CLI sessions on the local daemon. Do not reuse its
binding hook for subagents: Codex's subagent hooks can carry the parent session
ID. The Claude progress-nudge hook is not installed for Codex; task updates still
use the shared `send_message` status fields and server instructions.

## Validation

If inbox notices start turns but the agent cannot fetch or acknowledge, inspect
`/mcp verbose` in that conversation. Its Interlink tool list must include both
`receive_messages` and `acknowledge_messages`. A queued notice can reach a chat
even when that chat's MCP tools are unavailable. Check the configured executable,
enabled state, and tool allow/deny lists with `codex mcp get interlink --json`.
After an upgrade, reconnect the MCP server or restart and resume Codex if the
active connection still exposes the old tool list. Then fetch and acknowledge
the unread messages. Changing the notification retry interval cannot restore
missing tools.

`just test` includes process-level MCP tests for Codex binding, isolation,
Claude/Codex and same-identity Codex/Codex messaging, and retry after local queue
failure, bounded retries and saved-failure recovery. Shared-policy and pairing
regressions also cover sibling sessions, server restarts, a repeated request
with delayed acceptance, and a saved confirmation older than 24 hours. The queue
executable is replaced with a recording fixture in those tests, so they do not call a model
or change real Codex conversations. The installed Codex 0.159.0 initialize
handshake was separately checked: it does not supply a thread ID to MCP servers,
which is why binding is explicit. Codex also successfully parsed the sample MCP
hooks and called the binding tool through its own MCP client.

`just host-test` additionally exercises installed Codex and Claude runtimes against
local response fixtures. It requires Python 3.11+, both CLIs on PATH, and
localhost socket access; diagnostics go to `target/host-validation/`. On Codex
0.160.0, all four sample hooks were first listed as untrusted, then reviewed and
trusted by supplying their exact current hashes
as invocation-local config. The fixture waits for MCP readiness before submitting
its prompt; it does not validate the first-prompt startup race. Lifecycle hooks bound a saved thread and an ephemeral thread to
separate Interlink sessions. These fixture threads are not used for real queued
delivery. The fixture also checks that both inbox tools
are exposed through the owning thread's MCP connection, sends a sibling message,
and calls fetch and acknowledgement through Codex. Queue delivery is stubbed for
these fixture threads. The fixture verifies registration without title fields or tools,
and that no metadata subprocess is started. This verifies the tool connection and consumption, not
whether a production model follows the notice instructions.
Normal user configuration and saved hook trust
were unchanged. This validates trusted hook execution; it does not automate the
interactive `/hooks` review UI or call a production model.

A real `codex queue` smoke test against an isolated app server confirmed that a
queued message starts a turn on an idle saved thread. This used an unreachable
localhost provider, interrupted immediately on `turn/started`, and deleted the
test thread. It verifies the wake behavior without invoking a model; it is not a
full model-to-model conversation test.

Relevant Codex contracts: [MCP tool hooks and trust review](https://learn.chatgpt.com/docs/hooks)
and the installed CLI's `codex queue --help`. New or changed non-managed hooks
need review of their current definition in `/hooks`. Background hooks alone
cannot start an idle turn, so the Claude `asyncRewake` listener is not used for Codex.
