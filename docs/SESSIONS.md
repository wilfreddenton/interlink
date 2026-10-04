# Sessions and routing

Each Claude Code or supported Codex CLI session has one `interlink-mcp` process
and its own broker address, `public-key#session_id`. Multiple sessions can share
an identity and peer-policy file without sharing a polled inbox or a redb writer.

## Session identity

| Host | ID source | When it registers |
|---|---|---|
| Claude Code | `--session`, then `INTERLINK_SESSION`, then `CLAUDE_CODE_SESSION_ID`, otherwise a generated ID | Server startup |
| Codex CLI | Full thread UUID passed to `bind_codex_session` by trusted local hooks | After successful binding |

A Codex MCP instance binds once. Rebinding the same UUID is harmless; another
UUID is rejected. The Claude session override is not a substitute for Codex
binding. An unbound Codex instance does not announce or poll.

A restart within the same host session can recover its address. A genuinely new
session has a new ID; old queued messages are not automatically migrated to it.
In Claude's default delivery mode, the MCP server and Stop listener must also
use the same ID and local state directory. See [delivery](DELIVERY.md).

## Addressing

`discover` lists retained live and away sessions, grouped by identity, including
unpaired identities. Each entry shows the optional title, session ID, working directory, git-root
label, summary, and presence. `set_summary(summary)` updates the work description and
announces it.

## Titles

Version 0.10.2 and newer automatically choose a readable display title.
Version 0.10.1 supports only explicitly supplied titles.

The precedence is:

1. A saved `set_session_title` override, or `--title` / `INTERLINK_TITLE` when no
   saved override decision exists.
2. The native host conversation title.
3. Project (or working-directory name), node name, host, and the last eight
   session-ID characters, such as `Motif · mac · Codex · 56789abc`.

`set_session_title(title="Interlink development")` pins an Interlink-only title.
`set_session_title(title="")` clears that override, including a startup default,
and restores automatic naming. This choice and the last known native title
persist across MCP restarts, keyed by host and full session ID. A new session
gets its own state. Temporary lookup errors preserve the last known title;
a successful lookup reporting no native title restores the fallback.

Codex uses a separate, reusable `codex app-server --stdio` metadata connection,
reading only the owning thread with `thread/read` and `includeTurns=false`.
It never starts, resumes, or subscribes to a thread. Renames are checked every
five seconds without a model turn. Requests time out after three seconds;
lookup failures retry after thirty seconds. This reader uses the same Codex
executable and `CODEX_HOME` as the queue adapter. An unavailable or older metadata
API leaves the saved title or fallback usable and does not block registration.

Claude's plugin runs `interlink-mcp sync-title` on `SessionStart` and
`UserPromptSubmit`. These local hooks copy the custom `session_title` supplied by
Claude into the shared title state. `/rename` is reflected after the next prompt,
then within the five-second refresh interval. Claude-generated titles are not
included in that hook field, so unnamed Claude conversations use the fallback.
The hook and MCP server must share their Interlink state directory and effective
session ID. If you pin `INTERLINK_SESSION`, set the same value for both processes.
If the MCP uses `--session`, also pass it to the hook, for example
`interlink-mcp sync-title --session pinned-id`. The override takes precedence over
Claude's native hook ID.

Titles appear in discovery and selection lists without changing IDs, summaries,
or routing. Duplicate titles are allowed: resolve the human description against
the roster, then address the full session ID; ask when multiple sessions match.
Explicit titles are trimmed, limited to 256 UTF-8 bytes, and reject control
characters. Native names are bounded and cleaned for display. Interlink never
renames the host conversation.

## Selecting a session

`send_message(to="desktop", session="<id>", text="...")` selects a session
explicitly. An exact ID or a unique prefix of a roster entry is expanded to the
full ID. An ambiguous prefix is rejected. If nothing matches, the supplied value
is used literally and reported as gone; use a full ID when addressing an offline
session because an unknown prefix cannot be expanded.

Without `session`, selection is:

1. Use the last inbound sender session for this peer if it is still on the roster,
   whether live or away.
2. Otherwise use its single live session. Multiple live sessions require a choice.
3. With no live sessions, use a single away session. Multiple away sessions also
   require a choice.
4. With no roster entries, a remembered peer session may be used speculatively,
   with a gone warning. Without a remembered address, sending fails.

Reply stickiness is keyed by peer identity, not by task ID. Concurrent tasks with
different sessions under one key should specify `session` explicitly on sends.
`cancel_task` uses automatic routing and has no explicit session parameter.

The signed recipient is the bare public key. `reply_to` is an unsigned routing
hint, accepted only if its key matches the signed sender. `in_reply_to` is a
separate, signed message-correlation field.

## Sessions under one identity

`send_message(to="self", session="<id>", text="...")` reaches another session
sharing this key, including Claude-to-Codex delivery. No self-entry in
`peers.json` is required. The current session is excluded from routing candidates,
and explicit attempts to message itself are rejected.

Pairing admits an identity through the shared policy, but a knock and its
confirmation are delivered to specific sessions. A request prefers a live target
and can use an away one. The confirmation returns to the exact requesting session.
See [pairing](DISCOVERY.md).

## State and recovery

The ordinary outbox, outbound log, gate replay set, and sticky routes are in memory.
They survive suspension with the process, but are lost when it restarts.
`INTERLINK_AGENT_DB` / `--db` on the agent are accepted but ignored.

Separate files persist session title metadata, peer policy, pairing state, the shared inbound mailbox
and consumption, the Claude notification inbox and cursor, and Codex failed notices. These survive reopening the same session with the
same state directory. The broker queue survives a broker restart only with
`--db`; its roster never persists and is rebuilt by announcements.

On graceful shutdown, the MCP server stops and drains its background workers,
removes its roster entries, then attempts a signed goodbye to sticky peers.
Both network operations are best-effort. A crash or power loss leaves presence
until expiration. Sleep keeps the process and ID intact, so it reconnects and
resumes polling after waking. See [presence](PRESENCE.md).

A broker inbox is not deleted when presence expires. There is a per-recipient
message cap, but no abandoned-queue sweep. Local inbox files also have no automatic
compaction. Recovering an old session still depends on the 24-hour signed-message
freshness window for messages waiting at the broker; three-day presence retention
does not promise three days of deliverable messages.
