# Sessions and routing

Each Claude Code or supported Codex CLI session has one `interlink-mcp` process
and its own broker address, `public-key#session_id`. Multiple sessions can share
an identity and peer-policy file without sharing a polled inbox or a redb writer.

## Session identity

`get_my_session_id()` returns `{"session_id":"..."}` for the current MCP session.
It reads local state, so it works even when the broker is unavailable. On Codex,
it reports a setup error until the trusted binding hook supplies the owning
thread UUID; it never returns a provisional ID. On Claude it returns the effective
Interlink ID, including any configured session override.

Give this ID to a newly launched agent in its initial prompt so it can address a
ready message back to you. The new agent can use the same tool to include its own
ID in that reply. Sessions sharing an identity use `to="self"` with the destination
ID; agents under different identities use their configured peer names.

| Host | ID source | When it registers |
|---|---|---|
| Claude Code | `--session`, then `INTERLINK_SESSION`, then `CLAUDE_CODE_SESSION_ID`, otherwise a generated ID | Server startup |
| Codex CLI | Full thread UUID passed to `bind_codex_session` by trusted local hooks | After successful binding |

A Codex MCP instance binds once. Rebinding the same UUID is harmless; another
UUID is rejected. The Claude session override is not a substitute for Codex
binding. An unbound Codex instance does not announce or poll.
For now, send one initial user prompt to trigger binding. Registration before
the first turn remains a tracked improvement; see the
[Codex startup limitation](../codex/README.md#known-limitation-registration-before-the-first-turn).

A restart within the same host session can recover its address. A genuinely new
session has a new ID; old queued messages are not automatically migrated to it.
In Claude's default delivery mode, the MCP server and Stop listener must also
use the same ID and local state directory. See [delivery](DELIVERY.md).

## Addressing

`discover` lists retained live and away sessions, grouped by identity, including
unpaired identities. Each entry shows the session ID, working directory, git-root
label, summary, and presence. `set_summary(summary)` updates the work description and
announces it.

## Upgrading from session titles

Version 0.11.0 removes session titles, including manual overrides,
automatic names, title hooks, polling, and the Codex metadata subprocess. Use the
session's project, machine, ID, and `set_summary` work description for discovery.

Upgrade the MCP binary and Claude plugin together, then restart the clients.
Remove any custom `--title` argument and old `sync-title` hooks; those CLI options
no longer exist. `INTERLINK_TITLE` is no longer read. Saved files under the
Interlink state directory's `titles/` folder are unused and may be deleted.
Old clients' title fields are ignored when reading announcements; the original
session signatures and routing remain compatible. Already-running older releases
keep their title behavior until restarted with the new binary.

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

Separate files persist peer policy, pairing state, the shared inbound mailbox
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
