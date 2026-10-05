# Interlink plugin for Claude Code

The plugin registers the MCP server, the Interlink skill, a progress-reminder
hook, and the Stop listener for incoming messages. It configures Claude Code;
Codex has a [separate setup](../codex/README.md). Either host can talk to the other
through the same broker and identity policy.

## Published installation

```bash
claude plugin marketplace add wilfreddenton/interlink
claude plugin install interlink@interlink
```

The plugin invokes `npx -y interlink-mcp` for both the server and listener. Use
version 0.11.1 or newer for the notification backlog fix and explicit recovery. If upgrading,
update the installed plugin and restart Claude so it reloads the server and hooks.
The local instructions below are for testing development changes.
Version 0.11.0 removes title support; update the binary and plugin together
and follow the [upgrade notes](../docs/SESSIONS.md#upgrading-from-session-titles).

## Using this checkout

From the repository root, install the local binaries:

```bash
cargo install --path . --locked
```

In your local plugin copy:

1. In `plugin/.mcp.json`, change the Interlink server command to the installed
   binary's absolute path and its `args` to `[]`, retaining the env settings.
2. In `plugin/hooks/hooks.json`, change the Stop hook command to the same binary
   followed by `wait`, quoting the executable path if it contains spaces. Keep
   `async`, `asyncRewake`, and the timeout unchanged.
3. Disable any installed copy of Interlink in Claude's plugin manager so the
   session has only one Interlink MCP registration and hook set. Load the local
   plugin from the repository root:

```bash
claude --plugin-dir "$PWD/plugin"
```

Changing only the MCP command would leave the listener running the released npm
binary. Keep both on the same build. These are local test configuration changes;
restore the npm commands before committing release packaging.

## One-time setup

Reuse existing identity and policy files. On a new identity:

```bash
mkdir -p ~/.config/interlink ~/.local/state/interlink
interlink-keygen --out ~/.config/interlink/id.key
printf '{}\n' > ~/.config/interlink/peers.json
interlink-bus --db ~/.local/state/interlink/bus.redb
```

The npm package supplies only the MCP binary. Get keygen, broker, and launcher
from the source install or a full release archive. Run one broker for the mesh;
set `INTERLINK_URL` before starting Claude if it is remote.

The bundled `.mcp.json` explicitly uses `${HOME}/.config/interlink/` for the key
and policy. If your home is represented differently, or you use custom paths,
edit those env entries to match. On Windows, ensure those entries expand to valid
paths; the Rust binary's `USERPROFILE` fallback does not replace a path explicitly
passed by the plugin. The shell examples above use POSIX syntax.

## Delivery and hooks

Launch plain `claude` after setup. The default listener needs hooks and MCP to be
allowed, but does not require the Claude channel feature. Use a Claude version
that provides `CLAUDE_CODE_SESSION_ID` to MCP and supports `asyncRewake`; the host
fixture was validated on 2.1.278.

- `Stop`: runs `interlink-mcp wait` asynchronously. Verified messages are drained
  from the durable local inbox; exit 2 wakes the host. An OS file lock limits the
  listener to one process per session. After 50 idle minutes it requests a brief
  maintenance turn to renew before the one-hour hook timeout.
- `PostToolUse`: the Node progress hook runs after Bash, Edit, and Write, and
  nudges the model when a marked task has gone quiet. Set
  `INTERLINK_PROGRESS_INTERVAL` in the host environment (default 60 seconds;
  zero disables). It is a reminder, not a guaranteed progress sender.

For an installed marketplace plugin with native channels available, `interlinked`
sets `INTERLINK_CHANNELS=1` and adds the development-channel flag. The Stop listener
then self-disables. Applicable organization settings still apply. The launcher
names `plugin:interlink@interlink`, so it is intended for that marketplace install,
not the local `--plugin-dir` testing path.

## Tools

The shared tools cover messaging, task status, cancellation, session summaries,
discovery, peer management, pairing, and local message history. `bind_codex_session`
and `failed_deliveries` are exposed by the common server for the Codex workflow.
Binding and retrying a saved Codex failure require Codex mode; Claude delivery
does not create saved Codex failures.

See [the operating skill](skills/interlink/SKILL.md),
[delivery and recovery](../docs/DELIVERY.md),
[pairing](../docs/DISCOVERY.md), and
[progress reminders](../docs/AUTO-PROGRESS.md).
