# praetor plugin

Bundles the whole praetor setup for Claude Code into one install: the MCP server
(via `npx praetor-mcp`), the two `PreToolUse` guard hooks, and the `read-only`
capability agent. No hand-editing `settings.json`.

## Install

```
/plugin marketplace add wilfreddenton/praetor
/plugin install praetor@praetor
```

That registers, in every session:
- **MCP server** `praetor` — `send_message`, `fetch_request`, `add_peer`,
  `list_peers`, `remove_peer`, `message_status`, `conversation_history`,
  `list_pending`.
- **Hooks** — `pretooluse-guard.sh` (keeps `fetch_request` out of the main
  agent) and `peer-admin-guard.sh` (keeps `add_peer`/`remove_peer` out of
  subagents), wired via `${CLAUDE_PLUGIN_ROOT}`.
- **Agent** — `read-only`, a scoped capability handler.
- **Skill** — `praetor`, an on-demand playbook for operating the mesh (relaying a
  request and reporting the reply, handling incoming messages, onboarding peers
  via discover/pairing).

## One-time setup

The MCP config points at a standard config location; create your identity and
allowlist there:

```bash
mkdir -p ~/.config/praetor ~/.local/state/praetor
praetor-keygen --out ~/.config/praetor/id.key      # from `npx praetor-mcp`'s crate, or cargo install
printf '{}\n' > ~/.config/praetor/peers.json       # then add peers with add_peer
```

Set `PRAETOR_URL` in your environment if your bus isn't on `127.0.0.1:9440`.

To **receive** pushed messages, launch with the channel flag (research preview):

```bash
claude --dangerously-load-development-channels server:praetor
```

## Requirements

- **`praetor-mcp` on npm** (`npx -y praetor-mcp` resolves the pure-Rust binary).
  Or swap the `.mcp.json` `command` for a `cargo install`ed `praetor-mcp`.
- **A POSIX shell** for the guard hooks (macOS/Linux, or Git Bash on Windows).
  The MCP server and agent are cross-platform; the bash guards are the *nix
  enforcement layer.
