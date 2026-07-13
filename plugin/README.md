# interlink plugin

Bundles the interlink setup for Claude Code into one install: the MCP server (via
`npx interlink-mcp`) and the `interlink` skill. No hand-editing `settings.json`.

## Install

```
/plugin marketplace add wilfreddenton/interlink
/plugin install interlink@interlink
```

That registers, in every session:
- **MCP server** `interlink` — `send_message` (with task tracking: `task_id` /
  `status` / `in_reply_to`), `cancel_task`, `list_peers`, `add_peer`,
  `remove_peer`, `message_status`, `conversation_history`, `list_pending`,
  `discover`, and pairing (`request_pair` / `list_pair_requests` / `accept_pair`
  / `reject_pair`).
- **Skill** — `interlink`, an on-demand playbook for chatting with a peer,
  surfacing incoming messages, and connecting a peer via discover/pairing.
- **Hook** — a `PostToolUse` progress-nudge (Node, cross-platform): while a session
  executes a peer's task and goes quiet, it reminds the model to send a progress
  update. Debounced + task-gated; tune with `INTERLINK_PROGRESS_INTERVAL` (seconds,
  default 60; `0` disables).

## One-time setup

The MCP config points at a standard config location; create your identity and
allowlist there:

```bash
mkdir -p ~/.config/interlink ~/.local/state/interlink
interlink-keygen --out ~/.config/interlink/id.key   # from `npx interlink-mcp`, or cargo install
printf '{}\n' > ~/.config/interlink/peers.json       # then add peers via pairing or add_peer
```

Set `INTERLINK_URL` in your environment if your bus isn't on `127.0.0.1:9440`.

To **receive** pushed messages, launch with the channel flag (research preview):

```bash
claude --dangerously-load-development-channels server:interlink
```

## Requirements

- **`interlink-mcp` on npm** (`npx -y interlink-mcp` resolves the pure-Rust
  binary). Or swap the `.mcp.json` `command` for a `cargo install`ed
  `interlink-mcp`.
- Cross-platform: the MCP server runs anywhere Claude Code does.
