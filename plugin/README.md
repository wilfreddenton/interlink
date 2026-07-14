# interlink plugin

Bundles the interlink setup for Claude Code into one install: the MCP server (via
`npx interlink-mcp`) and the `interlink` skill. No hand-editing `settings.json`.

## Install

From the `claude` CLI (or the same as `/plugin …` slash commands in a session):

```bash
claude plugin marketplace add wilfreddenton/interlink
claude plugin install interlink@interlink
```

That registers, in every session:
- **MCP server** `interlink` — `send_message` (with task tracking: `task_id` /
  `status` / `in_reply_to`, and `session` to target one of a peer's live sessions),
  `cancel_task`, `set_summary` (describe + register this session for discovery),
  `list_peers`, `add_peer`, `remove_peer`, `message_status`,
  `conversation_history`, `list_pending`, `discover` (identity → live sessions;
  optional `peer` to filter to one), and pairing (`request_pair` /
  `list_pair_requests` / `accept_pair` / `reject_pair`).
- **Skill** — `interlink`, an on-demand playbook for chatting with a peer,
  surfacing incoming messages, and connecting a peer via discover/pairing.
- **Hooks** (Node, cross-platform) —
  - `PostToolUse` progress-nudge: while a session executes a peer's task and goes
    quiet, reminds the model to send a progress update. Debounced + task-gated; tune
    with `INTERLINK_PROGRESS_INTERVAL` (seconds, default 60; `0` disables).
  - `Stop` inbox-listener: in the channel-less default, an async `asyncRewake` hook
    *is* the listener — it runs `interlink-mcp wait`, which blocks on the inbox and
    `exit 2`s to wake the idle agent when a peer message lands (no model-driven
    arming). A `flock` keeps it single-instance. Self-disables when
    `INTERLINK_CHANNELS=1`.

## One-time setup

The MCP config points at a standard config location; create your identity and
allowlist there:

```bash
mkdir -p ~/.config/interlink ~/.local/state/interlink
# interlink-keygen + interlink-bus come from the release archive or `cargo install
# --git https://github.com/wilfreddenton/interlink --locked` (npm ships only the agent).
interlink-keygen --out ~/.config/interlink/id.key    # prints your public key to share
printf '{}\n' > ~/.config/interlink/peers.json        # then add peers via pairing or add_peer
interlink-bus --db ~/.local/state/interlink/bus.redb  # the one bus — 127.0.0.1:9440
```

Set `INTERLINK_URL` in your environment if your bus isn't on `127.0.0.1:9440`.

To **receive** messages, just launch plain `claude` — the Stop hook + background
`interlink-mcp wait` deliver them, no flags needed (works even where channels are
blocked). If you have Claude Code channels and want the native push instead, launch
with the bundled launcher:

```bash
interlinked    # sets INTERLINK_CHANNELS=1 and adds --dangerously-load-development-channels
```

## Requirements

- **`interlink-mcp` on npm** (`npx -y interlink-mcp` resolves the pure-Rust
  binary). Or swap the `.mcp.json` `command` for a `cargo install`ed
  `interlink-mcp`.
- Cross-platform: the MCP server runs anywhere Claude Code does.
