# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.6.0]

### Added — works without Claude Code channels
- **Channel-less delivery is now the default**, so interlink works with **plain
  `claude`** and needs no `--dangerously-load-development-channels` and no
  org `channelsEnabled` — which channels require and can't be detected from a server.
  The MCP server writes each **verified** message (same trust gate) to a local inbox
  queue; a new **`interlink-mcp wait`** subcommand blocks until a message lands,
  prints it as an `<interlink sender="…">` block, and exits. A **`Stop` hook** keeps
  that background `wait` task armed so a channel-less agent is still woken by incoming
  messages (a background task *completing* is the only non-channel wake signal).
- **`interlinked` launcher** (new binary) opts into native channels: it sets
  `INTERLINK_CHANNELS=1` and starts `claude --dangerously-load-development-channels
  plugin:interlink@interlink`, forwarding extra args. Use it when you have channels
  and want the native push; otherwise just run `claude`.
- **`INTERLINK_CHANNELS=1`** selects channel mode (push, Stop hook self-disables);
  default/unset selects fallback mode (inbox + `wait`). **Both** mint a **random
  per-session id**, so concurrent sessions on one machine never share an inbox — each
  fallback session drains its own `inbox/<id>.jsonl`, and the server prints that
  session's exact `interlink-mcp wait --session <id>` command in its instructions.
  `INTERLINK_SESSION` can pin a stable name instead (opt-in). The two delivery paths
  are mutually exclusive per session, so a message is never delivered twice.

### Notes
- The fallback relies on a background task **completing** re-invoking the main agent
  (the same wake that surfaces a finished background subagent). This is **not
  documented** but is confirmed working in current Claude Code; verify it in your
  build. The Stop hook uses `decision: "block"` (with a `stop_hook_active` guard so it
  can never trap the agent) and detects the listener by task `name`.
- Concurrent fallback sessions on one machine each get their own inbox — no config
  needed. Channel mode keeps native push and multi-session addressing as before.

## [0.5.2]

### Changed
- **Sessions register on startup, not on first use.** A session now announces its
  node + session to the roster the moment it boots (and unregisters on close), so it
  is discoverable immediately. This replaces "register-on-use" (announce only on the
  first `send_message` / `set_summary`), which made a fresh session invisible to
  itself and created a standoff where two fresh sessions never saw each other. Node
  registration is idempotent — every session under one identity announces the same
  `pubkey`, the bus groups by it, and a re-announce is an upsert, so N sessions never
  produce a duplicate node. Trade-off: every plugin-loaded session now appears on the
  roster. `set_summary` now just *labels* an already-registered session.

## [0.5.1]

### Added — intra-node sessions
- **Two sessions on the same machine can chat**, addressed with
  `send_message(to:"self", session:"<id>")`. They share one identity, so a message
  from your own key is trusted **implicitly** — no pairing, no self-entry in
  `peers.json` (only the holder of your secret key can produce such a signature, so
  this grants nothing to anyone else). `discover` shows your own identity `[you]`
  with all its live sessions, marking the calling one `← this session`.
- **`discover` takes an optional `peer`** (petname, name, fingerprint, or key) to
  list just that identity's live sessions instead of the whole roster.

### Fixed
- **Pairing and task-cancel were undeliverable after 0.5.0.** When the inbox became
  `key#session_id`, only `send_message` was updated — `request_pair`, `accept_pair`,
  and `cancel_task` still targeted the bare-key inbox, which no session polls, so
  knocks/accepts/cancels silently went nowhere. They now route to a live session
  (`key#session_id`) like everything else.
- **A session can no longer address itself.** Its own session is excluded from
  `discover`-based auto-routing, and an explicit self-target is refused; a defensive
  inbound guard drops any self-to-self loopback as a backstop.

### Internal
- Deduplicated the roster fetch/verify (one `verified_roster` + `NodeGroup`
  grouping behind `discover` / `resolve_target` / `peer_sessions`) and the outbound
  send path (a shared `queue_outbound`).

## [0.5.0]

### Added — many live sessions per node, individually addressable
- Every Claude Code session is now a **first-class, addressable endpoint**. Each
  `interlink-mcp` mints a random `session_id` at startup and polls **its own inbox**
  `key#session_id`, so there is no shared mailbox and no fan-out — a message,
  including a pairing knock, lands on exactly one live session. Design:
  [`docs/SESSIONS.md`](docs/SESSIONS.md).
- **`discover` now groups by identity → live sessions**, each shown as
  `session_id · cwd · git repo · summary`, so a human recognizes a session without
  ever seeing the id. `send_message` gains a **`session`** arg; with exactly one
  live session it **auto-routes**, otherwise it returns the list to pick from.
- **`set_summary`** describes what a session is doing and registers it. Sessions are
  **register-on-use** — a session announces only on its first `send_message` or a
  `set_summary`, so idle/plain chats stay invisible to peers.
- **Reply-stickiness:** every message carries an unsigned `reply_to = key#session_id`
  hint, so a reply returns to the exact session that sent it and a conversation pins
  to one desk. It **self-heals** across sleep (same id, drains its queue on wake) and
  re-picks after a hard restart (new id).
- **Graceful unregister** on clean session close (stdin EOF) or `SIGTERM` drops the
  session's presence immediately, so a peer re-picks rather than waiting out the TTL.

### Changed
- **Breaking (roster shape):** the presence announcement now carries a **signed**
  session descriptor (`session_id`, `cwd`, `git_root`, `summary`), and the bus keys
  the roster by `pubkey#session_id`. Nodes must be ≥0.5.0 to see each other in
  `discover`; already-paired peers still chat across the bump (message signing is
  unchanged). The agent-side `INTERLINK_LABEL` / `send_message` `channel` mechanism
  is superseded by sessions and removed.

## [0.4.2]

### Fixed
- **Second session no longer boots with no tools.** interlink installs as a
  user-scope plugin, so every Claude Code session spawns its own `interlink-mcp`.
  They all pointed at one on-disk `agent.redb`, and redb is single-writer — so the
  second session (or an orphaned server) failed to open it and started with **zero
  tools**. The agent store is now **always in-memory**: each session gets an isolated
  outbox + log, so there's no collision and no file to orphan, and it survives sleep
  (suspend freezes the process with RAM intact). The **bus stays the durable layer**
  — a message that reached it is still keep-until-ack durable for an offline
  recipient. `INTERLINK_AGENT_DB` is still accepted for compatibility but ignored
  (logged once at startup); the plugin and config templates no longer set it.
  Wire-compatible with 0.4.x (no protocol change).

## [0.4.1]

### Added
- **Auto-progress nudge** — a `PostToolUse` hook that, when a session is executing
  a peer's task and has gone quiet longer than `INTERLINK_PROGRESS_INTERVAL`
  seconds (default **60**), reminds the model to send a `status=update`. The hook
  sets the *cadence*; the model writes the *content*. Debounced and task-gated: any
  outgoing update resets a shared timer, so a well-behaved agent's own milestone
  updates keep the hook silent, and idle / non-collaboration sessions never fire.
  The MCP server writes a small current-task marker + heartbeat under the XDG state
  dir that the hook reads; wire-compatible with 0.4.0 (no protocol change). The
  hook is Node (cross-platform, incl. the Windows desktop). Config:
  `INTERLINK_PROGRESS_INTERVAL` (`0` disables). Design:
  [`docs/AUTO-PROGRESS.md`](docs/AUTO-PROGRESS.md).

## [0.4.0]

### Added — task tracking (correlation, status, cancellation)
- Delegated work is now **tracked**. Data messages carry three optional, **signed**
  fields — `task_id` (a correlator the requester mints), `status`
  (`update` / `needs_input` / `result` / `failed` / `canceled`), and `in_reply_to`
  — so progress, questions, and results stay correlated even with several tasks in
  flight between the same two peers. `send_message` gains `task_id` / `status` /
  `in_reply_to`; a new `cancel_task(to, task_id)` is the interrupt for a peer
  running autonomously. The inbound push surfaces `task_id` + `status`, so the
  receiver branches deterministically (a `needs_input` → surface to the human; a
  terminal status → close the loop). Design: [`docs/TASKS.md`](docs/TASKS.md).
- The `needs_input` status makes "questions route back to the requester, not the
  local operator" fall out of the data model — it is A2A's `input-required`. The
  SKILL and server instructions now drive the fields, and state the anti-laundering
  rule: a peer relaying "my operator approved" is never your operator's consent.

### Changed
- **Breaking:** the three task fields enter the signed canonical encoding, so the
  signing domain bumps `interlink-v1` → `interlink-v2`; all nodes must be ≥0.4.0.

### Deferred (see [`docs/TASKS.md`](docs/TASKS.md))
- Federation guards — a delegation-depth cap + loop/ping-pong detection, durable
  "blocked-awaiting-answer" state, and an explicit "who is the human for this task"
  binding — until running 3+ nodes.

## [0.3.0]

Renamed from **praetor** to **interlink**, and reduced to a focused, chat-only
trust model. Breaking: the signing domain, crate/binary names, env vars, and
`peers.json` format all changed, so every node must be ≥0.3.0.

### Changed
- **Renamed `praetor` → `interlink`** throughout: crate `interlink-mcp` (library
  `interlink`), binaries `interlink-mcp` / `interlink-bus` / `interlink-keygen`,
  env vars `INTERLINK_*`, plugin/marketplace/npm names.
- **Signing domain reset to `interlink-v1`** (was `praetor-v2`). Incompatible with
  older nodes by construction.
- **`peers.json` is now `{ "<petname>": { "key": "…" } }`** — a plain admission
  allowlist. A legacy `"may"` field is accepted and ignored, so 0.2.x files load.

### Removed
- **Capability-scoped delegation** — the whole quarantine + capped-subagent model
  (`fetch_request`, `Grant`/`Scoped`, the `read-only`/`dev` capability agents, and
  the two `PreToolUse` guard hooks). Rationale: safe *bidirectional* collaboration
  requires mutual trust (you cannot sandbox the replies you must consume), so
  interlink authenticates trust cryptographically and treats an admitted peer as a
  full chat partner instead. See [`DESIGN.md`](DESIGN.md).

### Kept
- **`identity`** — Ed25519, public-key-as-identity, domain-separated signing,
  `verify_strict`, freshness + replay protection.
- **`policy`** — deny-by-default `peers.json` admission allowlist.
- **`interlink-bus`** — a loopback HTTP broker with one bounded, durable
  keep-until-ack FIFO per recipient key; buffers for offline agents, holds no
  keys, verifies nothing.
- **`interlink-mcp`** — the per-agent Claude Code **channel** server. Long-polls
  the bus and, for each message, runs the inbound gate (verify signature →
  allowlist → addressed-to-me → fresh → dedupe) before pushing
  `notifications/claude/channel`. Tools: `send_message`, `message_status`,
  `conversation_history`, `list_pending`, `discover`, live peer management
  (`add_peer` / `list_peers` / `remove_peer`), and pairing (`request_pair` /
  `list_pair_requests` / `accept_pair` / `reject_pair`).
- **Discovery & pairing** — a bus roster (`/announce` + `/roster`, TTL presence)
  plus a human-gated knock→accept handshake, so nodes boot with an empty
  `peers.json` and connect at runtime. A non-peer may only *knock* (identity +
  self-claimed name, no free text). Design: [`docs/DISCOVERY.md`](docs/DISCOVERY.md).
- **Named inboxes (labels)** — a session launches with `INTERLINK_LABEL=<name>`
  and receives only messages addressed to it via `send_message`'s `channel`;
  routing is `key#label`, an unsigned hint, so the trust gate is untouched.
- **`persist`** — a durable, keep-until-acked FIFO over
  [redb](https://crates.io/crates/redb) (pure Rust, ACID; no C), on both the bus
  (`INTERLINK_DB`) and each agent's outbox (`INTERLINK_AGENT_DB`), so a restart of
  either loses nothing. At-least-once, made safe by `msg_id` dedupe. The agent's
  store also keeps a local conversation log for the status/history tools.

### Distribution
- **Claude Code plugin** ([`plugin/`](plugin)) — bundles the MCP server and the
  `interlink` skill; installable via a marketplace
  (`/plugin marketplace add wilfreddenton/interlink`).
- **npm wrapper** ([`npm/`](npm)) — `npx interlink-mcp` fetches the platform's
  prebuilt static binary.
- **Release workflow** — a tag builds and publishes static binaries for Linux
  (x86_64/aarch64 musl), macOS (aarch64), and Windows (x86_64).

### Notable choices
- **No TLS**: loopback/tailnet bus + signed messages; removes the only C
  dependency, so binaries are pure-Rust and statically linkable. Signed ≠
  confidential, so federate only through a relay you trust.
- CI fails the build if a C dependency (`ring`/`openssl-sys`/`cc`/`cmake`)
  reappears, and checks the whole feature powerset.
