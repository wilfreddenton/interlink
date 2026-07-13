# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

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
