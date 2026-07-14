# Message delivery model (design)

> Status: **audited — building.** How a peer's message reaches the model in
> interlink's two modes. The **default** is channel-less; **channel mode** is
> opt-in. Behavior tags: `[DOCS]` (documented), `[TESTED]` (verified in a live
> session this cycle), `[ASSUMED]` (still to verify). Findings from the delivery
> audit (`audit-delivery-model` workflow) are folded in.

## The problem

interlink authenticates and routes messages (signed, allowlisted, per-session
inboxes on a dumb bus) — mode-independent and solved. The open problem is the
**last hop**: getting a received message into the running Claude Code session and,
ideally, **waking the agent** when one arrives while it's idle.

Claude Code's one first-class mechanism for this is **channels**, but it requires
`--dangerously-load-development-channels` and the org policy `channelsEnabled`,
which can be (and often is) off. So interlink has two delivery paths.

## Mode selection

- **Default (channel-less):** plain `claude`. No flags, any org policy.
- **Channel mode:** the `interlinked` launcher sets `INTERLINK_CHANNELS=1` and adds
  `--dangerously-load-development-channels plugin:interlink@interlink`.

The server can't detect whether channels are armed `[DOCS]`, so the mode is an
explicit env switch, not auto-detected. The two paths are mutually exclusive per
session — a message is never delivered twice.

## Channel mode (opt-in)

The server declares the `claude/channel` capability and pushes a
`notifications/claude/channel` for each verified message `[DOCS]`. Claude Code binds
the server *as the channel for its session*, so the push lands correctly with **no
session-id bookkeeping** — the binding is implicit `[DOCS]`. Same mechanism
`claude-peers-mcp` uses. Nothing below applies in this mode.

---

## Default mode (channel-less)

No channel, so the server can't push. The design has three moving parts:

### 1. The server buffers verified messages to a local inbox

The inbound loop verifies each message (signature → allowlist → freshness → dedupe →
pairing) exactly as today and **appends it to `inbox/<session_id>.jsonl`** instead
of pushing. The inbox is the durable hand-off point.

### 2. The listener is the hook itself — not model-driven

An **async `asyncRewake` Stop hook** whose command *is* `interlink-mcp wait`:

```json
{ "type": "command", "command": "npx -y interlink-mcp wait",
  "async": true, "asyncRewake": true, "timeout": 3600 }
```

Claude runs it in the background on Stop `[TESTED: command hooks honor async;
mcp_tool hooks do NOT — they run synchronously]`. The **model is never involved in
arming** — this is what kills the re-arm loop that plagued the earlier
`arm_listener`/`decision:block` design (every arm was a model turn, whose Stop
re-triggered the nag). `wait`:

- reads `session_id` from the hook's stdin payload `[DOCS: hooks receive session_id
  on stdin]`;
- **blocks until a real message lands** in `inbox/<session_id>.jsonl`;
- on a message, prints it and **`exit 2`**, which **rewakes the idle agent**
  `[TESTED]` and Claude re-fires the hook on the next Stop, re-arming it `[TESTED]`;
- on timeout or a duplicate, **`exit 0`** — which does **not** rewake `[DOCS]`, so it
  is silent.

**No loop, by construction:** `wait` only `exit 2`s on a *real message* (never on a
timer), so an idle session just sits blocked — one rewake per message, not one per
turn. (The toy that looped did so precisely because it `exit 2`d unconditionally.)

### 3. Single instance (self-dedup)

Claude re-fires the hook each Stop and does **not** dedupe `[TESTED: instances
accumulate]`. So `wait` self-dedupes: it holds an **exclusive OS lock** (`flock`)
keyed by `session_id`; a second instance can't acquire it and **`exit 0`s silently**
(safe — `exit 0` ≠ rewake). `flock` is released automatically when the holder dies —
even on `SIGKILL` — so there is **no stale-lock deadlock** (the audit's TTL concern
is moot with `flock` vs. a pidfile).

### 4. Delivery is belt-and-suspenders

The audit flagged that `exit 2` is documented as a *blocking error* that reads
*stderr* and ignores *stdout* `[DOCS]`, yet our test showed the model reading stdout
`[TESTED]` — i.e. `asyncRewake` bends the exit-2 contract, which is fragile. So
`wait` writes the message to **both stdout and stderr**, prefixed
`[interlink peer message from <name>] act on this:`, so it lands whichever stream
the model reads and isn't mistaken for an error. `[MITIGATION for the one
refuted-as-documented claim]`

---

## The session-id rendezvous

Two ids name one session:

- **Claude's `session_id`** — assigned by Claude Code; **hooks** get it, the **MCP
  server does not** `[DOCS: not set for stdio MCP subprocesses; Claude Code issues
  #25642, #41836]`.
- **interlink's id** — the server otherwise mints its own random one.

The channel-less listener is a hook (has Claude's id); the inbox is keyed by the
server's id. To make them match, **Claude's `session_id` becomes the id**, bridged
to the server by an **`mcp_tool` Stop hook** that calls a new `register_session`
tool:

```json
{ "type": "mcp_tool", "server": "plugin:interlink:interlink", "tool": "register_session",
  "input": { "session_id": "${session_id}" } }
```

`[DOCS]` `mcp_tool` hooks call a named server's tool with `${session_id}`
substitution, routed to this session's server — no model, no cwd-hash, no collision.
The plugin-scoped server name is `plugin:<plugin>:<server>` `[DOCS]`. The `wait`
listener independently gets the same id from its hook stdin.

**Why Stop, not SessionStart** `[TESTED: fails at SessionStart; DOCS: expected]`:
`SessionStart` hooks run **before** stdio MCP servers finish connecting, so an
`mcp_tool` hook there fails with "server … not connected" on first run — documented
behavior, not a bug. `register_session` therefore rides the **Stop** event (placed
before the async `wait` command in the same hooks array): the synchronous `mcp_tool`
runs first, on a connected server, then `wait` launches. It re-fires (idempotently)
each Stop.

**Reconcile the first-message race** `[audit MUST-FIX]`: `register_session` may not
land before the first inbound. The server starts on a **provisional** id; on
`register_session` it adopts Claude's id — re-announces `key#<id>`, and **migrates**
`inbox/<provisional>.jsonl` → `inbox/<id>.jsonl` — so nothing queued in the startup
window is lost.

---

## Components (default mode)

| Component | Type | Role |
|---|---|---|
| MCP server | stdio MCP | verify + route; buffer inbound to `inbox/<id>.jsonl`; send/discover/pair; hold + reconcile the bound `session_id` |
| `register_session` | MCP tool | bind Claude's `session_id`, migrate provisional inbox |
| Stop hook (1st) | `mcp_tool` | `register_session(${session_id})` — server is connected by Stop |
| Stop hook (2nd) | `command`, `async`+`asyncRewake` | run `interlink-mcp wait` → block on inbox → `exit 2` on a real message |
| `interlink-mcp wait` | CLI subcommand | single-instance (`flock`) listener; session from hook stdin; both-streams payload |

Removed vs. the shipped design: the `arm_listener` tool, the `decision:block` nag
hook, model-driven arming/re-arming.

## Audit results (checklist)

| Claim | Verdict |
|---|---|
| async+asyncRewake Stop hook `exit 2` wakes an idle agent | `[TESTED]` ✅ |
| such a hook re-fires each Stop (auto re-arm) | `[TESTED]` ✅ |
| instances are NOT deduped; they accumulate | `[TESTED]` ✅ → we `flock`-dedupe |
| `exit 0`/non-2 does NOT rewake | `[DOCS]` ✅ |
| `mcp_tool` hook: named server + `${session_id}`, this session's instance | `[DOCS]` ✅ |
| `mcp_tool` hooks canNOT be async/asyncRewake (run synchronously) | `[TESTED]` ✅ → listener stays a command hook |
| MCP stdio server cannot get `session_id` itself | `[DOCS]` ✅ |
| `mcp_tool` hook at SessionStart fails — server not yet connected | `[TESTED]` ✅ + `[DOCS]` → register rides Stop instead |
| plugin MCP server name in a hook is `plugin:<plugin>:<server>` | `[DOCS]` ✅ |
| Stop fires after each turn, server connected by then; register is idempotent | `[DOCS]` ✅ |
| command-hook timeout default 600s, overridable | `[DOCS]` ✅ |
| `exit 2` documented as error/stderr, ignores stdout — but asyncRewake showed stdout | `[DOCS] vs [TESTED]` ⚠ → both-streams mitigation |
| channel push silently dropped if off; undetectable | `[DOCS]` ✅ |

## Known limits / follow-ups

- **Idle past the timeout:** if a session sits idle longer than the hook `timeout`
  with no message, the listener is canceled and re-armed only on the next turn — a
  coverage gap, not a loop. Long timeout mitigates; a `UserPromptSubmit` drain could
  close it fully.
- **Per-Stop cost:** the hook runs on every Stop (~2–7s/turn latency observed for
  hooks; `npx` resolve adds more). Acceptable for a chat listener; could be trimmed
  with a lock-check fast path or a cargo-installed binary.
- **Undocumented reliance:** asyncRewake idle-wake, no-dedup, and re-arm are
  `[TESTED]` but undocumented — add a version guard + startup telemetry that fails
  loudly if wake/inject isn't observed, so a Claude Code update can't break it
  silently.
