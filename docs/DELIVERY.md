# Message delivery model (design)

> Status: **validated live.** How a peer's message reaches the model in interlink's
> two modes. The **default** is channel-less; **channel mode** is opt-in. Behavior
> tags: `[DOCS]` (documented), `[TESTED]` (verified in a live session this cycle),
> `[ASSUMED]` (still to verify). An idle default-mode session was woken by a peer
> message and replied end-to-end; the session-id rendezvous now rides
> `CLAUDE_CODE_SESSION_ID` (v2.1.154+) rather than a `register_session` handshake.

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

`INTERLINK_CHANNELS` must be **declared in the plugin `.mcp.json` `env` block**
(`"INTERLINK_CHANNELS": "${INTERLINK_CHANNELS:-0}"`): Claude Code forwards only the
env vars a server declares, so without it the launcher's `INTERLINK_CHANNELS=1` never
reaches the MCP subprocess and the server silently runs in inbox mode while Claude
expects a push `[TESTED]`.

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

The inbox is keyed by `session_id`. Two parties must agree on it: the **server** (it
writes `inbox/<id>.jsonl`) and the **`wait` hook** (it drains that file). They agree
for free, because both read Claude's own session id from the same source of truth:

- **Server:** Claude Code injects **`CLAUDE_CODE_SESSION_ID`** into every stdio MCP
  subprocess's environment `[DOCS: Claude Code v2.1.154 — "Stdio MCP server
  subprocesses now receive CLAUDE_CODE_SESSION_ID and CLAUDECODE=1"; resolves issue
  #25642]`. The server reads it at startup and *is* that id. No handshake, no tool.
- **`wait` hook:** reads `session_id` from its Stop-hook stdin payload `[DOCS]` — the
  same value.

Priority for the server's id: an explicit `INTERLINK_SESSION` pin (manual/testing) →
`CLAUDE_CODE_SESSION_ID` (the normal case) → a random fallback (non-Claude usage).

**Stable across restarts** `[TESTED]`: Claude re-spawns the stdio server within a
session (on config changes / reconnects). Because the id comes from the environment,
**every instance shares it** — so a peer's `reply_to` always addresses the same
inbox. (The earlier random-per-launch id diverged across restarts and orphaned
replies; that whole class of bug is gone.)

This replaces the previous `register_session` + `mcp_tool`-hook + provisional-id +
`migrate_inbox` rendezvous entirely — the server has the real id from process start,
so there is no startup window to reconcile.

**Addressing a session** `[TESTED]`: because the id is now a full UUID (not the old 8
hex), a caller passing `session=<id>` to `send_message` is matched by
**exact-or-unique-prefix** against live sessions and expanded to the full id. Without
this, a model that copies a truncated id addresses a dead bus key and the message is
silently lost — the concrete regression the UUID switch introduced.

---

## Components (default mode)

| Component | Type | Role |
|---|---|---|
| MCP server | stdio MCP | reads `CLAUDE_CODE_SESSION_ID` as its id; verify + route; buffer inbound to `inbox/<id>.jsonl`; send/discover/pair |
| Stop hook | `command`, `async`+`asyncRewake` | run `interlink-mcp wait` → block on inbox → `exit 2` on a real message |
| `interlink-mcp wait` | CLI subcommand | single-instance (`flock`) listener; session from hook stdin; both-streams payload |

Removed vs. earlier iterations: the `register_session` tool, the `mcp_tool`
SessionStart/Stop bridge, the provisional id + `migrate_inbox` reconcile, the
`arm_listener` tool, and the `decision:block` nag hook — all obsoleted by
`CLAUDE_CODE_SESSION_ID` (rendezvous) and the hook-is-the-listener design (arming).

## Audit results (checklist)

| Claim | Verdict |
|---|---|
| async+asyncRewake Stop hook `exit 2` wakes an idle agent | `[TESTED]` ✅ |
| such a hook re-fires each Stop (auto re-arm) | `[TESTED]` ✅ |
| instances are NOT deduped; they accumulate | `[TESTED]` ✅ → we `flock`-dedupe |
| `exit 0`/non-2 does NOT rewake | `[DOCS]` ✅ |
| stdio MCP server receives `CLAUDE_CODE_SESSION_ID` in its env (v2.1.154+) | `[DOCS]` + `[TESTED]` ✅ → server reads it directly |
| that id equals the hook stdin `session_id` and is stable across restarts | `[TESTED]` ✅ (server + `wait` agree, no handshake) |
| `mcp_tool` hook at SessionStart fails — server not yet connected | `[TESTED]` ✅ + `[DOCS]` (why we abandoned the hook bridge) |
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
- **`CLAUDE_CODE_SESSION_ID` reliance:** documented for MCP servers (v2.1.154+), but
  the fallback to a random id keeps non-Claude/older-CLI usage working. `wait` still
  takes its id from hook stdin (documented), so the two never depend on the env var
  being present in the *hook's* process.
