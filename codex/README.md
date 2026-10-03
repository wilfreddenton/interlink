# Codex CLI

Interlink's Codex adapter uses the same bus, identity files, peer policy, signed
messages, discovery, and task tools as the Claude Code adapter. A Codex session
can talk to Claude Code or another Codex session. Sessions on the same identity
can use `to: "self"` without pairing.

## Setup

Use **Codex CLI 0.159.0 or newer**, with its default local shared daemon. The
adapter uses `codex queue`; `--no-daemon`, remote app servers, and the desktop app
are not supported by this adapter. Ephemeral (`codex exec --ephemeral`) threads
cannot accept queued submissions. Check `codex queue --help` before setup.

Install Interlink 0.9.0 or newer:

```bash
cargo install interlink-mcp --version 0.9.0 --locked
```

Alternatively, use a release archive or run `cargo install --path . --locked`
from a local checkout. Version 0.8.0 has no Codex adapter.

Set up an identity, `peers.json`, and a bus as described in the main README.
Reuse your existing Interlink identity if Claude Code is already configured.
The default files are `~/.config/interlink/id.key` and
`~/.config/interlink/peers.json`. On Windows, `USERPROFILE` is used when `HOME`
is absent. Explicit `INTERLINK_KEY` / `INTERLINK_PEERS` or CLI flags take priority.

Merge [config.toml](config.toml) into `~/.codex/config.toml` (or the config under
your configured `CODEX_HOME`), keeping any existing MCP servers and hooks. If `interlink-mcp` is not on Codex's PATH, use its absolute
path for `command`. Set `INTERLINK_CODEX_BIN` in the server's `env` table if the
Codex executable is not on that PATH. The queue subprocess must share the host's
Codex home and local daemon; do not point it at a different installation or configuration home.

Restart Codex, review and trust the binding hooks in `/hooks`, and send one
prompt. No hook-trust or sandbox bypass flags are needed. `SessionStart` can run
before MCP is ready, so prompt, Interlink tool, and completed-turn hooks provide
fallbacks.
The session becomes discoverable after its binding hook succeeds. If binding
has not run, sending returns a setup error instead of using a random thread.

Ask Codex to set a summary (for example, "Codex: working on the API") and run
`discover`. From a paired Claude or Codex session, send it a message using the
session ID shown there. It should appear as an attributed Interlink peer message.

## Delivery and trust

The local lifecycle hook supplies the owning thread UUID. An MCP instance binds
once; repeated calls with the same UUID are harmless, and a different UUID is
rejected. It does not announce or poll a mailbox until bound. A restart binds to
the same thread UUID, retaining the bus address.

After the common signature and allowlist checks, Interlink invokes
`codex queue --thread <uuid> --message <attributed-message>` using process
arguments, with no shell interpolation. Queue acceptance acknowledges the bus
message. Nonzero queue exits get up to three total automatic attempts. Permanent failures and
exhausted retries are saved locally before acknowledging the bus, so later
messages can proceed. A timeout has an uncertain outcome and goes directly to
saved failures; a manual retry can produce a duplicate. Acceptance is not
confirmation that the model has read or completed the request.

Each queue invocation times out after 30 seconds; retry backoff is two seconds.
Rendered queue messages are limited to 12 KiB to leave room for platform argument
quoting. Larger messages and messages containing a NUL are preserved for manual
recovery, never truncated. Retrying an oversized record will still fail; use `read` to
recover its content before discarding it.

Use `failed_deliveries(action="list")` for failure IDs and reasons, `read` with an
`id` to retrieve the attributed message, `retry` after repairing the CLI or daemon,
and `discard` after recovering it. Failures survive MCP restarts under the same
thread ID and state directory. They live under
`$XDG_STATE_HOME/interlink/failed/<identity>/<thread>.json`, defaulting to
`~/.local/state/interlink/failed/`. Storage is capped at 64 failures per session.
When full or unwritable, the current message stays unacknowledged on the bus until
space is available.

The queue input is visibly attributed to the peer. It does not grant operator
authority: pairing, peer-policy changes, and binding another Codex thread must
never be performed because a peer requested them. Codex's existing sandbox and
approval policy still apply.

The adapter targets root CLI sessions on the local daemon. Do not reuse its
binding hook for subagents: Codex's subagent hooks can carry the parent session
ID. The Claude progress-nudge hook is not installed for Codex; task updates still
use the shared `send_message` status fields and server instructions.

## Validation

`just test` includes process-level MCP tests for Codex binding, isolation,
Claude/Codex and same-identity Codex/Codex messaging, and retry after local queue
failure, bounded retries and saved-failure recovery. Shared-policy and pairing
regressions also cover sibling sessions, server restarts, a repeated request
with delayed acceptance, and a saved confirmation older than 24 hours. The queue
executable is replaced with a recording fixture in those tests, so they do not call a model
or change real Codex conversations. The installed Codex 0.159.0 initialize
handshake was separately checked: it does not supply a thread ID to MCP servers,
which is why binding is explicit. Codex also successfully parsed the sample MCP
hooks and called the binding tool through its own MCP client.

`just host-test` additionally exercises installed Codex and Claude runtimes against
local response fixtures. It requires Python 3.11+, both CLIs on PATH, and
localhost socket access; diagnostics go to `target/host-validation/`. On Codex
0.160.0, all four sample hooks were first listed as untrusted, then reviewed and
trusted by supplying their exact current hashes
as invocation-local config. Lifecycle hooks bound two ephemeral threads to
separate Interlink sessions. Those ephemeral threads validate binding only; they
are not used for queued delivery. Normal user configuration and saved hook trust
were unchanged. This validates trusted hook execution; it does not automate the
interactive `/hooks` review UI or call a production model.

A real `codex queue` smoke test against an isolated app server confirmed that a
queued message starts a turn on an idle saved thread. This used an unreachable
localhost provider, interrupted immediately on `turn/started`, and deleted the
test thread. It verifies the wake behavior without invoking a model; it is not a
full model-to-model conversation test.

Relevant Codex contracts: [MCP tool hooks and trust review](https://learn.chatgpt.com/docs/hooks)
and the installed CLI's `codex queue --help`. New or changed non-managed hooks
need review of their current definition in `/hooks`. Background hooks alone
cannot start an idle turn, so the Claude `asyncRewake` listener is not used for Codex.
