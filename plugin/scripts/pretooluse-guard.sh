#!/usr/bin/env bash
# PreToolUse guard for the scoped-peer path.
#
# It enforces exactly one rule: the main agent may not call
# `mcp__praetor__fetch_request`. Only a subagent may — because that is how an
# untrusted peer's request body is kept out of the long-lived main context. The
# per-capability tool limits are enforced separately, and more strongly, by each
# capability agent's own `tools:` frontmatter (a subagent cannot call a tool it
# was never given).
#
# Empirically verified (see experiments/): PreToolUse fires for MCP tools and
# can deny them; `agent_type` is present on the stdin payload inside a subagent
# and absent for the main agent.
#
# Register with a matcher for the fetch_request tool:
#
#   { "hooks": { "PreToolUse": [ {
#       "matcher": "mcp__praetor__fetch_request",
#       "hooks": [ { "type": "command", "command": "/path/to/pretooluse-guard.sh" } ]
#   } ] } }
set -euo pipefail

payload="$(cat)"

# agent_type is present iff this call originates inside a subagent.
agent_type="$(printf '%s' "$payload" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin).get("agent_type",""))' 2>/dev/null || true)"

if [ -n "$agent_type" ]; then
  exit 0   # inside a subagent (a capability handler): allow the fetch
fi

# Main agent: deny. The body must never enter the main context.
printf '%s\n' '{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"deny","permissionDecisionReason":"fetch_request is only for a capability subagent; spawn the subagent named in the request metadata and let it fetch the body."}}'
