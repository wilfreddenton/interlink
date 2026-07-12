#!/usr/bin/env bash
# PreToolUse guard for the peer-management tools.
#
# `add_peer` / `remove_peer` change *who is trusted* — an operator action. A
# scoped (untrusted) peer's request is handled inside a subagent, so denying
# these tools whenever the call originates in a subagent stops an untrusted peer
# from escalating itself onto the allowlist. The main agent is unaffected and
# still gets Claude Code's normal permission prompt before the change is made.
#
# This mirrors pretooluse-guard.sh but with the opposite condition: that guard
# keeps fetch_request *in* subagents; this one keeps peer edits *out* of them.
#
# Empirically verified (see experiments/): `agent_type` is present on the stdin
# payload inside a subagent and absent for the main agent.
#
# Register with a matcher for both tools:
#
#   { "hooks": { "PreToolUse": [ {
#       "matcher": "mcp__praetor__add_peer|mcp__praetor__remove_peer",
#       "hooks": [ { "type": "command", "command": "/path/to/peer-admin-guard.sh" } ]
#   } ] } }
set -euo pipefail

payload="$(cat)"

# agent_type is present iff this call originates inside a subagent.
agent_type="$(printf '%s' "$payload" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin).get("agent_type",""))' 2>/dev/null || true)"

if [ -z "$agent_type" ]; then
  exit 0   # main agent: allow (Claude Code still prompts the operator)
fi

# Inside a subagent (a capability handler): deny. Trust is operator-only.
printf '%s\n' '{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"deny","permissionDecisionReason":"add_peer/remove_peer change the trust allowlist and are operator-only; a subagent handling an untrusted peer must not alter who is trusted."}}'
