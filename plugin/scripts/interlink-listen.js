#!/usr/bin/env node
// Stop hook: keep the channel-less inbox listener armed.
//
// In fallback mode (no Claude Code channels), incoming peer messages are surfaced
// by a background task running `interlink-mcp wait`, which blocks until a message
// lands and then returns — and a background task *completing* re-invokes the main
// agent (the same mechanism that surfaces a finished subagent). That listener is
// one-shot, so on every Stop this hook checks whether it's still running and, if
// not, blocks the stop and tells the model to (re-)arm it before parking.
//
// Self-disables when channels are on (INTERLINK_CHANNELS=1): there the MCP server
// pushes messages directly, so no listener is needed.
//
// Node (guaranteed present via the npm package) so it runs cross-platform.

const channels = /^(1|true)$/i.test(process.env.INTERLINK_CHANNELS || "");
if (channels) process.exit(0); // channel mode: the server pushes; nothing to arm

let payload = "";
process.stdin.on("data", (c) => (payload += c));
process.stdin.on("end", () => {
  let data = {};
  try {
    data = JSON.parse(payload);
  } catch {}

  // Claude Code raises `stop_hook_active` as the consecutive-block cap approaches.
  // Always let the agent stop then, so this hook can never trap it.
  if (data.stop_hook_active) process.exit(0);

  // Is the listener already running? Match a live background task whose name looks
  // like our `wait` command. (The Stop payload carries id/name/status, not the full
  // command, so we match on the name.)
  const tasks = Array.isArray(data.background_tasks) ? data.background_tasks : [];
  const armed = tasks.some(
    (t) =>
      t &&
      t.status !== "completed" &&
      t.status !== "failed" &&
      /interlink/i.test(String(t.name || "")),
  );
  if (armed) process.exit(0); // listening — allow the stop

  // Not armed: block the stop and drive a re-arm. `decision: block` continues the
  // turn so the model can spawn the task now, rather than parking deaf. We do NOT
  // print a command here — the exact one is session-specific and uses the server's
  // own binary path (not on PATH under npx), so we point at the server instructions.
  const reason =
    "You are in interlink's channel-less mode and have no inbox listener running, so " +
    "you would park without hearing incoming peer messages. Before you stop: call the " +
    "interlink `arm_listener` tool, then run the exact command it returns as a Bash " +
    "background task (run_in_background: true). That command blocks until a peer message " +
    "arrives, prints an <interlink> block, and exits — which wakes you. When it returns, " +
    "handle the message and re-arm (call arm_listener and run it again).";

  process.stdout.write(JSON.stringify({ decision: "block", reason }));
  process.exit(0);
});
