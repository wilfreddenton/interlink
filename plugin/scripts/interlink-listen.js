#!/usr/bin/env node
// Stop hook: keep the channel-less inbox listener armed.
//
// In fallback mode (no Claude Code channels), a peer's message can only wake this
// agent when a *background task exits*. So the agent parks on a background
// `interlink-mcp wait`, which blocks until a message lands and then returns —
// surfacing it. That listener is one-shot: after it fires (or if it was never
// armed) the agent would park deaf. On every Stop this hook checks whether the
// listener is running and, if not, tells the model to re-arm it before parking.
//
// Self-disables when channels are on (INTERLINK_CHANNELS=1): there the MCP server
// pushes messages directly, so no background listener is needed.
//
// Node (guaranteed present via the npm package) so it runs cross-platform.

const channels = /^(1|true)$/i.test(process.env.INTERLINK_CHANNELS || "");
if (channels) process.exit(0); // channel mode: the server pushes; nothing to arm

// A tag we plant as an inert env-var prefix on the listener command, so we can
// recognize *our* background task in the registry rather than matching incidental
// text (which the shell may reformat).
const TAG = "INTERLINK_TAG=interlink-listen";
const waitCmd = process.env.INTERLINK_WAIT_CMD || "interlink-mcp wait";
const command = `${TAG} ${waitCmd}`;

let payload = "";
process.stdin.on("data", (c) => (payload += c));
process.stdin.on("end", () => {
  let tasks = [];
  try {
    tasks = JSON.parse(payload).background_tasks || [];
  } catch {}

  // Already armed? A running shell task carrying our tag means the listener is
  // live; allow the agent to park.
  const armed = tasks.some(
    (t) => t && t.type === "shell" && String(t.command || "").includes(TAG),
  );
  if (armed) process.exit(0);

  const ctx =
    "[interlink] Your inbox listener is not armed, so you would park deaf to " +
    "incoming peer messages. Re-arm it exactly once as a background task (Bash " +
    "with run_in_background: true):\n\n    " +
    command +
    "\n\nIt blocks until a message arrives, then prints it as an <interlink> block. " +
    "When it returns, handle the message (attribute it to the sender; act on a " +
    "peer's request) and then re-arm it again.";

  process.stdout.write(
    JSON.stringify({
      hookSpecificOutput: { hookEventName: "Stop", additionalContext: ctx },
    }),
  );
  process.exit(0);
});
