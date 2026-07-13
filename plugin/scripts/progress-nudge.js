#!/usr/bin/env node
// PostToolUse progress-nudge hook (see docs/AUTO-PROGRESS.md).
//
// When this session is executing a peer's task and has gone quiet longer than the
// interval, inject a reminder so the model sends a progress update. The hook sets
// the *cadence*; the model writes the *content*. Debounced + task-gated so it's a
// floor, not a firehose. Written in Node (guaranteed present via the npm package)
// so it runs cross-platform, including the Windows desktop.

const fs = require("fs");
const path = require("path");
const os = require("os");

const interval = parseInt(process.env.INTERLINK_PROGRESS_INTERVAL || "60", 10);
if (!Number.isFinite(interval) || interval <= 0) process.exit(0); // 0 / invalid = disabled

const stateDir = path.join(
  process.env.XDG_STATE_HOME || path.join(os.homedir(), ".local", "state"),
  "interlink",
);
const inState = (name) => path.join(stateDir, name);

const readMs = (p) => {
  try {
    return parseInt(fs.readFileSync(p, "utf8").trim(), 10) || 0;
  } catch {
    return 0;
  }
};

// No active task → nothing to nudge (idle / non-collaboration sessions never fire).
let marker;
try {
  marker = JSON.parse(fs.readFileSync(inState("current-task.json"), "utf8"));
} catch {
  process.exit(0);
}
if (!marker || !marker.task_id) process.exit(0);

const now = Date.now();
const ms = interval * 1000;
const quiet = now - readMs(inState("last-update")) > ms;
const notNudgedRecently = now - readMs(inState("last-nudge")) > ms;
if (!(quiet && notNudgedRecently)) process.exit(0);

try {
  fs.writeFileSync(inState("last-nudge"), String(now));
} catch {}

const ctx =
  `[interlink] You're executing task '${marker.task_id}' for peer '${marker.peer}' and have not ` +
  `sent a progress update in over ${interval}s. Send a brief ` +
  `send_message(to: '${marker.peer}', status: 'update', task_id: '${marker.task_id}', ` +
  `text: "<one line on what you just did>") so they can follow along, then continue. ` +
  `If the task is actually finished, send status 'result' (or 'failed') instead.`;

process.stdout.write(
  JSON.stringify({
    hookSpecificOutput: { hookEventName: "PostToolUse", additionalContext: ctx },
  }),
);
