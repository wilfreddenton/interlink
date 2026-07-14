#!/usr/bin/env node
// Launcher: exec the native interlink-mcp binary that install.js downloaded,
// wiring stdio straight through (the MCP protocol runs over stdin/stdout).

const { spawn } = require("child_process");
const path = require("path");

const ext = process.platform === "win32" ? ".exe" : "";
const bin = path.join(__dirname, `interlink-mcp-bin${ext}`);

const child = spawn(bin, process.argv.slice(2), { stdio: "inherit" });

child.on("error", (e) => {
  console.error(`interlink-mcp: could not launch the native binary: ${e.message}`);
  console.error("interlink-mcp: reinstall the package, or build with `cargo install --git https://github.com/wilfreddenton/interlink`.");
  process.exit(1);
});

// Forward termination so the MCP host can stop the server cleanly.
for (const sig of ["SIGINT", "SIGTERM"]) {
  process.on(sig, () => child.kill(sig));
}

child.on("exit", (code, signal) => {
  process.exit(code ?? (signal ? 1 : 0));
});
