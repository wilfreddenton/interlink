#!/usr/bin/env node
// Postinstall: fetch the prebuilt interlink-mcp binary for this platform from the
// matching GitHub Release (tag `v<version>`) and drop it next to the launcher.
//
// Trust basis is the same as `cargo install --git`: HTTPS from a GitHub Release.
// (A future hardening: verify a SHA-256 published alongside the assets.)

const fs = require("fs");
const path = require("path");
const { version } = require("./package.json");

const REPO = "wilfreddenton/interlink";

// node platform+arch  ->  Rust target triple (must match release.yml asset names)
const TARGETS = {
  "linux x64": "x86_64-unknown-linux-musl",
  "linux arm64": "aarch64-unknown-linux-musl",
  "darwin arm64": "aarch64-apple-darwin",
  "win32 x64": "x86_64-pc-windows-msvc",
};

async function main() {
  // Ensure the launcher (the npm `bin` entry) is executable. `npm link` would set the
  // x-bit, but a raw tarball extraction — as npx's cache does — keeps the published
  // file mode, so belt-and-suspenders it here too (postinstall always runs). Harmless
  // on Windows, where the .js is run via node regardless.
  try {
    fs.chmodSync(path.join(__dirname, "bin", "interlink-mcp.js"), 0o755);
  } catch {}

  const key = `${process.platform} ${process.arch}`;
  const target = TARGETS[key];
  if (!target) {
    console.error(
      `interlink-mcp: no prebuilt binary for ${key}. ` +
        `Build from source instead: cargo install --git https://github.com/${REPO}`,
    );
    process.exit(1);
  }

  const ext = process.platform === "win32" ? ".exe" : "";
  const asset = `interlink-mcp-${target}${ext}`;
  const url = `https://github.com/${REPO}/releases/download/v${version}/${asset}`;
  const dest = path.join(__dirname, "bin", `interlink-mcp-bin${ext}`);

  console.error(`interlink-mcp: downloading ${asset} (v${version}) ...`);
  const res = await fetch(url); // Node >=18: fetch follows GitHub's redirects
  if (!res.ok) {
    console.error(`interlink-mcp: download failed (HTTP ${res.status}) from ${url}`);
    process.exit(1);
  }
  const buf = Buffer.from(await res.arrayBuffer());
  fs.mkdirSync(path.dirname(dest), { recursive: true });
  fs.writeFileSync(dest, buf, { mode: 0o755 });
  console.error(`interlink-mcp: installed ${dest}`);
}

main().catch((e) => {
  console.error("interlink-mcp: install error:", e);
  process.exit(1);
});
