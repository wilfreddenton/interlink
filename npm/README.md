# interlink-mcp npm wrapper

This package downloads the native Rust MCP server from the matching GitHub
release. It contains no JavaScript messaging implementation. Its launcher
forwards arguments and connects the native process to stdin and stdout.

```json
{
  "mcpServers": {
    "interlink": {
      "command": "npx",
      "args": ["-y", "interlink-mcp"],
      "env": {
        "INTERLINK_KEY": "/absolute/path/to/id.key",
        "INTERLINK_PEERS": "/absolute/path/to/peers.json",
        "INTERLINK_URL": "http://127.0.0.1:9440"
      }
    }
  }
}
```

This registers an MCP server only. For Claude's default incoming-message wake,
install the [Claude plugin](../plugin/README.md), which also installs the Stop
listener. For Codex, follow the [Codex adapter guide](../codex/README.md). Codex
support starts at Interlink 0.9.0; use 0.10.0 or newer for shared inbox
consumption and lost-notification recovery. Version 0.11.0 removes session titles and their synchronization.
When upgrading from 0.10.1 or 0.10.2, update the Claude plugin together with the
binary and follow the [upgrade notes](../docs/SESSIONS.md#upgrading-from-session-titles).

Node 18 or newer is required. `postinstall` fetches the release asset for Linux
x64/arm64, macOS arm64, or Windows x64. Other platforms need a source build.
The npm package supplies only `interlink-mcp`; the full release archive or
`cargo install` also supplies `interlink-bus`, `interlink-keygen`, and `interlinked`.

`INTERLINK_AGENT_DB` is obsolete and ignored. The ordinary MCP outbox and outbound log are
in memory; separate files persist the shared inbound mailbox and consumption,
pairing, Claude notification inbox, and failed-delivery recovery.

## Releasing

The source of truth is [release.yml](../.github/workflows/release.yml). A pushed
`v*` tag triggers native builds, release assets, then automatic crates.io and npm
publication using configured OIDC trusted publishers.

1. Update `Cargo.toml`, the root package entry in `Cargo.lock`, `npm/package.json`,
   and `plugin/.claude-plugin/plugin.json` to the same release version. Move the
   Unreleased changelog entries into the release section and update setup notes
   that currently require a source checkout.
2. Run `just ci`; ensure the GitHub feature and platform checks pass. Run
   `just host-test` when changing host integration.
3. Tag the reviewed release commit as `v<version>` and push that tag.
4. Check all release workflow jobs. The raw assets are named
   `interlink-mcp-<target>` (plus `.exe` on Windows); full archives contain all four
   binaries. The npm installer downloads the raw asset for its package version.

Normal releases do not need a separate manual `npm publish`. Both registries must
have trusted publishing configured for this repository and `release.yml`; a
successful binary build alone does not establish publication success.
