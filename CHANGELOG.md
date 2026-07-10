# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
- `escapement-bus` — a loopback HTTP broker with one bounded FIFO per recipient
  key; buffers for offline agents, holds no keys, verifies nothing.
- `escapement-agent` — the per-agent Claude Code **channel** server. Long-polls
  the bus and, for each message, runs the inbound gate (verify signature →
  allowlist → addressed-to-me → fresh → dedupe) before pushing
  `notifications/claude/channel`. Tools: `send_message`, `fetch_request`.
- `escapement-keygen` — generate an Ed25519 identity; the public key is the id.
- **`identity`** — Ed25519, public-key-as-identity, domain-separated signing,
  `verify_strict`, freshness + replay protection.
- **`policy`** — `peers.json`: per-peer grant of `"*"` (inline) or a capability
  name (scoped to an agent's `tools:` frontmatter). Deny-by-default.
- **Scoped enforcement** — untrusted bodies are quarantined and reachable only
  via `fetch_request`, gated to subagents by
  [`contrib/pretooluse-guard.sh`](contrib/pretooluse-guard.sh); tool limits are
  the capability agent's frontmatter.
- Live integration harnesses in [`experiments/`](experiments) that drive a real
  Claude session through a PTY, plus the runtime facts they established.
- `contrib/stop-hook.sh` — the pre-channels fallback, for environments where
  channels can't run.

### Notable choices
- **No TLS**: loopback bus + signed messages; removes the only C dependency, so
  binaries are pure-Rust and statically linkable.
- CI fails the build if a C dependency (`ring`/`openssl-sys`/`cc`/`cmake`)
  reappears, and checks the whole feature powerset.
