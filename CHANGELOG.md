# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
- `duet-bus`: async per-recipient long-poll message queue over HTTPS
  (`/send`, `/recv`, `/armed`), served with `tokio-rustls` + `hyper-util`.
- `duet-liveness`: Claude Code `Stop` hook that keeps a background listener
  armed across turns.
- `duet-mcp`: helpers (`BusClient`, crypto install, CA-trusting client) for
  MCP-server-to-HTTP-backend bridges.
- `duet-chat`: MCP server exposing `send_message` / `poll_messages`.
- Drop-in `.mcp.json` and `Stop`-hook settings for two instances.
