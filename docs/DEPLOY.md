# Deploying interlink

Run one bus somewhere all participating machines can reach. Each Claude Code or
Codex CLI session runs its own local `interlink-mcp`; the agent is started by its
host, not deployed as a shared daemon.

Install Interlink 0.10.0 or newer for Codex support and the persistence fixes:
`cargo install interlink-mcp --version 0.10.0 --locked`. The npm/plugin path is
documented separately in [the plugin guide](../plugin/README.md).

## Private-network setup

The broker and agent speak plain HTTP. Use loopback when all agents share a
machine. For different machines, use a trusted private network such as Tailscale,
and restrict port 9440 to the participating devices. The broker has no transport
authentication, so network reachability is its boundary for queue access.

On a Tailscale-connected broker host:

```bash
mkdir -p ~/.local/state/interlink
interlink-bus --addr "$(tailscale ip -4):9440" --db ~/.local/state/interlink/bus.redb
```

For a single machine, omit `--addr` to bind `127.0.0.1:9440`. The non-loopback
warning is expected for a private-network bind. Avoid binding all interfaces
unless firewall rules provide the intended restriction.

`--db` / `INTERLINK_DB` makes the broker queue persistent. Omitting it creates an
in-memory broker whose backlog is lost on restart. `--queue-cap` /
`INTERLINK_QUEUE_CAP` defaults to 1024 messages per recipient, with oldest-first
eviction. Create the database's parent directory before starting the broker.

Signatures authenticate messages but do not encrypt them. Private-network
transport can protect traffic in transit; the broker still sees plaintext. The
agent has no HTTPS support, so an HTTPS-only reverse proxy is not compatible
without changes to the transport.

## Configure the agents

Create a key and an initially empty `peers.json` on each participating identity,
following the [main setup guide](../README.md#install). Reuse the same key and
policy for sessions that should share one identity.

For Claude Code, install the plugin and set the relay URL before launching:

```bash
export INTERLINK_URL=http://busbox.your-tailnet.ts.net:9440
claude
```

The plugin supplies both the MCP registration and the Stop listener. Adding only
an MCP server with `claude --mcp-config` does not install the listener needed for
default inbound delivery. Use the [plugin guide](../plugin/README.md) for local
checkout testing and the optional `interlinked` native-channel launcher.

For Codex CLI, merge [codex/config.toml](../codex/config.toml) into its config,
including the lifecycle hooks, and add the remote URL to that server's env table:

```toml
[mcp_servers.interlink.env]
INTERLINK_URL = "http://busbox.your-tailnet.ts.net:9440"
```

Review and trust the binding hooks, then send a prompt. Follow the complete
[Codex setup guide](../codex/README.md); the Claude plugin does not configure Codex.

## Start the bus automatically

From the repository root on a systemd host:

```bash
mkdir -p ~/.config/systemd/user
cp contrib/interlink-bus.service ~/.config/systemd/user/
```

Edit `ExecStart` in the copied file for your binary path and private-network
address. Its default binds loopback and uses a persistent database. Then:

```bash
systemctl --user daemon-reload
systemctl --user enable --now interlink-bus
loginctl enable-linger "$USER"
```

The [service file](../contrib/interlink-bus.service) creates its state directory
and restarts the broker if it exits. Lingering allows the user service to start
without an interactive login. Host sessions themselves must still be launched or
resumed by the operator.

## Reconnection and recovery

Agent polling retries after connection failures. Suspending a process preserves
its in-memory state; a process restart does not. The ordinary agent outbox,
outbound log, gate replay set, and sticky routes are in memory, even if
`INTERLINK_AGENT_DB` is set. That old option is accepted but ignored.

Peer policy, pairing state, shared inbound mailboxes and consumption, Claude
notification inboxes, and Codex saved failures use separate local files. Reopening the same session and state directory recovers them. A new
host session does not automatically inherit another session's pending messages.
See [sessions](SESSIONS.md) and [delivery](DELIVERY.md) for storage paths and limits.

Durable broker queues do not imply unlimited delivery: overflow drops oldest
messages, freshness checks reject messages older than 24 hours, and host handoffs
lack end-to-end consumption acknowledgments. Broker presence is always in memory
and rebuilds as agents announce after restart.

## Multiple relays

`INTERLINK_URL` accepts a comma-separated list:

```text
http://busbox.your-tailnet.ts.net:9440,http://backup.your-tailnet.ts.net:9440
```

Agents announce to and poll every relay, and attempt each relay on send. A send
is considered accepted when at least one relay accepts; failures on other relays
are not retried after that. Receivers deduplicate by message ID within a bounded,
process-local replay set. Relays do not synchronize with one another.

## Public hosting

The current unauthenticated broker is not a public-relay service. Public access
requires authenticated queue operations, resource limits, and, if the relay
operator is untrusted, end-to-end encryption. Hosting behind TLS alone does not
supply these properties. See [deferred hardening](../DIRECTORY.md#public-relay-hardening).
