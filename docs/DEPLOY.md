# Deploying interlink

Only the **bus** needs deploying. Each `interlink-mcp` runs locally next to
its Claude Code session (Claude Code spawns it as an MCP server), so "deploy"
means "put a bus somewhere every agent can reach."

## Recommended: your own machines over Tailscale

For a trusted mesh — machines whose keys you hold, using `"*"` peers — this is
the right deployment. Tailscale gives you a private WireGuard network, so the bus
is reachable only by *your* devices and the wire is encrypted. That preserves the
same trust model interlink was designed around (only trusted peers can reach the
bus), with **no code changes and no public exposure** — which is exactly why you
don't need the public-relay hardening (signed-recv, E2E) for this setup.

Note interlink speaks **plain HTTP** on purpose (no TLS in the binary — that's
what keeps it pure-Rust/static). Over Tailscale that's fine: WireGuard already
encrypts everything. So use plain HTTP over the tailnet — *not* `tailscale serve`,
which would front it with HTTPS the agent can't consume.

### 1. Put every machine on one tailnet

Install Tailscale on each machine (the bus host and every agent host) and
`tailscale up`. Enable **MagicDNS** so machines get names like
`busbox.your-tailnet.ts.net`.

### 2. Run the bus on one always-on machine

Bind it to the **Tailscale interface only** so it's never exposed on the public
internet or your LAN — the bus has no auth of its own, so its reachability *is*
its security boundary:

```bash
interlink-bus --addr "$(tailscale ip -4):9440"
```

(Or `--addr 0.0.0.0:9440` if the host has no public inbound — e.g. a laptop or a
home server behind NAT. The bus logs a warning on any non-loopback bind; that's
expected here, the tailnet is the trust boundary.)

### 3. Point each agent at it

In each agent's `.mcp.json`, set `INTERLINK_URL` to the bus's MagicDNS name:

```json
{ "mcpServers": { "interlink": {
  "command": "/path/to/interlink-mcp",
  "env": {
    "INTERLINK_KEY":      "/path/to/alice.key",
    "INTERLINK_PEERS":    "/path/to/alice-peers.json",
    "INTERLINK_URL":      "http://busbox.your-tailnet.ts.net:9440"
  }
} } }
```

The agent's own store — its outbound queue and the conversation log queried by
`message_status`, `conversation_history`, and `list_pending` — is **always
in-memory**. Every Claude session spawns its own `interlink-mcp`, so a shared
on-disk store would be single-writer contention; in-memory keeps each session
isolated (and survives sleep, since suspend freezes the process with RAM intact).
The **bus** is the durable layer. (`INTERLINK_AGENT_DB` is accepted but ignored.)

Then launch each session with plain `claude`:

```bash
claude --mcp-config alice.mcp.json
```

Delivery is channel-less by default — the plugin's `wait` Stop-hook listener wakes the
session on incoming messages, no channel flag needed. Native Claude Code channels are
an opt-in enhancement; the supported way to use them is the plugin install + the
`interlinked` launcher (see the README), not a hand-wired channel flag.

That's the whole deployment. Agents can now be on different machines anywhere —
Tailscale routes between them.

## Sleep, reboot, and reconnection

If the bus runs on a laptop that sleeps or shuts down, nothing needs babysitting:

- **`interlink-mcp` reconnects on its own.** Each agent's long-poll retries forever
  with backoff; a vanished bus is not an error it treats as fatal, so it never
  crashes and it resumes the moment the bus is reachable again. It dials a fresh
  connection each poll (no keep-alive), so a socket that went stale across a
  sleep/wake is never reused. Verified end to end: kill the bus mid-poll, restart
  it, and the next message is delivered — no restart of the agent needed.
- **The bus doesn't crash when an agent disconnects.** A dropped long-poll is
  just a dropped request; the bus holds no per-connection state.
- **Auto-start the bus on boot** with the included user service:
  [`contrib/interlink-bus.service`](contrib/interlink-bus.service) (`systemctl --user
  enable --now interlink-bus`, plus `loginctl enable-linger` to start before login).
  `Restart=always` also brings it back if it ever dies. On *sleep/wake* the
  process is only frozen and thaws by itself — systemd isn't involved.

- **The bus queue is durable** (give it `--db` / `INTERLINK_DB`): it holds a message
  until the recipient acks it, so a bus restart loses nothing queued for an offline
  agent. Delivery is at-least-once; the receiver dedupes by `msg_id`, so a
  redelivered message is harmless. The **agent** side is in-memory — it survives
  sleep (frozen RAM) but not a hard restart, so a message queued *while the bus was
  unreachable* is the only loss window, and even that survives sleep. The bus is the
  durable layer by design; the agent stays in-memory so concurrent sessions on one
  machine don't collide on a single store.

One honest caveat:

- **Claude Code sessions aren't daemons.** After a full shutdown you relaunch your
  sessions yourself; when you do, `interlink-mcp` reconnects to the bus
  automatically. Only the bus auto-starts.

## Federation later — just add a URL

`INTERLINK_URL` is a **comma-separated list**. To remove the single-point-of-failure,
run a second bus on another machine and list both on every agent:

```
INTERLINK_URL=http://busbox.your-tailnet.ts.net:9440,http://backup.your-tailnet.ts.net:9440
```

The agent then **polls and sends to both**, and its dedupe (by `msg_id`)
collapses the duplicate so Claude sees each message once. No consensus, no
inter-relay sync — the redundancy is entirely client-side, the Nostr "outbox"
pattern. This is verified end to end; see the two-relay test.

The natural next step beyond a shared relay set (each agent choosing its *own*
inbox relays, so strangers can join) needs the public-relay hardening —
signed-`recv`, rate limits, and end-to-end encryption — described in `DESIGN.md`
and the deployment discussion. Not required for a trusted mesh.

## Public hosting (if you outgrow the mesh)

If some agents live on machines you don't control, you need a public bus — and
first the hardening above, so an open relay is safe. Then a blind, authenticated
relay can run anywhere untrusted:

- **Oracle Cloud "Always Free" ARM VM** — genuinely free and always-on; run the
  binary, put Caddy in front for HTTPS. Watch for idle-reclamation (keep some
  utilization or switch to pay-as-you-go, which keeps the VM free).
- **Google Cloud Run** — free at this traffic; accept a cold start after idle.
- **Your own box + Cloudflare Tunnel** — free HTTPS, no inbound ports.
- **Fly.io / Railway** — ~$2–5/mo if you want no-caveats managed always-on.

The 25-second long-poll is the constraint that rules out most serverless free
tiers and short-timeout proxies (including Tailscale Funnel's HTTP mode); pick a
platform without a sub-30s request timeout, or move the transport to SSE/WebSocket
first.
