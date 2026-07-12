# Discovery & Pairing (design)

> Status: **approved design, not yet implemented.** Moves the "peer discovery /
> registry / presence" item from [`DIRECTORY.md`](../DIRECTORY.md) into scope.

Today trust is configured out-of-band: you exchange public keys and hand-edit
`peers.json` (or `add_peer`). This design lets nodes **start with no peers**,
**find each other through the bus**, and **establish mutual trust with a
human-gated handshake** — without ever weakening the property that makes praetor
worth using.

## The invariant this must not break

Deny-by-default: a message from a key not in your `peers.json` is dropped at the
gate, before the model sees it. Discovery necessarily lets an *unknown* key reach
you, so the hole is made as small as it can be:

> **A non-peer can only *knock*. It cannot message you.** The sole thing an
> unknown key may deliver is a bounded pairing request — identity + a self-claimed
> name, no free-text body. Real messages still require mutual trust, and accepting
> a knock is an explicit, operator-only action. Any non-knock from a non-peer is
> dropped exactly as today.

## 1. Registry = presence on the bus

The bus stays dumb — a bulletin board, not a trust authority.

- `POST /announce` — a node posts a **signed self-attestation**
  `{ pubkey, name, ts, sig }`. The bus stores it in a roster with a short TTL
  (~90s); nodes re-announce on a heartbeat, so the roster reflects *who is online
  now*. The bus does **not** verify — it stores and serves; **clients verify** the
  signature and discard anything that doesn't check out.
- `GET /roster` — the live (non-expired) announcements. Bounded (drop-oldest) so a
  flood can't grow it without limit.
- **Names are hints, not identity.** Not globally unique, not enforced by the bus.
  Discovery renders `name (fingerprint)`; the client flags collisions. Identity is
  the key — you verify and **pin the key on first pair (TOFU)**, never the name.

Federation falls out for free: with several relays, a node announces to and reads
the roster from all of them; the union is deduped by pubkey.

## 2. The knock (pairing request)

Messages gain a **`kind`** field: `message` (default, today's behavior),
`pair_request`, `pair_accept`. `kind` (and the knock's name) enter the signed
canonical encoding, so the signing domain bumps `praetor-v1` → `praetor-v2` (a
coordinated, pre-1.0 break).

Flow, A pairing with B:

1. A `discover`s the roster, finds B's key by its `name (fingerprint)`.
2. A sends a `pair_request` to B carrying only **A's self-claimed name** (signed).
   The grant A will assign B is A's *local* decision (below) and never crosses the
   wire.
3. B's gate sees a `pair_request` from a non-peer and, instead of dropping it,
   **holds it** (bounded, drop-oldest, deduped) and pushes a **metadata-only**
   notice: *"Pairing request from fingerprint `a1b2c3` claiming name 'A'. Review
   with `list_pair_requests`."* The claimed name is shown as an untrusted label.
   No attacker-controlled free text reaches the session.

## 3. Accept → mutual, each side grants from its own capabilities

A **capability is local**: `Grant::Scoped("run-tests")` means "handle this peer
with *my* `.claude/agents/run-tests.md` subagent," which runs on the *receiving*
side. So the grant each node assigns references *its own* capability files — the
two nodes need **not** share a capability vocabulary, and `"*"` (inline, no file)
is the only universal grant. Each side therefore sets, independently, the grant it
*hosts* for the other:

New tools: `discover`, `request_pair(target, grant)`, `list_pair_requests`,
`accept_pair(fingerprint, grant)`, `reject_pair(fingerprint)`.

- `request_pair(target, grant)` — A chooses the grant **A gives B** (from A's own
  capabilities or `"*"`); it's applied to A's `peers.json` when B accepts.
- **Accept** — `accept_pair(fingerprint, grant)` — B chooses the grant **B gives
  A** (from B's own capabilities or `"*"`), writes B→A into `peers.json`, and sends
  `pair_accept` back. A then writes B→A's counterpart (the grant A chose at step 2)
  into its own `peers.json`. Two independent grants; each side controls what it
  hosts, so `"*"`↔`"*"`, `"*"`↔`read-only`, or any mix all work.
- **Reject** drops the held request; nothing is written.
- `accept_pair` / `reject_pair` are **operator-only** — kept out of subagents by
  the same guard pattern as `add_peer`, and confirm-prompted in the main agent.
  Pairing changes who you trust; a peer's message must never drive it.

## Threat model / bounding

- **No free text from strangers.** A knock carries identity + name + requested
  capability only; the name is surfaced as an untrusted, escaped label.
- **Bounded knock queue** (drop-oldest) + dedupe by sender key, so a non-peer
  can't exhaust memory or spam you unboundedly. (Per-key rate limiting on
  `/announce` and knocks is the follow-on for a *public* relay — see
  [`DIRECTORY.md`](../DIRECTORY.md); on a tailnet the boundary is Tailscale.)
- **Freshness + replay.** Announcements and knocks carry `ts` and are subject to
  the existing skew window + dedupe.
- **TOFU key pinning.** You pin the key at pair time; a later announcement
  re-using a name with a different key is a *new* identity, shown as such — never
  silently conflated with the pinned peer.
- **The bus learns nothing it didn't already route.** The roster is public
  self-attestations with a TTL; the bus still holds no secrets and verifies
  nothing.

## Build order

1. Registry: `/announce` + `/roster` on the bus; heartbeat announce + `discover`
   tool in the agent. (See who's online.)
2. Wire format: `kind` field + `praetor-v2` domain; `pair_request`/`pair_accept`.
3. Gate: the knock branch + bounded pending-knock store (mirrors `Quarantine`).
4. Tools: `request_pair` / `list_pair_requests` / `accept_pair` / `reject_pair`,
   + the operator guard.
5. End-to-end test on the two-machine tailnet: both boot with empty `peers.json`,
   discover, knock, accept, converse.

## Decisions

- **Grants are per-side, not symmetric.** A capability is a local subagent file,
  so each node grants the other from its *own* capabilities (or `"*"`); the two
  need not share a vocabulary. `request_pair`/`accept_pair` each take the grant
  that side hosts.
- **Targeting by name, fingerprint as tiebreak.** `request_pair` resolves a name
  through the roster for convenience, but requires the fingerprint when a name is
  ambiguous — and the key, not the name, is what gets pinned (TOFU).
