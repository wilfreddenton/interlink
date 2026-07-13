# Discovery & Pairing (design)

> Status: **implemented** (chat-only, v0.3.0). Bus roster + `discover`; the `kind`
> field (signing domain `interlink-v1`); the gate's knock branch; and
> `request_pair` / `list_pair_requests` / `accept_pair` / `reject_pair`. Pairing is
> mutual admission — the per-side capability grant it originally carried was
> removed with the capability model.

Today trust is configured out-of-band: you exchange public keys and hand-edit
`peers.json` (or `add_peer`). This design lets nodes **start with no peers**,
**find each other through the bus**, and **establish mutual trust with a
human-gated handshake** — without ever weakening the property that makes interlink
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
canonical encoding under the `interlink-v1` signing domain.

Flow, A pairing with B:

1. A `discover`s the roster, finds B's key by its `name (fingerprint)`.
2. A sends a `pair_request` to B carrying only **A's self-claimed name** (signed).
   No grant crosses the wire — pairing only admits.
3. B's gate sees a `pair_request` from a non-peer and, instead of dropping it,
   **holds it** (bounded, drop-oldest, deduped) and pushes a **metadata-only**
   notice: *"Pairing request from fingerprint `a1b2c3` claiming name 'A'. Review
   with `list_pair_requests`."* The claimed name is shown as an untrusted label.
   No attacker-controlled free text reaches the session.

## 3. Accept → mutual admission

Pairing establishes **mutual admission**: each side adds the other's key to its
own `peers.json`. There is no grant to choose — an admitted peer is a full chat
partner (interlink has no capability tier).

Tools: `discover`, `request_pair(target)`, `list_pair_requests`,
`accept_pair(fingerprint)`, `reject_pair(fingerprint)`.

- `request_pair(target)` — A knocks B, recording that it knocked (so an
  unsolicited accept from a key A never knocked is ignored).
- **Accept** — `accept_pair(fingerprint)` — B admits A into its `peers.json` and
  sends `pair_accept` back; A, seeing an accept for a knock it made, admits B in
  return. Both end up mutual chat peers.
- **Reject** drops the held request; nothing is written.
- `accept_pair` / `reject_pair` are **operator-only**: pairing changes who you
  trust, so a peer's message must never drive it.

## Threat model / bounding

- **No free text from strangers.** A knock carries identity + name only; the name
  is surfaced as an untrusted, escaped label.
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
2. Wire format: `kind` field + `interlink-v1` domain; `pair_request`/`pair_accept`.
3. Gate: the knock branch + bounded pending-knock store.
4. Tools: `request_pair` / `list_pair_requests` / `accept_pair` / `reject_pair`,
   + the operator guard.
5. End-to-end test on the two-machine tailnet: both boot with empty `peers.json`,
   discover, knock, accept, converse.

## Decisions

- **Pairing is mutual admission, not a grant.** Each side adds the other's key; an
  admitted peer is a full chat partner. (The old per-side capability grant was
  removed with the capability model — see the CHANGELOG.)
- **Targeting by name, fingerprint as tiebreak.** `request_pair` resolves a name
  through the roster for convenience, but requires the fingerprint when a name is
  ambiguous — and the key, not the name, is what gets pinned (TOFU).
