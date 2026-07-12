# Deferred work

Signed identity with pre-shared keys is **built** — each agent has an Ed25519
keypair and knows peers' public keys via `peers.json`. How it works (signing,
`verify_strict`, the inbound gate, capability scoping) is in
[`DESIGN.md`](DESIGN.md) and the code (`src/identity.rs`, `src/policy.rs`,
`src/agent.rs`).

This file records what was researched and deliberately left for later. None of
it is needed for a trusted mesh; the natural trigger for each is noted.

## Peer discovery / registry / presence

Today an agent must already know a peer's public key (in `peers.json`). A
registry would turn "know the key in advance" into "ask who's around, and who can
do X": a roster with TOFU-pinned keys, presence, and capability lookup. The
federation path (many dumb relays, each agent publishing its own relay list —
the Nostr "outbox" model) is the no-single-point-of-failure upgrade and is
already unblocked by multi-relay support in `praetor-mcp`.

## Opening the relay to strangers

A trusted mesh needs none of this (Tailscale is the boundary — see
[`DEPLOY.md`](docs/DEPLOY.md)). To let anyone join a *public* relay safely:

- **Signed `/recv`** — the bus challenges the poller to sign a nonce, proving it
  holds the private key for the queue it's draining (the Nostr NIP-42 pattern,
  native to our Ed25519 keys). Stops anyone who merely knows a public key from
  stealing messages.
- **Rate limits** — token bucket per verified pubkey, plus a coarse per-IP cap;
  optional proof-of-work on send.
- **End-to-end encryption** — X25519 (Ed25519 keys convert) → HKDF →
  ChaCha20-Poly1305 sealed box, so the relay is a blind forwarder and can be
  hosted on untrusted infrastructure.

## Reply / thread correlation

Today a reply is just another message; conversations are correlated **by peer**
(`conversation_history` groups on the petname, in time order). That's enough for
two-party, human-in-the-loop chat. It falls short once you **fan out to several
peers with concurrent async requests** and need to match a reply to the request
it answers — the natural trigger to build this.

The shape is the standard one (email `In-Reply-To`/`References`, Slack `thread_ts`,
Matrix `m.in_reply_to`, Nostr NIP-10 `root`/`reply` markers): carry the parent's
`msg_id` (and optionally a thread `root`). praetor already has a stable per-message
`msg_id` to point at, so it's additive — an optional `reply_to` on `SignedMessage`,
a `send_message` argument, and a threaded `conversation_history` render. The one
care point is that `reply_to` must enter the signed `canonical()` encoding, which
bumps the domain tag `praetor-v1\0` → `praetor-v2\0`.

## Semantic capability matching

Route by "who can do X" instead of an exact capability name: local embeddings via
`fastembed` + `bge-small-en-v1.5` (384-dim), brute-force cosine over an in-memory
`Vec`, per-capability max-similarity, with a lexical fallback when the model can't
be fetched. Fully local; nothing leaves the machine.

## Post-quantum signatures

If long-lived authenticity or a shifted threat model ever warrants it: a hybrid
**Ed25519 + ML-DSA-65** composite (both must verify) via `ed25519-dalek` +
`fips204`/RustCrypto `ml-dsa`. Additive, not a rewrite. Cost is ~4.5 KB per signed
message — the reason it's deferred. Falcon/FN-DSA is not an option (FIPS 206
unpublished, no production Rust crate). Note: harvest-now-decrypt-later is an
*encryption* threat, not a signature one, so PQ signing is not urgent.
