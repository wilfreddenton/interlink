# Identity & directory — design

> Status: **identity is BUILT** (`src/identity.rs`, `src/policy.rs`); this doc is
> now the record of the design and of the deferred work.
>
> Signed identity with pre-shared keys — each agent has an Ed25519 keypair and
> knows peers' public keys via `peers.json` — shipped. What remains deferred, and
> deliberately so, is **peer discovery** (a registry so you don't have to know the
> key in advance), **semantic capability matching**, and **post-quantum
> signatures**. Those are in [Future work](#future-work-deferred), researched and
> parked. The sections below describe the design as built and the reasoning.

## 1. Goals (v1)

1. **Stable identity** — an agent is an id bound to an Ed25519 public key.
2. **Authenticity** — a recipient can *prove* a message from "alice" was signed
   by alice's key, not spoofed by anyone who can `POST /send` with
   `from: "alice"`.
3. Keep it **small and fast** — signing must not bloat a chatty local bus.

Non-goals (v1): discovery, capability routing, presence directory, multi-host
federation, authorization policy.

## 2. Signature choice: Ed25519

- **Crate:** `ed25519-dalek` 2.2.0 — `#![forbid(unsafe_code)]`, pure Rust,
  production-grade, widely reviewed. Code against the RustCrypto `signature`
  traits (`Signer`/`Verifier`) so a different backend can drop in later.
- **Sizes:** 32-byte public key, 64-byte signature. A signed message adds ~88
  base64 chars — negligible next to the payload (contrast ML-DSA at ~4.4 KB).
- **Why not post-quantum now:** "harvest-now-decrypt-later" is an *encryption*
  threat; a signature has no secret that leaks retroactively (NIST IR 8547
  §3.1.2), so PQ signing is not urgent. Deferred behind the `signature` trait —
  see [Future work](#future-work-deferred).
- **Rejected lighter option:** HMAC/shared-secret would be even smaller but is
  symmetric — the verifier would hold the signer's secret, losing public-key
  identity ("who is who"). Ed25519 keeps verification keyless-of-secrets.

## 3. Keys & configuration

- **Own key:** each agent loads its Ed25519 secret key from a file
  (`ESC_KEY=/path/alice.key`). A `escapement-keygen` step generates the keypair,
  writes the secret, and prints the **public key (base64)** to share out-of-band.
- **Peer keys:** each agent loads a peers file (`ESC_PEERS=/path/peers.toml`)
  mapping known ids → base64 public keys:
  ```toml
  # peers.toml (alice's copy)
  bob = "3b6a27bcceb6a42d62a3a8d02a6f0d73653215771de243a63ac048a18b59da29"
  ```
- Rotation later: re-share a new public key (or sign new keys with the old). Out
  of scope for v1 beyond editing the peers file.

## 4. Signed message & where verification happens

The bus payload (opaque JSON, unchanged) carries an app-level signed message:

```
SignedMessage {
  from: String,          // sender id
  to:   String,          // recipient id
  text: String,          // (or arbitrary payload)
  ts:   u64,             // unix millis, part of the signed bytes (anti-replay-ish)
  alg:  "ed25519",
  sig:  base64,          // signature over a domain-separated canonical encoding
}
```

Signing covers a **domain-separated canonical representative**:
`"escapement-v1\0" || from || "\0" || to || "\0" || ts_le || "\0" || text`. The prefix
prevents cross-context signature reuse.

**Verification runs in the receive pipe, not inside Claude.** The background
listener becomes:

```
curl -sN --cacert <ca> "<url>/recv?me=alice" | escapement-verify --self alice --peers peers.toml
```

`escapement-verify` reads the broker line, extracts the `SignedMessage`, checks the
signature against `peers[from]`, and emits an annotated result
(`{"verified":true,"from":"bob","text":"…"}` or a clear `verified:false`
warning). So Claude only ever sees a message already stamped verified/unverified —
verification is deterministic and outside the model.

- **Sending:** `duet`'s `send_message` builds the `SignedMessage`, signs
  with `ESC_KEY`, and sends it as the bus payload.
- **Broker stays dumb.** It does not verify (no key knowledge) — peer-verify only.
  An optional broker spot-check is possible later but not v1.

## 5. Module & binary layout (v1)

All of this lands in the single `escapement` crate behind a new **`identity`**
feature (additive, like `hook`/`bus`/`mcp`), so nobody who just wants the hook
compiles `ed25519-dalek`.

| Item | New? | Role |
|---|---|---|
| `escapement::identity` | **new module** (`identity` feature) | Ed25519 keygen/load, sign/verify over the canonical representative, key (de)serialization, peers-file loader. No network I/O. |
| `escapement-keygen` | **new bin** (`required-features = ["identity"]`) | Generate a keypair, write the secret, print the public key. |
| `escapement-verify` | **new bin** (`required-features = ["identity"]`) | Stdin filter for the listener pipe: verify a bus line against the peers file, annotate. |
| `duet` | extend | `send_message` signs; add a `whoami` tool that prints this agent's id + public key. |
| `escapement::bus` | unchanged | Transport as-is. |

## 6. Phasing (v1)

1. **`escapement::identity`** — keys + sign/verify + unit tests (round-trip, tamper
   detection, wrong-key rejection, domain-separation).
2. **`escapement-keygen`** — generate/print keys; write the peers-file format.
3. **`duet` signs** — `send_message` produces `SignedMessage`; add `whoami`.
4. **`escapement-verify`** — the listener-pipe verifier; update the `config/` listener
   command and docs to pipe through it.
5. **End-to-end demo** — extend `scripts/demo.sh`: bob signs, alice's pipe shows
   `verified:true`; a tampered message shows `verified:false`.

Each phase leaves a working system; signing is additive to the existing bus.

---

## Future work (deferred)

Researched and intentionally out of v1. Captured so the path is clear.

### Discovery / registry / presence
A `escapement::registry` (roster + SQLite persistence + presence derived from the
existing `/armed` signal) with endpoints `POST /register` (TOFU-pin keys),
`GET /whois/:id`, `GET /agents?capability=…`. Turns "initiator must know the id"
into "ask who's around and who can do X." Presence = a live `/recv`.

### Semantic capability matching
Local embeddings via `fastembed` v5 + `bge-small-en-v1.5` (384-dim), brute-force
cosine over an in-memory `Vec` (no ANN index at this scale), per-capability
max-similarity, with a lexical tag/substring **fallback** when the model can't be
fetched. Fully local; nothing leaves the machine. (Anthropic has no embeddings
API; Voyage would be the hosted escape hatch — not needed.)

### Post-quantum signatures
If long-lived authenticity or a shifted threat model ever warrants it: upgrade to
a **hybrid Ed25519 + ML-DSA-65** composite (both must verify) via `ed25519-dalek`
+ `fips204`/RustCrypto `ml-dsa`. Because v1 signs behind the `signature` trait,
this is an additive second component, not a rewrite. Cost: ~4.5 KB per signed
message — the reason it's opt-in/deferred. Falcon/FN-DSA is not an option (FIPS
206 unpublished, no production Rust crate).
