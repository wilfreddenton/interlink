# Session presence & lifecycle (design)

> Status: **design of record — not yet implemented.** How interlink should track a
> session's liveness across close, sleep, and crash, so a peer routes to the right
> place and isn't silently black-holed. Companion to [`DELIVERY.md`](./DELIVERY.md)
> (which covers the last-hop *delivery* mechanics). Behavior tags: `[TESTED]`
> (measured this cycle), `[DOCS]` (documented), `[SPEC]` (MCP spec).

## The problem

interlink runs a **stateful, presence-aware** server (a roster of who's online, sticky
peer relationships) on top of MCP, which is **stateless by design**. That mismatch
surfaces at session end: when a session goes away, does a peer find out, and does mail
addressed to it still reach it?

Concretely, three things were unclear or wrong:

1. **Close notification.** When a session closes, peers are never told — the shutdown
   path only removes the bus roster entry; it messages no one.
2. **Sleep vs. kill are indistinguishable** at the moment they happen — both go silent.
3. **The 90s roster TTL fights sleep.** A laptop sleeps for far longer than 90s, so the
   TTL evicts a slept session's presence — and worse, if that identity has an awake
   sibling session, `route_session` **reroutes the slept session's mail to the sibling**
   (wrong recipient). This is exactly the single-user-multi-machine case.

## What we established (grounding)

- **MCP is stateless by design** `[SPEC]`. Shutdown is just a transport close — *"no
  specific protocol messages are required"*; in-flight requests are *"simply lost."*
  There is no shutdown notification and no peer concept. Presence/cleanup is entirely
  the application's job — this is a **named "fundamental challenge"** for stateful MCP
  servers, and the standard answers are exactly what interlink already does:
  cleanup-on-close + an inactivity TTL.
- **Graceful close cleans up reliably** `[TESTED]`. On `/exit`, Ctrl-D, tab-close, or a
  server restart, Claude Code closes the server's stdin (or SIGTERMs it); the server
  shuts *itself* down — runs `unregister_now` and exits — **before** Claude escalates to
  SIGKILL. Measured: roster entry removed in **~1–40 ms**, 3/3 clean across `claude -p`
  exits. So the **explicit unregister is a dependable "gone" signal.**
- **Only a hard death misses it** `[TESTED]`: SIGKILL / crash / power loss send no
  unregister → the session lingers until the TTL.
- **Sleep freezes, doesn't kill** `[TESTED/reasoned]`. On suspend the process is frozen
  with all state intact (same PID, same `session_id`, sockets); it sends no unregister.
  On resume it continues, re-announces under the **same** id (stable thanks to
  `CLAUDE_CODE_SESSION_ID`), and drains any messages queued for it during the sleep. So
  **sleep is recoverable** — the message queue (keep-until-ack) already survives it; the
  only casualty is *presence/routing*, via the TTL.

The through-line: **silence ≠ death. Unregister = death.**

## The model: live / away / gone

Replace the single 90s "delete" threshold with three states. The 90s becomes a
*freshness* threshold, not a *deletion* one; deletion is driven by the authoritative
unregister, with a long retention backstop for hard deaths.

| State | Condition | Meaning |
|---|---|---|
| **live** | heartbeat age `< LIVE_MS` (90s) | fresh, actively heartbeating |
| **away** | age `LIVE_MS … AWAY_RETAIN_MS`, no unregister | silent — **probably asleep**, maybe crashed |
| **gone** | explicit unregister, **or** age `≥ AWAY_RETAIN_MS` | removed from the roster |

### Constants

- `LIVE_MS = 90_000` (90s) — freshness threshold (unchanged value; heartbeat is 30s, so
  3 misses ⇒ "away").
- `AWAY_RETAIN_MS = 259_200_000` (**3 days**) — how long a silent, un-unregistered
  session is retained as **away** before hard-expiry. Covers a long-weekend suspend.
  A bus flag (the one real knob: larger ⇒ better sleep recovery, longer crash-zombie
  lingering).

## Bus changes (`bus.rs`)

1. **Retention.** `announce()`/`roster()` prune only entries with age `≥ AWAY_RETAIN_MS`
   (was `≥ LIVE_MS`). Slept sessions stay in the map instead of vanishing at 90s.
2. **Expose age (additively).** The roster already stores `(announcement, received_at)`
   but returns only the announcement. Add the bus-authoritative age as an unsigned
   `age_ms` field **on each announcement** — as `#[serde(default, skip_serializing_if =
   "Option::is_none")] age_ms: Option<u64>` on `Announcement`. **Confirmed safe**
   `[TESTED: code-read]`: `Announcement` has no `deny_unknown_fields`, so
   `verified_roster`'s `serde_json::from_value` drops it cleanly on old clients; and
   `Announcement::verify()` reconstructs the signed bytes from *named* fields via
   `announce_canonical(id, name, session, ts)`, so `age_ms` never touches the signature.
   This is the same pattern already used for `SignedMessage.reply_to` (an unsigned field
   "deliberately outside `canonical`"). Bus-side age also avoids clock-skew games.
   Backward-compatible; *not* a wrapper object.
3. **`unregister()` — unchanged.** Still an immediate removal. It is now the
   *authoritative* "gone."

## Client changes (`route_session`, `peer_sessions`, `discover`)

1. **`peer_sessions`** returns each session tagged **live** or **away** (today it returns
   only live sessions).
2. **`route_session`** — the sibling-misroute fix:
   - sticky is **live** → use it (unchanged).
   - sticky is **away** → **use it anyway** (it will wake and drain). *This is the key
     change; today it re-picks a sibling here.*
   - sticky is **gone** (absent from the roster) → re-pick among **live** siblings: one ⇒
     use it; several ⇒ error listing them (with states); none ⇒ refuse-or-queue with the
     honest "not on roster" note below.
   - The old `sessions.is_empty() → sleep-heal to sticky` branch largely disappears — a
     slept session now *stays* in the roster as **away** rather than emptying out.
3. **`discover`** shows state per session, e.g.
   `sessionB · ~/repo · away (last seen 45m ago)` — so the operator/model can tell
   "asleep, will deliver on wake" from "live now."

## Send-time feedback (three-tier)

`send_message` reports the target's state so the sender never assumes success blindly:

| Target state | result |
|---|---|
| **live** | `queued for desktop (a1b2…); delivering` |
| **away** (fresh) | `queued for desktop (a1b2…) — that session is away (asleep? last seen 20m ago); it'll deliver when it wakes` |
| **away** (stale, near the 3-day bound) | `queued for desktop (a1b2…) — away and last seen 2 days ago, so it may be gone, not asleep; run discover` |
| **gone** | `queued, but that session isn't on the roster (closed / long-dead) — may not deliver; run discover` |

The "away?" wording escalates toward "likely gone" as age approaches `AWAY_RETAIN_MS`.
And when `route_session` picks an **away** sticky session while a **live** sibling of the
same identity exists, the result names the live alternative
(`…pass session=<live-id> to reach the awake one instead`) — so preferring conversational
continuity never silently hides that a live option was available.

## Notification policy: away is pull, gone is push

- **away (sleep)** — a peer learns of it by **pulling**: the three-tier send feedback
  above, or `discover`. **No proactive ping.** Rationale: laptops sleep constantly, a
  slept peer will be back, and your queued message lands on wake — so it isn't an
  actionable event worth interrupting an idle peer for. (A napping peer ≠ a leaving one.)
- **gone (close)** — immediate via unregister, and the one place a proactive **push**
  makes sense: an optional **goodbye** message the closing session sends to its sticky
  peers so an idle peer is actively told "I closed" rather than discovering it on its
  next send. The graceful-close path is reliable enough (~1–40 ms) that this fires
  dependably. *Additive polish — two-tier presence fixes routing without it.*

## Edge-case resolution

| Scenario | Today | With this design |
|---|---|---|
| Sleep < 90s | live | live |
| **Sleep 90s–3d, awake sibling exists** | **misroutes to sibling** ❌ | stays **away**, routes to it, drains on wake ✅ |
| Sleep 90s–3d, no sibling | sleep-heal (queue) | **away**; same, and now visible in `discover` ✅ |
| Sleep > 3d | gone at 90s | gone at 3d; re-announces to live on wake |
| **Graceful close** | gone at 90s (silent) | **gone immediately** via unregister (+ optional goodbye) ✅ |
| Hard crash (no unregister) | gone at 90s | **away** up to 3d (visible zombie), then gone — minor |
| Bus's own host sleeps | bus sleeps too | on wake, wall-clock jumps so **every** entry looks stale/expired until each session re-announces (≤30s) — a transient, self-healing; same failure mode as today's TTL |

## Unchanged / out of scope

- **Message queue** (keep-until-ack, bounded) — untouched. Delivery to a slept session
  already worked; this only stops the *roster* from stealing the mail. Note the queue's
  bound is **independent** of `AWAY_RETAIN_MS`: a session away for days still has a capped
  inbox, so a high-volume flood evicts oldest regardless of the away window — "away for
  3 days" is not "nothing is lost for 3 days."
- **`message_status`** semantics are unchanged: it reports *sent to the bus*, not *read
  by the peer*. A message queued for an **away** session still shows "sent," not
  "delivered" — true delivery confirmation is the E2E-ack future work below, not
  something this design provides.
- Signing/trust gate, the 30s announce heartbeat, `unregister` semantics — all the same.
- **Distinguishing slept from hard-killed in the moment** — out of scope; it's a
  failure-detection impossibility (a silent death can't announce itself). We only
  separate the *achievable* line: **gone (announced) vs. silent (sleep or crash)**.

## Future / optional (not required for this design)

- **Goodbye-on-close push** to sticky peers (see policy above) — reliable on graceful
  close; additive. Bonus interaction: a received goodbye lets the peer **clear its sticky
  pointer** for that session immediately, so it stops preferring a now-gone session
  instead of waiting to notice it's `gone` on the next send.
- **End-to-end acknowledgment — considered and dropped.** A recipient-signed ACK
  (`message_status` showing "acknowledged by peer" vs. "sent to bus only") was weighed
  as the answer to "did my message land?", but it's largely redundant here: the
  three-tier send feedback already flags live/away/gone *at send time*, a reply is
  itself an ack for conversation, and delegated tasks carry richer `update`/`result`/
  `failed` status. It also can't distinguish slept from killed (both sit "unacked"
  until a timeout). Revisit only if one-way fire-and-forget delivery receipts become a
  real need.

## Rationale, in one line

**Silence ≠ death; unregister = death.** That single reframing fixes the
sibling-misroute for the multi-machine case, keeps sleeping laptops addressable for
3 days, and preserves fast, definite cleanup on real closes — all without touching the
message-delivery path.
