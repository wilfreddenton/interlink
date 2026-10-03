# Session presence and lifecycle

Presence distinguishes a session that announced its closure from one that is
silent. A silent process might be asleep, disconnected, or crashed; a heartbeat
cannot determine which.

## States

| State | Condition | Routing meaning |
|---|---|---|
| Live | Last broker-received heartbeat is less than 90 seconds old | Recently reachable |
| Away | Heartbeat age is 90 seconds to less than three days | Retained address, possibly asleep |
| Gone | Explicit unregister or age of at least three days | No retained roster entry |

Agents announce every 30 seconds. `LIVE_MS = 90_000` is a client constant and
`AWAY_RETAIN_MS = 259_200_000` is a broker constant. Neither has a CLI override.
Send feedback becomes more cautious once an away session is at least one day old.

## Roster and trust

The broker stores signed announcements under `pubkey#session_id`, with the time
it received each one. `/roster` returns a flat array and adds unsigned `age_ms`.
Clients verify each announcement and group sessions by identity for `discover`.
The broker neither verifies announcements nor authenticates unregister requests.
A live session re-announces after an accidental removal.

The roster is in memory even with a durable broker queue. After a broker restart,
connected agents repopulate it on their next announcement, normally within
30 seconds. Expired entries are pruned on announcement; a full roster of 4096
retained entries refuses newcomers.

## Routing

A remembered reply session stays selected while it is either live or away. This
prevents a sleeping session's messages from moving to an awake sibling. If the
remembered session is gone, the sender chooses a single live sibling if available.
Ambiguous choices require an explicit `session` argument.

With no live sessions, one away session can be selected automatically. With no
roster entry, a remembered address can still be queued speculatively. An explicit
full ID can also be used when absent from the roster. Feedback distinguishes
these cases; a successful enqueue is not a guarantee of later delivery.

An away target's feedback includes its last-seen age and, when available, a live
sibling that could be selected explicitly. See [session routing](SESSIONS.md) for
the complete selection order.

## Shutdown and sleep

Graceful MCP shutdown stops and drains background workers before unregistering
from each relay. This prevents a late heartbeat from recreating the entry. After
unregistering, the server attempts a signed goodbye to its sticky peers, with a
three-second overall deadline for that best-effort step. A goodbye is an ordinary
message; receiving it does not automatically clear the sticky route.

Unregister and goodbye can fail when a relay is unreachable. A hard kill, crash,
or power loss may skip them entirely, leaving an away entry until expiration.
Sleep preserves the process and session ID; on waking, it reconnects and resumes
polling. Codex participates in presence only after its thread-binding hook succeeds.

## Delivery limits

Presence retention and message validity are separate:

- Broker queues are kept until acknowledgment or capacity eviction, and persist
  across broker restart only with `--db`.
- The default cap is 1024 messages per recipient, dropping the oldest when full.
- Signed messages more than 24 hours old are rejected at receipt. Three-day
  presence retention therefore does not promise three-day message delivery.
- Unregistering or expiring presence does not remove its broker queue. No
  abandoned-queue sweep is implemented.
- A `sent` message status means relay acceptance, not that the peer read it.

Tests cover roster retention, age reporting, unregister, routing classification,
and MCP shutdown cleanup. Host-independent process tests use real local brokers;
they do not simulate physical suspension or power loss. End-to-end receipts remain
[deferred](../DIRECTORY.md#delivery-and-storage).
