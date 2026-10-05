# Discovery and pairing

Discovery and human-gated pairing are implemented. Current messages use the
`interlink-v2` signing domain, shared with task-tracking messages.

## Trust boundary

Ordinary chat requires a locally authorized key. A non-peer can send a signed
pairing request, presented as metadata with an unverified claimed name. A signed
acceptance is handled only when the receiver has a matching outstanding request.
Names are hints; the key is the identity. Pairing is an operator decision.

## Discovery

Each bound session sends a signed announcement containing
`{ pubkey, name, session, ts, sig }`. `session` contains its ID, working directory,
git-root label, and summary. Sessions announce on startup (after binding for
Codex) and every 30 seconds.

The v1 announcement signature remains compatible with older releases. Retired
title extensions are ignored by the MCP client; discovery uses the signed
session ID, project, and summary instead.

The bus stores announcements by `pubkey#session_id`. `/roster` returns a flat
array with unsigned `age_ms` values; the MCP server verifies signatures, merges
entries from its relays, and groups them by identity for `discover`.

`discover` reports an error when no configured broker returns a valid roster.
If some brokers respond and others fail, it returns the available entries with
a warning identifying the failed brokers; those results may be incomplete.
Connection failures, HTTP errors, and malformed roster responses are distinct
from a successful empty roster. This behavior is shared by Claude Code and Codex.

A session is live below 90 seconds of silence, away until three days, and gone
when unregistered or expired. The roster is held in memory and capped at 4096
entries. Expired entries are pruned on announcement; when the cap is still full,
a new entry is refused. See [presence](PRESENCE.md).

`request_pair(target)` accepts a roster name, full public key, or its exact
8-character fingerprint. Ambiguous names or fingerprints require the full key.
It prefers a live session and can fall back to a retained away session. With no
session on the roster, the request fails rather than sending to a bare-key inbox.

## Handshake

1. A discovers B and the operator verifies the fingerprint.
2. A calls `request_pair(target)`. It saves the outstanding request and a signed
   `pair_request` before returning. The message contains A's claimed name and a
   return route to A's exact requesting session.
3. B verifies the message, saves it, and surfaces a pairing notice. Its operator
   inspects `list_pair_requests` and calls `accept_pair(fingerprint)` or
   `reject_pair(fingerprint)`.
4. Acceptance writes A to B's shared `peers.json` and queues a signed `pair_accept`
   to A's return session. Its signed `in_reply_to` identifies the request.
5. A matches the acceptance to its outstanding request and adds B to its policy.
   Both identities are now admitted across all sessions sharing those policy files.

The requester uses its `target` string as the proposed local petname. The
accepter uses the requester's claimed name. Names still cannot overwrite a
mapping to another key. `reject_pair` removes the saved inbound request without
changing trust; it does not send a rejection notification.

## Persistence and retries

Pairing state lives under
`$XDG_STATE_HOME/interlink/pairing/<identity>/<session>.json`, with
`~/.local/state` as the default state root. Reopening the same identity and
session resumes it. File locks and atomic replacement protect each update.

The control-message worker retries every two seconds after a send pass. It
attempts all configured relays and retires a job once at least one accepts it.
Transport acceptance is not a confirmation that the remote operator or model
has handled the request.

Repeating an outstanding request keeps its original ID, so an acceptance delayed
by an outage still matches. Before sending a saved control message, the worker
creates a fresh signature while preserving the message ID, content, and reply
correlation. An unsent confirmation therefore survives an outage longer than
24 hours. A message already on the broker can still expire before the recipient
returns; freshness is checked at receipt.

For compatibility, an acceptance without `in_reply_to` matches an outstanding
request by sender key. A correlated acceptance must match its request ID.
Unsolicited acceptances are ignored. Requests from an already admitted peer are
ignored; repeat requests are retries of an unfinished handshake, not a way to
change existing trust.

## Failure recovery

- A full confirmation queue is rejected before authorizing a peer.
- Policy and pairing are separate files. If authorization succeeds but saving
  the confirmation fails, `accept_pair` explicitly reports partial completion.
  Repair storage, inspect `list_pair_requests`, and retry `accept_pair` if the
  request is still pending. The policy update is idempotent.
- A local petname conflict while handling an acceptance produces a notice and
  retires the outstanding request, allowing later messages to proceed. The
  operator can finish local authorization with `add_peer` using the verified key
  and an available name. Repeating the knock alone cannot fix one-sided trust.
- If that key is already present under another petname, its existing name is
  retained and the handshake completes.
- Other storage failures while handling inbound pairing controls retain the bus
  message and retry; they do not silently acknowledge an uncommitted state change.

## Bounds and validation

Each session stores at most 64 inbound and 64 outbound requests, replacing by key
and evicting the oldest at capacity. The control outbox holds at most 128 jobs
and refuses additions when full. These bounds are not per-sender rate limits.
Message freshness and the process-local replay set still apply.

Process integration tests cover restart recovery, exact requesting-session
routing, repeated requests with delayed acceptance, expired unsent confirmations,
shared policy updates, and conflict handling. Unit tests cover queue capacity and
partial persistence failure. Public-relay hardening remains
[deferred](../DIRECTORY.md#public-relay-hardening).
