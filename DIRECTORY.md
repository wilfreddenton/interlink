# Deferred work

The current project implements signed identity, human-gated pairing, discovery,
per-session routing, task correlation, status messages, and cooperative
cancellation. Codex CLI support and persistence improvements shipped in
[0.9.0](CHANGELOG.md#090---2026-10-03). Shared mailbox consumption, progress
supersession, and expiring notification retries ship in
[0.10.0](CHANGELOG.md#0100---2026-10-03).

## Public-relay hardening

A trusted private network is the current deployment boundary. Before exposing a
broker to untrusted clients, it needs:

- Authenticated receive and acknowledgment operations, so knowing a recipient's
  public key is not enough to drain its queue.
- Authorization for presence changes, including unregistering sessions.
- Rate and resource limits that bound aggregate queues and stored bytes, beyond
  the existing per-recipient message cap and roster cap.
- End-to-end encryption if relay operators must not see message contents.

These require protocol and deployment work, not just an HTTPS reverse proxy.
See [deployment](docs/DEPLOY.md) and the [trust model](DESIGN.md).

## Task orchestration

`task_id`, `status`, and signed `in_reply_to` already correlate messages. They
are conventions for cooperating agents, not a persistent scheduler. Potential
extensions are durable blocked-task state, task-owner tracking across multiple
hops, and delegation-depth or loop limits. See [task tracking](docs/TASKS.md).

`reply_to` already has a different purpose: it is an unsigned session-routing
hint. It must not be confused with signed `in_reply_to` message correlation.

### Graph supervision discussion (2026-11-01)

Existing operator workflows include a coordinator with two reviewers, or an
implementer, reviewer, and security specialist with separate tool access. Herdr
tabs make each agent's work inspectable. The immediate reliability gap is a
session stopping on model capacity or exhausted usage and waiting hours for a
human to notice and resume it.

Proposed direction, not an implemented scheduler:

- Keep identity, discovery, messages, and consumption in Interlink. Put durable
  assignments, dependencies, blocked reasons, retry counts, and next retry times
  in a supervisor outside model turns. A coordinator agent can hit the same
  capacity or usage limit as its workers.
- Use Herdr as the first launcher and visibility adapter. Give graph tasks stable
  IDs independent of tabs and agent sessions, and record the exact revision a
  review covered. A later skill could launch role profiles and exchange session
  IDs to connect the graph.
- Distinguish transient capacity failures, usage resets, approval requests,
  human-input requirements, completion, and intentional interruption. Idle is
  not completion, and a heartbeat is not evidence of progress.
- Prefer structured host signals. Use bounded retries, known reset times when
  available, and conservative checks otherwise. Prevent duplicate continuations
  and repeated external actions; account for workers sharing a usage allowance.
- Start with supervising one test agent and simulated failures, then validate
  capacity recovery, quota waits, supervisor restart, interruption, and approval
  handling before trying a multi-agent graph.

Isolation requirements: inspect existing sessions read-only. Use a separate
checkout, state, identity, private test broker, and explicitly identified test
tabs. Do not replace installed binaries, alter hooks or configuration, restart
the live broker, or control existing Herdr/Interlink sessions. Clean up only
resources created by the experiment. Obtain approval before real model trials
that consume the operator's shared usage. UX investigation below takes priority;
no supervision prototype is authorized by these notes alone.

## Messaging UX investigation (2026-11-01)

Scope: observations against main `99e71fd` (v0.12.1) and the operator's October 30
Claude screenshot. Record causes and evaluate options before changing delivery.
The current coordination works; presentation improvements must preserve it.

### Identity labels and harness attribution

- `src/agent.rs` assigns the display petname `self` to an incoming message from
  the receiver's own identity key when no explicit peer entry supplies a name.
  This means another session sharing the key, not the receiving agent talking
  to itself. `send_message(to="self", session="...")` is also a routing alias.
  Neither the alias nor the key identifies the sender's harness.
- `render_received` in `src/bin/interlink-mcp.rs` displays that petname and the
  message/task identifiers, but omits the originating session hint. Multiple
  sibling sessions therefore look alike even though routing distinguishes them.
  Candidate improvement: display a peer/node label plus session identifier and
  available session summary, while keeping routing aliases and receipt keys
  stable. Treat the unsigned session hint and summaries as descriptive metadata,
  not authenticated identity or authority. This does not require title syncing.
- The screenshot's outer “Another Claude session sent a message” label and
  accompanying same-harness paragraph come from Claude's host presentation.
  Those exact strings are present in the locally installed Claude 2.1.312 binary;
  they are not Interlink's notice template. Interlink's server instructions
  already describe both Claude Code and Codex CLI peers. Receiving through a
  Claude socket does not establish that the original sender used Claude.
  Investigate a supported generic attribution mechanism or upstream correction;
  do not patch the host binary or remove its permission boundary warning.

### Why instructions are visible and peer content is collapsed

`Notice::text` in `src/mailbox.rs` supplies the long fetch/acknowledgement
instructions. `src/bin/mcp/delivery.rs` wraps them as a message from Interlink.
The Claude socket adapter sends that wrapper as a user-message frame; Codex
passes it to `codex queue --thread ... --message ...`. The actual peer body is
returned later by `receive_messages`, as an MCP tool result. The screenshot
shows collapsed MCP calls followed by the receiver acting on the review result,
consistent with this split. The screenshot alone does not expose those calls'
arguments or results.

This indirection is intentional: a notice fetches the current unread mailbox,
so messages already consumed while the wake-up was queued are not replayed as
stale bodies. Replacing notices with queued full messages would lose that
property. The host, rather than the MCP server, controls tool-result collapsing.

Candidate direction: a concise, harness-neutral visible wake-up, with the
fetch/acknowledgement protocol in server instructions or trusted lifecycle-hook
context. Keep peer content clearly attributed as external agent content. Both
[Claude hooks](https://code.claude.com/docs/en/hooks#add-context-for-claude) and
[Codex hooks](https://learn.chatgpt.com/docs/hooks) support `additionalContext`.
Claude documents that this context is absent from the normal chat display.
Codex background-hook output alone does not start an idle turn, so context
injection is not a replacement for wake-up delivery. Validate installed host
versions, hook timing, compaction, and missed hooks before relying on this split.

The desired UI is an expandable, attributed peer-message item with protocol
instructions out of the normal conversation. Support for that presentation in
both hosts remains unverified. Claude's documented `MessageDisplay` hook only
transforms assistant text, not incoming prompts or tool results, so it does not
solve this case. Do not promise that a generic MCP annotation can control either
host's UI without a host compatibility test.

### Conditional hooks do not prevent agent-initiated empty fetches

`Mailbox::stop_check` counts unread, non-superseded, non-progress messages.
`Inner::check_inbox` returns an empty response when that count is zero. Codex's
Stop hook can request a fetch/acknowledgement continuation when work remains;
Claude additionally requires the prompt hook to have observed the current
notice. After one Stop continuation, remaining work produces a warning rather
than another continuation. A hook running at the end of each turn is therefore
not an instruction for the model to fetch an empty inbox every turn.

Follow-up transcript inspection confirmed the operator's report: Codex sessions
`01a1781f...` and `01a17f89...` repeatedly fetched with no new notice, including
empty responses without any outstanding-notice diagnostic. Calls used `{}` or
`{limit:100}` rather than a new notification ID. Some occurred during ordinary
work and automatic goal continuations. Thus conditional hooks do not establish
that agents only fetch when messages are unread. Agents can independently use
the tool to check for updates. Other inspected calls did handle stale notices,
so these are distinct sources of empty fetches.

One plausible contributor is the persistent server instruction in
`Interlink::get_info`: “Silence is allowed only after an empty fetch or
acknowledgement.” The plugin skill similarly says “End silently only after an
empty fetch or acknowledgement.” These sentences follow notice-handling advice,
but do not explicitly restrict themselves to that context. They can be read as
a standing requirement on every quiet turn. The transcripts confirm polling,
not that this wording is its sole cause; coordination tasks can also motivate
agents to check voluntarily. No equivalent repeated-empty pattern was established
in the sampled Claude transcript.

Candidate instruction change: explicitly scope fetch/ack requirements to a new
notice or unread-work recovery hook, and discourage routine polling after turns
or during waits. Preserve explicit operator-requested reads, purposeful manual
reads of quiet progress, full-batch draining, and recovery. Apply the wording
consistently to server instructions, the plugin skill, tool descriptions, and
notice text. Do not add a second model-facing polling tool merely to ask whether
the inbox is empty.

Validation for this investigation: all 19 mailbox unit tests passed, as did the
existing busy-host/history/restart integration test across adapters and the
Claude started-notice recovery integration test. These use isolated state and
local test transports, not production model calls. They cover notification
state transitions, not whether a model interprets standing instructions as a
reason to poll. This follow-up changed notes only; live sessions and installed
configuration were not changed.

Arrival-driven notices already wake the agent when attention is needed. Quiet
`status=update` progress does not wake it. A notice can still become stale while
waiting in a host queue if a manual fetch drains the mailbox first. One empty
fetch in that situation is expected; repeatedly generating accepted notices
without new work is not. Preserve the distinction between mailbox state, notice
handoff, prompt observation, message consumption, and task completion.

Before any UX implementation, validate clearer labels and shorter notices in
isolated Claude and Codex fixtures, then explicitly approved interactive trials.
Cover idle and busy recipients, manual consumption before notice delivery,
multiple senders, quiet progress, duplicate/stale notices, compaction, restart,
missing tools, fetch/ack failures, and the bounded Stop fallback. Do not remove
the fallback merely to hide its instructions or acknowledge bodies before the
agent has received them. See [delivery semantics](docs/DELIVERY.md).

### Host presentation findings (2026-11-01)

An isolated probe ran installed Claude Code 2.1.312 and Codex CLI 0.165.0 with
temporary configuration, a fixture MCP server, and localhost model-response
servers. No production model requests, live Interlink broker, or Herdr sessions
were involved. Each host received an ordinary prompt followed by a short notice.
The fixture returned MCP server instructions at initialization and additional
protocol context only when its prompt hook matched the notice.

| Check | Claude | Codex |
|---|---|---|
| MCP server instructions reach the model | Confirmed in captured request | Confirmed in captured request |
| Notice-specific hook context reaches the model | Confirmed via actual local socket delivery | Confirmed via isolated app-server turn submission |
| Ordinary initial prompt omits notice-specific context | Confirmed | Confirmed |
| Context appears as a normal visible message | Absent from captured stream output | Absent from `userMessage` items; present in `hook/completed` diagnostics |
| Host adds same-harness attribution | Socket message gets the Claude wrapper | Not applicable to the tested text submission |

These checks establish transport and context placement, not model compliance or
interactive terminal rendering. The probe used deterministic replies, did not
exercise real `codex queue` delivery, and did not test compaction or resume.
An initial probe failed because its test provider lacked an import; the corrected
run passed both hosts, and the probe processes were closed. The existing
delivery integration tests remain the evidence for fetch/ack state transitions.

The smallest implementation should first reuse existing MCP server instructions
for the shared protocol and shorten the visible notice. A short notice can retain
its ID and a one-line fetch cue. No additional hook is needed just to install
static instructions. If per-notice reinforcement proves necessary, the existing
binding hook can return `additionalContext` for a verified current notice,
without introducing a new executable or polling loop. Context is not a secret:
diagnostics and saved transcripts can expose it even when normal chat does not.
Keep peer message bodies in attributed tool results, not elevated hook context.

Claude's installed socket-wrapper function chooses same-harness wording even for
host-injected peer messages. No supported sender/harness-label override was found
for the current socket path. Do not impersonate an internal teammate or activity
event to alter that classification. The documented alternative is
[Claude channels](https://code.claude.com/docs/en/channels-reference): events use
a generic channel source and optional sender metadata, but require channel
enablement and allowlisting. The development override is interactive-only and
does not bypass organization policy. Changing transports solely for wording
would introduce deployment requirements; prefer an upstream attribution fix.
Meanwhile, Interlink can identify itself as the bridge and explicitly state that
its peers may use different harnesses, without claiming to remove Claude's text.

No supported cross-host mechanism was established for replacing an incoming
notice or MCP result with a custom expandable peer-message card. The
[MCP tool protocol](https://modelcontextprotocol.io/specification/2025-11-25/server/tools)
leaves presentation to clients. Claude's `MessageDisplay` hook affects assistant
text only. Codex exposes tool-result items to clients through its
[app-server API](https://learn.chatgpt.com/docs/app-server), but that does not grant
an MCP server control over the stock CLI's rendering. Keep readable attribution
in the existing tool result; a custom card remains a host feature request or a
separate client UI project, not part of this small Interlink change.

Recommended implementation scope: clearer sender/session labels, concise notices,
and consistent instructions that discourage routine empty polling. Preserve
notification IDs, explicit message receipts, coalescing, bounded Stop recovery,
and the ability to handle long notices already queued before an upgrade. Test
old and new notice formats, missing hooks, and context loss before release. Do not
make automatic consumption depend on UI display, and do not replace lost host
functionality with model-driven inbox polling.

## Delivery and storage

- End-to-end receipts could distinguish relay acceptance from recipient handling
  if one-way delivery confirmation becomes necessary.
- Inbox compaction and cleanup of abandoned session files and broker queues are
  not implemented. Retention needs to preserve unread messages and cursor safety.
- The ordinary agent outbox and outbound log are still in memory. Persisting
  them would need isolation and recovery rules for concurrent sessions.
- A queued message can expire while waiting on the broker. Renewing an unsent
  pairing control message does not extend the validity of copies already sent.

## Host compatibility

The current Codex adapter supports root CLI sessions on the local shared daemon.
Desktop, remote app-server, ephemeral-thread delivery, and subagent support are
separate work. The local host fixtures validate lifecycle behavior without a
production model; interactive UI and production-model acceptance tests remain
outside those fixtures. See [Codex validation](codex/README.md#validation) and
[Claude validation](docs/DELIVERY.md#validation).
