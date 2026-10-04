# rustqueue evaluation

Decision: **do not adopt rustqueue 0.3.0 for the current delivery changes.**
It supplies durable job retries, but does not meet the combined requirements of
solving Interlink's delivery problems and significantly reducing our code.

This is an isolated, unpublished crate. The application dependencies, production
code, installed agents, and broker were unchanged by this experiment. The measurements
below describe the working tree at evaluation time, before the subsequent mailbox fix.

## What was built

`src/lib.rs` is a 74-line candidate notification adapter. It uses immediate-durable
redb storage and one outstanding job per session. A host accepting a notification
leaves the job active. Receiver acknowledgement completes it. Explicit rejection
or a missing acknowledgement allows retry through rustqueue's housekeeping APIs.
No background worker automatically acknowledges a host handoff.

The prototype uses fixed retry backoff and a high, finite retry limit. It does not
implement the application mailbox, a host transport loop, a new MCP acknowledgement
tool, or a production migration. Those omissions are part of the fit assessment,
not capabilities supplied by the library.

`tests/behavior.rs` contains 11 behavior probes and one subprocess helper. Tests
named `limitation_*` deliberately assert the observed mismatch. A green test suite
means the evaluation is reproducible, not that the candidate satisfies all of
Interlink's requirements.

## Results

| Requirement | Observed behavior | Remaining Interlink work |
|---|---|---|
| Recover a lost host notification | Active job becomes available again after stall detection, including after reopening the database | Invoke housekeeping, connect host transports, and reconcile against unread mailbox state |
| Survive a process crash | Active job survives child-process exit without Rust destructors and is redelivered | Keep mailbox persistence and broker acknowledgement ordering |
| Do not consume a dropped fetch response | Job remains outstanding without an explicit acknowledgement | Implement fetch/ack protocol and per-message receipt state |
| Retry a rejected host handoff with backoff | Supported; delayed retry is not immediately pulled | Host status reporting and recovery policy |
| Consume directly through history | `ack` rejects a waiting job; a repeated acknowledgement of a retained completed job also errors | Idempotent consumption independent of notification lifecycle |
| Never resurrect acknowledged messages on replay | Reusing a unique key after completion creates a new job | Durable sender/message tombstones and retention policy |
| Fence stale notification attempts | An old attempt can acknowledge the current attempt because `ack` takes only the job ID | Generation checks coordinated atomically with mailbox state |
| Preserve new arrivals during acknowledgement | A new wake request is deduplicated while the previous job is active; acknowledging that job leaves no wake | Reconcile after consumption and on a fallback timer |
| Supersede older progress | Cancelling an active progress job errors | Sender/session/task coalescing and filtering against authoritative mailbox state |
| Eventually notify while unread work remains | The default retry budget moves the job to the dead-letter queue after three stalls | Reconciliation/recreation after exhaustion; increasing the retry count is not an unlimited guarantee |
| Share persistent state between overlapping MCP processes | A second independent open of the same database fails while the first is alive | One owner plus IPC, or a different ownership/opening strategy |

The lost-notification test runs the same simulated handoff for Claude channel,
Claude Stop, and Codex queue labels. These are shared state-machine tests, not real
CLI integration tests. The crash probe uses a real child process; other restart
tests close and reopen the database. Stall probes backdate only test fixture
timestamps and then call the actual recovery APIs.

A stale job acknowledgement is not intrinsically wrong for an immutable job.
It is insufficient as acknowledgement of an aggregate, changing mailbox. Likewise,
active-progress cancellation is a mismatch only when mapping messages directly to
jobs. Both cases can be handled by retaining our own mailbox state, which reduces
the amount of code this library could replace.

## Build and dependency cost

The published crate fails Interlink's no-C requirement even with
`default-features = false`:

- `rustqueue -> reqwest -> native-tls -> openssl-sys`
- `rustqueue -> metrics-exporter-prometheus -> hyper-rustls -> rustls -> aws-lc-sys`

The unmodified native build also failed on this machine because OpenSSL development
headers were unavailable. A vendored OpenSSL attempt required an unavailable Perl
module. No system packages were installed.

To complete the behavior evaluation, `prepare.py` copies the published crate into
`target/rustqueue-evaluation/upstream` and disables default features on its reqwest
and Prometheus exporter dependencies. These are the only two upstream manifest
edits. No upstream Rust source or queue semantics are changed. All passing runtime
results in this report are for that dependency-patched copy. The patch removes
HTTPS support from the crate's reqwest client, so it is not a general-purpose
upstream fix.

For the native `aarch64-unknown-linux-gnu` dependency graph, Cargo metadata resolves
170 packages for Interlink with all features, 264 for the isolated published
prototype, and 232 for the patched prototype. These counts include each root and
dev dependencies. They are separate graph sizes, not additive dependency increases
or binary-size measurements. The patched prototype has none of the packages
forbidden by Interlink's no-C check. Static builds and macOS builds were not tested.

rustqueue also uses redb 2 while Interlink uses redb 4. It does not expose an
application transaction spanning its jobs and our mailbox records. Keeping both
stores requires repairing the window between saving a message and scheduling its
notification. Moving mailbox state into a custom storage implementation would
reintroduce substantial code and storage migration work.

## Code reduction assessment

Measured physical lines, including blanks and comments, excluding unit tests:

| Code | Lines | Could this prototype replace it? |
|---|---:|---|
| Current `src/mailbox.rs` before its test module | 252 | No. Admission limits, persistence, history, replay suppression, coalescing, and message acknowledgement remain |
| Current `notification_loop` | 46 | Only its reservation/scheduling calls. Host dispatch, status persistence, and error handling remain |
| Candidate rustqueue adapter | 74 | Additional integration code, still missing the application behavior listed above |

The current reservation method is only 24 of the mailbox's lines. The adapter is
not a complete replacement for even that method's unread-message filtering and
notification-to-message association. The existing implementation still needs its
recovery bug fixed, so these numbers are not a comparison against a completed
correct baseline. No speculative line savings from an unwritten scheduler are
credited, and test/setup code is excluded from both sides.

Two mappings were assessed: messages as jobs and notification hints as jobs. The
former needs mailbox semantics outside the job lifecycle. The latter is the
implemented prototype and leaves nearly all mailbox logic intact. Completing jobs
at host handoff and scheduling periodic hints is another possible mapping, but it
still needs authoritative unread-state reconciliation and consumption tracking.
That would replace a small timer loop with a larger job framework.

There is no demonstrated significant code reduction, even after setting aside the
dependency patch. The recommendation is to keep the shared mailbox and implement
expiring notification reservations plus explicit, idempotent message consumption
there. Reconsider a job framework if Interlink later needs a broader background-job
system.

## Reproduce

From this directory, with Rust and `just` installed:

```sh
cargo fetch --locked
just upstream-tree
just upstream-no-c  # Expected failure for the published crate.
just test
just lint
```

`just test` and `just lint` prepare a disposable copy and build under the repository's
ignored `target` directory. They do not modify the Cargo cache, the production
manifest, or the production lockfile. After fetching, all evaluation commands run
offline. `Cargo.lock` pins the published dependency graph; the disposable copy has
its own adjusted lockfile for the local patch.

Validation on 2026-10-03: 12 Rust test functions passed, formatting passed, and
Clippy passed with warnings denied. The published no-C gate failed as expected.
Existing application tests and live host fixtures were not rerun because this
experiment changes no application source or dependencies.
