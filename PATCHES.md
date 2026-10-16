# Boundless SOCKS patch

Based on upstream `v0.1.20` (`b23a13e2b7ee73e15ba008cd9b19dcd2d3861957`).
Retain upstream history and license; keep application behavior outside this fork.

URI IPv6 brackets are normalized before connection setup using the shared
upstream helper from #302/#306. The original fork-specific classification branch
has been removed. Literals retain their IP wire address types and domains retain
proxy-side DNS behavior. This patch adds no DNS lookup, retry or route fallback.

```sh
cargo test --features full --lib uri_hosts_preserve_socks_address_types
```

The regression checks exact SOCKS handshake bytes for IPv6, IPv4 and domains,
including both DNS modes for literals. Boundless additionally tests fixed local
resolver answers, proxy-side DNS and username/password authentication.

Boundless consumes a full commit SHA. Before updating that pin, compare with the
upstream release and run both fork and downstream regressions. Remove the fork
override when an official release provides this behavior and those checks pass.

## Selected upstream fixes (2026-10-02)

Backport #302/#306 for centralized URI IPv6 normalization; retire the original
classification implementation and retain its exact-wire regression. Backport
#308 for non-consuming fragmented reply parsing and correct domain ports, with
all IPv4/IPv6/domain split positions and coalesced responses covered. Parse
retained responses before another socket read. Backport #292 to cancel the idle
pool timer when the pool empties. Preserve the local CONNECT parser that retains every early tunnel byte; upstream #315
does not yet return buffered early bytes. No retry or route fallback is added.

Validation: `cargo test --features full --lib` using the matching Boundless hyper
override: 82 passed, one existing ignore on macOS arm64.

## Executor-owned client diagnostics (issue #63, batch 5)

The client executor handoff now carries `Result<(), Box<dyn Error + Send +
Sync>>`. Real connection and losing background dial errors reach it with
their sources intact. Pool reuse refusal and local cancellation are ordinary
maintenance ends mapped to Ok; the public generic Executor trait and
TokioExecutor are unchanged.
A connection's establishment error has one shared slot: readiness takes it
for the request, or a dropped/successful waiter releases it to the background
task. The task ends Ok after delivery, so it cannot duplicate the request's
error. Dropping a waiter after publication cannot discard the only error.

Using the matching Boundless hyper override,
`cargo test --features full --test task_results` passes four regressions
(exit 0), and `cargo test --features full --lib cancelled_waiter_leaves`
passes the handoff regression (exit 0) on macOS arm64. These exercise actual
connections, readiness after a successful response, a losing dial, delivery
without duplication and the cancellation window. Baseline comparison and
the affected suites also ran: the original result-erasing implementation
fails the connection/readiness/losing-dial regressions (exit 101); the patch
passes (exit 0). All 83 library checks (one existing ignore) and all 21 legacy
client integration checks pass (exit 0). Strict Clippy remains blocked by
exactly the same diagnostic messages as the original source (exit 101), with
no suppressions or unrelated repairs.


## Ordinary pool retirement and ALPN disposal (issue #63 regression)

Post-response readiness decides only whether a sender can return to the
pool. It completes cleanly when reuse is refused, as upstream's on_idle does;
the connection task retains the actual driver or body failure once. A started
losing connection attempt still reports a real connect/handshake failure,
but the pool's internal Canceled (another ALPN h2 attempt won) is clean local
disposal. Classification stays at these purpose-aware client tasks, not in
the generic executor or a blanket closed/EOF filter.

Regression evidence: `tests/task_results.rs` requires clean background ends
for Connection: close and an ALPN race forced at the h2 preface write while
the connecting lock is owned, a real post-response body failure
exactly once, and a real losing dial with its original source. Connection
failure and establishment-handoff regressions remain. `cargo test -p
hyper-util --features full --locked` passes on macOS arm64 with the matching
hyper override.
