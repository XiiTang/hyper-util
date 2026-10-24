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


## Preserve an unstarted reused-connection failure (driver plan v23 W7a, Rule 8)

At 66402f8, `src/client/legacy/client.rs:257-261` returns the native
error when `retry_canceled_requests(false)` is set, discarding the
`connection_reused` fact. `Error::is_canceled` is private (:1659) and cannot
distinguish that case from other cancellations. The caller cannot identify
which requests it may resend without parsing text or guessing.

`Error::is_unstarted_on_reused_connection()` now reports the conjunction
already proven by the legacy client's recovered-request branch and its
pool's reused flag. The existing return point is the one checkpoint; a
private error field, initialized false in both constructors, retains the
fact, with no public setter. Error kind, source and connection information
are preserved. Existing retry configuration and its `true` default stay.
This adds no per-attempt callback and performs no additional send.

`tests/reused_connection_retry.rs` holds the native connection driver,
queues a request on a reused connection, then removes that driver before
it can take the request. With retry off there is one dial and one warm-up
wire request, and the returned error retains the fact. With the untouched
default there is a second dial and request. Clearing only the retained
fact reproduces the regression (exit 101); the fixed focused test passes
(exit 0). The test waits on driver and peer state, with a timeout only as
hang protection.

On macOS arm64, Rust 1.98.1, `cargo test --all-features` passes:
127 tests, nine ignored (exit 0). `cargo fmt --all --check` passes
(exit 0). Strict `cargo clippy --all-features --all-targets -- -D warnings`
exits 101 both on untouched 66402f8 and this patch, with identical error
categories: 17 library and 22 library-test diagnostics, including pool
comment style, large request errors, old constructors and server reborrows.
No lint is suppressed or unrelated repair included. The current formatter
normalizes one import's order in the existing tunnel module.
