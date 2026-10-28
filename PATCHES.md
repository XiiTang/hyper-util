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

## Restore immediate malformed CONNECT rejection (F11)

Start from upstream b23a13e2 `src/client/legacy/connect/proxy/tunnel.rs:206-229`:
it classifies a received reply immediately and rejects an invalid prefix.
The retained fork parser now runs its existing httparse parser after each
byte instead of waiting for the header terminator. Partial valid headers keep
waiting; malformed bytes fail immediately. The only needed differences from
upstream remain fragmented-prefix tolerance, informational/2xx parsing, and
one-byte reads that leave early tunnel bytes in the original transport.
`an_invalid_connect_prefix_is_rejected_while_the_proxy_stays_open` drives the
invalid-prefix path; `fragmented_connect_preserves_bytes_following_the_response_header`
keeps the existing tunnel-boundary regression.

On macOS arm64 with consumer Rust 1.97.0 and its pinned hyper/h2, all-feature
tests and formatting pass. Strict all-target Clippy is blocked by unchanged
code (15 library and 20 library-test diagnostics in pool, client/cache, DNS,
constructors and server reborrows). No diagnostic names the touched parser or
regression, and those other source files are unchanged from the base.

## Preserve immediate final CONNECT refusal (review B F11)

Start from upstream b23a13e2 `connect/proxy/tunnel.rs:225-228`: a known 407
answers ProxyAuthRequired immediately; other final refusals answer
TunnelUnsuccessful without awaiting headers or EOF. Port those same exits
before the incremental parser's Partial continuation. The necessary fork
difference remains tolerance for fragmented valid informational/2xx heads,
with one-byte reads preserving the tunnel-byte boundary. A parsed final
refusal needs no rest-of-head wait. Invalid syntax, I/O errors, EOF and the
8192-byte budget retain their existing exits.

`final_connect_refusals_are_reported_while_the_proxy_stays_open` supplies
only a 407 or 403 status line and keeps the real TCP proxy open until its
client closes. At 8cec82b7 the first case hangs (test exits 101); after this
change both retain their distinct refusal and the peer sees closure.
Fragmented success and early tunnel bytes remain covered by the existing
regression.

On macOS arm64 with consumer Rust 1.97.0 and pinned hyper/h2, all-feature
tests (including both held-open refusals) and fmt pass (exit 0). Strict
all-target Clippy still reports the previously reviewed 15 library / 20
library-test diagnostics in unchanged files; no parser or new-test diagnostic
is reported. That baseline blocker is retained, with no suppression/repair.

## Reject every already-refused CONNECT status (review B recheck F11)

Start from upstream b23a13e2 `connect/proxy/tunnel.rs:216-228`: after the
special 407 result, unsupported replies take TunnelUnsuccessful without
waiting for more headers. The previous incremental classifier omitted
parsed 0xx codes and could wait forever on a held-open 099 status line.
At that same checkpoint, retain only 100 and 102..=299 while waiting for
the head boundary; any other parsed status is refused, with 407 still
ProxyAuthRequired and 101 still TunnelUnsuccessful. An unparsed status
continues parsing. The necessary upstream differences remain fragmented
valid informational/success heads and one-byte reads preserving tunnel
bytes. No new parser, retry or timeout mechanism is added.

Extend `final_connect_refusals_are_reported_while_the_proxy_stays_open`
with 099 and 101 alongside 407/403. With only the test change at 1e7e8fa0,
the held-open 099 case times out (exit 101); after this correction all
four refuse immediately while the peer remains open, preserving their
distinct errors. All-feature tests, including fragmented success and
early tunnel bytes, and fmt pass with consumer Rust 1.97.0 and pinned
hyper/h2 (exit 0). Strict all-target Clippy still exits 101 with the same
15 library / 20 library-test diagnostics in unchanged files; none names
this classifier or regression. No baseline repair or suppression.
