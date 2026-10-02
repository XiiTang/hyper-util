# Boundless SOCKS patch

Based on upstream `v0.1.20` (`b23a13e2b7ee73e15ba008cd9b19dcd2d3861957`).
Retain upstream history and license; keep application behavior outside this fork.

`src/client/legacy/connect/proxy/socks/v5/mod.rs` strips URI IPv6 brackets for
IP-address classification. Without this change an IPv6 literal is encoded as a
SOCKS domain. The original hostname remains intact for proxy-side DNS. This patch
adds no DNS lookup, retry or route fallback.

```sh
cargo test --features full --lib uri_hosts_preserve_socks_address_types
```

The regression checks exact SOCKS handshake bytes for IPv6, IPv4 and domains,
including both DNS modes for literals. Boundless additionally tests fixed local
resolver answers, proxy-side DNS and username/password authentication.

Boundless consumes a full commit SHA. Before updating that pin, compare with the
upstream release and run both fork and downstream regressions. Remove the fork
override when an official release provides this behavior and those checks pass.

## Proxy authentication guard

Tunnel and SOCKS5 connectors accept an optional caller-owned RequestGuard.
CONNECT checks after TCP/TLS connection establishment and immediately before
writing its authorization-bearing request. SOCKS5 checks before negotiation
and again after the greeting, immediately before username/password dispatch.
Guard failures retain the original error source. No guard affects application
traffic after an authenticated tunnel is established. Boundless's owned proxy
fixtures cover waits crossing expiry and zero credential bytes after refusal.

## Selected upstream fixes (2026-10-02)

Backport #302/#306 for centralized URI IPv6 normalization; retire the original
classification implementation and retain its exact-wire regression. Backport
#308 for non-consuming fragmented reply parsing and correct domain ports, with
all IPv4/IPv6/domain split positions and coalesced responses covered. Parse
retained responses before another socket read. Backport #292 to cancel the idle
pool timer when the pool empties. Preserve post-wait authentication guards and
the local CONNECT parser that retains every early tunnel byte; upstream #315
does not yet return buffered early bytes. No retry or route fallback is added.

Validation: `cargo test --features full --lib` using the matching Boundless hyper
override: 82 passed, one existing ignore on macOS arm64.
