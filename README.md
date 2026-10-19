# Nanonis proxy

A packet-level TCP multiplexer with a native egui dashboard. Clients connect to
`localhost:6500`; four workers share a bounded queue and forward to Nanonis ports
`6501` through `6504`. Neither listener nor upstream ports are configurable.

```sh
cargo run --release                         # desktop dashboard
cargo run --release -- --headless           # terminal-only operation
cargo run --release -- --nanonis-ip 192.168.1.20
cargo run --release -- --nanonis-host nanonis.lab.local
```

On NixOS, use `nix develop` for the native window-system libraries. The desktop
uses egui's default styling and fonts. There are no command-specific decoders,
application-error interpretations, payload captures, or automatic request replays.

## Connection controls

The proxy connects automatically at startup. **Disconnect** closes the listener,
cancels queued and enqueue-blocked work, and lets running transfers finish before
closing upstream and remaining client sockets. Clients whose work was canceled
must reconnect. **Force Disconnect** interrupts remaining transfers if draining
cannot finish. There is intentionally no command execution timeout: Nanonis
operations may take a long time.

The host / IP field accepts hostnames, IPv4, and IPv6 addresses and is enabled
only after all connections, connection attempts, and client tasks have stopped.
Apply saves the address; Connect starts listening and reconnecting again.
Hostnames are resolved on each connection attempt; lookup failures appear in
diagnostics and retry normally. A command-line host override applies only to
that launch; `--nanonis-ip` remains an alias for `--nanonis-host`.
Closing the window or pressing Ctrl-C in headless mode interrupts transfers,
closes the proxy, and flushes queued diagnostic writes.

## Dashboard and metrics

- **Overview:** connection status, active command header name and client, elapsed
  time, reconnection failures, throughput, queue pressure, and trend graphs.
- **Clients:** sortable live connections, states, assigned ports, per-connection
  byte counts, rates, and completed/failed transfer counts. The Logs button filters
  diagnostics by client ID; session IDs in records distinguish separate launches.
- **Diagnostics:** severity, time, client, request, port, command-header name, and
  text filters; pause/follow-tail; full record details, copy, and JSONL export.
  Export creates a new file and never overwrites an existing file.

Bytes are counted once when written to the destination, including partial writes
before failure. Request bytes flow toward Nanonis; response bytes flow toward the
client. A truncated header that cannot be forwarded contributes zero forwarded
bytes. Per-worker and per-client numbers describe the same transfers and should
not be added together. Successful forwarding does **not** mean a Nanonis command
succeeded.

Enqueue backpressure measures the wait for Kanal's send to complete. Queue wait
starts when that completion is observed and ends at worker dispatch. If a worker
receives the request before the sender resumes, dispatch marks enqueue completion
and queue wait is approximately zero. Execution time includes reading
from the client, writing to Nanonis, waiting for a response when requested, and
writing it back to the client. Idle percentage uses connected time only;
disconnected time is reported separately. The queue can hold 32 requests plus
four executing requests; additional ready clients wait for capacity.

Graphs retain 900 one-second samples. Timing percentiles cover the last 60
seconds, capped at the most recent 100,000 completed or failed transfers.
Unavailable statistics display N/A. Counters last for the process lifetime;
client counters last for the connection lifetime.

## Configuration and diagnostics

Configuration uses the platform application configuration directory and diagnostic
files use the platform local application data directory. On Linux these are
`~/.config/nanonis-proxy/config.json` and
`~/.local/share/nanonis-proxy/logs/` (or the corresponding XDG overrides).
The diagnostic viewer displays the actual log directory.

Logs retain `proxy.jsonl` plus four rotated files, each up to 10 MiB. The live
viewer holds the latest 10,000 records, including retained records from earlier
sessions. **Search retained files** scans all retained files using the current
filters, in pages of 10,000 matching records, oldest first. Search again after
changing filters. Archive pages may shift if files rotate while browsing.
UTC filters use the displayed ISO timestamp format (`2026-10-18T22:30:00.000Z`).

Disk logging has a bounded 4,096-record queue. If storage fails or cannot keep up,
the dashboard reports the error and the number of records not persisted; live
memory diagnostics remain available. `RUST_LOG` controls terminal tracing without
suppressing diagnostic-file records. Failure details include transport stage,
error chain, correlation IDs, forwarded bytes, and elapsed timings. Packets and
application results are never retained or decoded.

## Development

`backend.rs` retains the original client handler, per-port retry loop, shared
Kanal queue, and oneshot socket return. Packet forwarding uses the original
`Cursor(header).chain(src.take(body_len))` with `tokio::io::copy`.
`instrumentation.rs` observes client/worker state, timings, and successful writes;
its writer wrapper leaves buffering and forwarding to Tokio. `lifecycle.rs`
adapts that backend to GUI controls and publishes telemetry snapshots. Its
cancellation hooks stop waits on disconnect and interrupt transfers only on force
disconnect or application shutdown. Mock TCP tests live in `backend_tests.rs`.

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

Tests use local mock TCP peers; no Nanonis hardware is required.
