# Changelog

All notable public QTStreamX changes are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed

- The README has a badge header, an installation section with Maven and Gradle
  snippets, and a quick start.

## [0.1.0-rc.4] — 2026-10-07

### Added

- `EvmRpcHistoricalLogStream` replays confirmed logs over one closed block range
  over JSON-RPC HTTP only: no WebSocket, no checkpoint, and no replay ceiling, because
  an explicitly bounded range is not a backlog. Replaying the same range against
  unchanged chain state emits the same logs in the same order. A range that is not yet
  confirmed is rejected with `EvmHistoricalRangeNotFinalizedException`, and a finished
  replay reports the new `EvmLogStreamStatus.COMPLETED`.
- `CandleInterval.ONE_SECOND`.
- `Link.openClients()` and `LinkManager.openClientCount()` report how many stream
  clients a link has opened and not closed. It is one per connected link; a higher
  number means clients were orphaned with their socket still open.

### Fixed

- A `Link` could leave a stream client open forever, still delivering data to the
  link's sinks, so the exchange saw more connections than links and every message
  arrived more than once. Two reconnects running at the same time (a renewal and a
  dropped socket, for example) each opened a client and the second replaced the
  first without closing it. Reconnects are now one at a time per link, and a client
  is closed on every path that abandons it: a failed connect, a replaced client, or
  a link closed while a reconnect was in flight.
- Every successful reconnect armed another preemptive-renewal timer without
  cancelling the previous one, and each timer re-armed itself, so a link that had
  dropped a few times renewed several times a day and the renewals could overlap.
  A link now holds a single renewal timer.
- `EvmBlockTimestampResolver` compared its epoch-seconds input against block
  timestamps in epoch microseconds, so every lookup resolved to block 0.
- The installed-CLI tests failed on build hosts that export `JAVA_TOOL_OPTIONS`,
  because the JVM's own notice on standard error was counted as output of the
  executable under test. They no longer inherit the JVM option variables.

## [0.1.0-rc.3] — 2026-09-03

### Added

- `uniswap format` converts a completed DEX trade capture into a QTSurfer ticker
  dataset CSV, preserving microsecond timestamps and plain decimal values.

### Changed

- The DEX discovery CLI now validates the provider configuration before an
  on-chain request or capture starts, and identifies the option or environment
  variable that supplied it without exposing an endpoint value. It deliberately
  keeps RPC endpoints runtime-only rather than shipping a default provider.

## [0.1.0-rc.2] — 2026-08-31

### Changed

- Bumped `org.awaitility:awaitility` to 4.3.0, `com.alibaba.fastjson2:fastjson2`
  to 2.0.64, `org.slf4j:slf4j-api`/`slf4j-simple` to 2.0.18,
  `org.graalvm.buildtools.native` to 1.1.10, and the Gradle wrapper to 9.7.1.
- Bumped the pinned GitHub Actions versions used in CI (`checkout`,
  `setup-java`, `setup-gradle`, `wrapper-validation`, `setup-graalvm`).

### Fixed

- `FileEvmLogCheckpointStore.save()` could mask the real exception with a
  cleanup failure from its `finally` block if the atomic rename itself failed.

## [0.1.0-rc.1] — 2026-08-23

### Added

- First public release candidate of the modular Java 25 market-data streaming
  libraries and DEX discovery CLI.
- JitPack publication for the approved library modules.
- JVM and native (Linux AMD64, macOS ARM64, Windows AMD64) CLI distribution
  metadata.
