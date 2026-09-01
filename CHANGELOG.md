# Changelog

All notable public QTStreamX changes are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
[Semantic Versioning](https://semver.org/).

## [Unreleased]

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
