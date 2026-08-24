# AGENTS.md

Guidance for AI coding agents and automated contributors working in QTStreamX.
Human contributors should read [CONTRIBUTING.md](CONTRIBUTING.md) first; the
rules below are additional context, not a replacement for it.

## Project overview

QTStreamX is a modular Java 25 library for normalized crypto market-data
streaming. It provides interfaces for WebSocket clients, codecs, transports,
exchange stream parsing, and read-only EVM/DEX market data. Every integration
point is an interface so implementations can be swapped without touching the
rest of the stack.

## Non-negotiable rules

1. **Preserve the public boundary.** Never add credentials, private
   infrastructure hostnames, provider URLs containing keys, captured market
   data, absolute developer paths, or references to non-public planning
   material — including internal task, phase, or milestone codes. Provider
   endpoints are runtime inputs, never source. Every shipped file must make
   sense to a reader who has only this repository.
2. **Do not weaken a check to make it pass.** If a security, license,
   publication-boundary, or reproducibility check blocks you, report the
   blocker. Deleting or relaxing the check is not a fix.
3. **Use the Gradle wrapper.** `./gradlew`, never a machine-installed `gradle`.
   A clean clone must build with no external Gradle install.
4. **Java 25 only.** Do not add compatibility claims for other JDKs unless a CI
   job actually verifies them on that JDK.
5. **Market data only.** QTStreamX does not route orders, sign transactions,
   synthesize bid/ask, or manage liquidity. Do not add trading APIs.

## Tech stack

- **Language**: Java 25 (records, virtual threads, sealed interfaces)
- **Build**: Gradle 9.4.1 (Kotlin DSL), wrapper included and hash-verified
- **Test**: JUnit 5 + AssertJ
- **Distribution**: JitPack for libraries, GitHub Releases for CLI artifacts

## Build commands

```bash
./gradlew build                     # Full build + tests
./gradlew test                      # Tests only
./gradlew :qtstreamx-core:test      # Single module tests
./gradlew clean build               # Clean rebuild
./gradlew check                     # Tests + every repository verification
./gradlew publishToMavenLocal       # Approved libraries to ~/.m2
```

`check` runs the repository's own verifications, and `build` depends on it:

| Task | Invariant |
|---|---|
| `verifyPublicationBoundaries` | only the approved modules publish, each with sources and Javadoc |
| `verifyPublishedRuntimeLicenses` | the resolved library runtime graph matches `THIRD_PARTY_LICENSES.md` |
| `verifyCliRuntimeLicenses` | the third-party JARs in the CLI distribution match `THIRD_PARTY_LICENSES.md` |
| `verifyVersion` / `verifyTagVersion` | the build version and the release tag both match `VERSION` |

## Publication boundary

The repository has 25 modules; **22 of them publish Maven artifacts**. The root
aggregate, `qtstreamx-dex-discovery-cli`, `qtstreamx-benchmark`, and
`qtstreamx-canary` are source and application modules, never dependency
artifacts. CLI JVM and native distributions ship as GitHub Release assets.

`verifyPublicationBoundaries` enforces the exact published set and that every
published module attaches sources and Javadoc JARs. Changing that set is a
deliberate, reviewed decision — never a side effect of adding a module.

`verifyPublishedRuntimeLicenses` resolves the external runtime graph of every
published library and requires it to match the **Published library runtime
graph** table in `THIRD_PARTY_LICENSES.md` exactly. That table is the single
source of truth for the published dependency set, so a dependency change is
incomplete until the table is updated in the same commit.

## Module structure

```text
qtstreamx-core                    Interfaces + model records (no heavy deps)
  model/                          Ticker, Kline, FundingRate, Instrument, Exchange
  codec/                          StreamCodec<T>
  client/                         StreamClient, StreamClientConfig
  ws/                             WebSocketClient
  transport/                      TransportPublisher, TransportSubscriber

qtstreamx-codec-json              Jackson StreamCodec implementation
qtstreamx-codec-msgpack           MessagePack StreamCodec (hand-tuned, no reflection)
qtstreamx-ws-native               java.net.http.WebSocket implementation (zero deps)
qtstreamx-ws-javaws               Java-WebSocket implementation
qtstreamx-transport-nats          NATS publisher + subscriber
qtstreamx-link                    Connection pool, partitioner, reconnect, instrument refresh

qtstreamx-exchange-binance        Binance stream client (spot + USDT-M futures)
qtstreamx-exchange-bybit          Bybit stream client (spot + linear USDT perps)
qtstreamx-exchange-okx            OKX stream client (spot + linear swaps)
qtstreamx-exchange-kraken         Kraken stream clients (spot + futures)
qtstreamx-exchange-bitget         Bitget stream client (spot + USDT futures)
qtstreamx-exchange-gateio         Gate.io stream clients (spot + USDT futures)
qtstreamx-exchange-htx            HTX stream clients (spot + linear swaps)
qtstreamx-discovery-binance       Binance exchangeInfo to Instrument sets

qtstreamx-chain-evm-rpc           Bounded EVM reads + confirmed-log recovery
qtstreamx-dex-core                Shared EVM tokens + logical pair orientation
qtstreamx-dex-uniswap-v2          Uniswap v2 pair normalization + known examples
qtstreamx-dex-uniswap-v3          Uniswap v3 pool normalization + known examples
qtstreamx-dex-discovery-uniswap   Bounded discovery + known deployments
qtstreamx-dex-capture-csv         Durable normalized-trade CSV writer with fsynced checkpoints
qtstreamx-market-aggregation      Market-scoped ticker/candle aggregation

qtstreamx-dex-discovery-cli       Terminal discovery/capture tool     (not published)
qtstreamx-canary                  Explicit + discovery-driven capture (not published)
qtstreamx-benchmark               JMH benchmarks                      (not published)
```

## Conventions

### Module and package naming

- `qtstreamx-chain-*` — reusable blockchain infrastructure
- `qtstreamx-dex-*` — DEX domain values, discovery, and protocol adapters
- `qtstreamx-market-*` — venue-neutral market processing
- `com.qtsurfer.qtstreamx.core.*` — core interfaces and model
- `com.qtsurfer.qtstreamx.codec.<impl>` — codec implementations
- `com.qtsurfer.qtstreamx.ws.<impl>` — WebSocket implementations
- `com.qtsurfer.qtstreamx.transport.<impl>` — transport implementations
- `com.qtsurfer.qtstreamx.dex.core` — shared EVM DEX value types and invariants
- `com.qtsurfer.qtstreamx.dex.uniswap.<version>` — versioned Uniswap adapters
- `com.qtsurfer.qtstreamx.exchange.<name>` — exchange-specific stream clients

### Java imports

- Import Java types normally; do not use fully qualified class names in field,
  parameter, local-variable, constructor, or generic declarations.
- Use a fully qualified name inline only when two required types share a simple
  name and the collision cannot be removed cleanly.

### Model records

- All model types are Java **records** — immutable, no Lombok.
- Timestamps are always **epoch microseconds**.
- Instrument names follow **CCXT unified format**: `BTC/USDT` (spot),
  `BTC/USDT:USDT` (linear perp), `BTC/USD:BTC` (coin-margined).
- Nullable fields use object types (`BigDecimal`), never primitives.

### Interface design

- Every extension point is an interface in `qtstreamx-core`.
- Implementations live in their own module with minimal dependencies.
- No reactive libraries in the hot path (exchange clients, codecs).
- Exchange clients use `Consumer<T>` callbacks, not observables.

### Codec implementations

- `StreamCodec<T>` is the contract: `encode(T) -> byte[]`,
  `decode(byte[], Class<T>) -> T`.
- The JSON codec is generic and uses a Jackson `ObjectMapper`.
- The MessagePack codec is hand-tuned per model type: fixed field order, no
  reflection.
- New codecs go in their own `qtstreamx-codec-<name>/` module.

### Exchange clients

- Each exchange module implements `StreamClient`.
- Parse exchange-native JSON into normalized model records.
- Handle WebSocket reconnection internally.
- Support snapshot plus delta merge for stateful streams.
- Public market-data streams need no authentication; do not add credential
  handling to them.

### Documentation in published modules

Javadoc on published libraries is part of the release artifact. Write it so a
reader outside this repository can act on it: no internal planning references,
no phase or task codes, no links to non-public resources. `./gradlew javadoc`
runs with doclint enabled and fails on error.

### Testing

- JUnit 5 + AssertJ.
- JSON fixtures in `src/test/resources/` for exchange message parsing.
- Test adapters and normalization separately from WebSocket connection.
- Codec tests verify roundtrip: `decode(encode(x))` equals `x`.
- Network-dependent tests are opt-in and must never be required for a clean
  `./gradlew build`.

## Key design decisions

1. **No Lombok** — records provide immutability and remove the boilerplate.
2. **No reactive types in core** — callbacks are simpler and sufficient.
3. **Microsecond timestamps** — chosen to avoid conversion at write time.
4. **CCXT naming** — an industry standard that enables cross-exchange matching.
5. **Interface-first** — every integration point is pluggable.
6. **Gradle Kotlin DSL** — typed build logic across a 25-module tree.

## Adding a new exchange

1. Create `qtstreamx-exchange-<name>/` with a `build.gradle.kts`.
2. Depend on `qtstreamx-core` and Jackson.
3. Implement `StreamClient`.
4. Parse the exchange-specific WebSocket messages.
5. Normalize to `Ticker`, `Kline`, and `FundingRate`.
6. Add JSON fixtures in `src/test/resources/` and parsing unit tests.
7. Register the module in `settings.gradle.kts`.
8. If it should publish, add it to `libraryModules` in the root
   `build.gradle.kts` and add a row to the README module table.

## Adding a new codec

1. Create `qtstreamx-codec-<name>/` with a `build.gradle.kts`.
2. Implement `StreamCodec<T>` for the target types.
3. Add roundtrip tests.
4. Register the module in `settings.gradle.kts` and, if published,
   in `libraryModules`.

## Changing a third-party dependency

A version bump or a new dependency is not done until all of these agree:

1. the module `build.gradle.kts` files that declare it;
2. the **Published library runtime graph** table in `THIRD_PARTY_LICENSES.md`,
   if the dependency reaches a published library's runtime classpath;
3. the **Runtime distribution set** table in `THIRD_PARTY_LICENSES.md`, if the
   JAR ships inside the CLI distribution;
4. any attribution text the new version requires.

`./gradlew check` reads those tables, so a mismatch fails the build naming the
exact missing or unexpected coordinate. There is no second list to update.

## Versioning and changelog

- `VERSION` at the repository root is the single source of truth. Release tags
  are SemVer without a `v` prefix and must match `VERSION` exactly; CI enforces
  this through `verifyVersion` and `verifyTagVersion`.
- `CHANGELOG.md` follows [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/).
  Add user-visible changes under `## [Unreleased]`; release notes are generated
  from the matching version section.
- Update `CHANGELOG.md` in the same change that alters public API or release
  behavior, not afterwards.

## Before opening a pull request

```bash
./gradlew clean build javadoc
```

Then confirm the PR checklist: no credentials, private endpoints, captured
data, or internal operational details; publication boundary unchanged or
intentionally reviewed; changelog updated when user-visible behavior changed.
