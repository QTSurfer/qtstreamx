# QTStreamX

High-performance normalized crypto market data streaming library for the JVM.

## Why

Existing JVM libraries for crypto streaming (XChange, etc.) are monolithic, slow to evolve, and couple transport, serialization, and exchange logic together. QTStreamX separates these concerns with clean interfaces, letting you pick the WebSocket client, codec, and transport that fit your stack.

## Architecture

```
Exchange WS ──→ StreamClient ──→ Ticker/Kline/FundingRate ──→ StreamCodec.encode() ──→ TransportPublisher
                                                                                             │
                                                                                           NATS
                                                                                             │
                    TransportSubscriber ──→ StreamCodec.decode() ──→ Ticker/Kline ──→ your engine/DB
```

EVM market data follows a separate source seam so confirmation and recovery do
not leak into CEX clients:

```text
EVM HTTP → DEX discovery library/CLI → typed Uniswap v2/v3 descriptors
provider A HTTP+WS ─┐
                    ├→ active/passive supervisor → durable confirmed EvmLog
provider B HTTP+WS ─┘                                → Uniswap v2/v3 MarketTrade
                                                     → MarketTicker + event-time MarketKline
```

Every box is an interface. Swap any implementation without touching the rest.

The provider route classifies both HTTP and WebSocket capabilities before
capture, selects one complete bundle at a time, and replaces it only after
typed upstream recovery exhaustion. Both bundles reuse one provider-neutral
stream identity and fsynced checkpoint. After a restart or switch, bounded HTTP
reconciliation reaches the safe head before WebSocket delivery becomes live;
the downstream cursor advances only after the normalized trade batch is durably
accepted. There is no request hedging or skip-to-latest fallback. See the
[canary active/passive runbook](qtstreamx-canary/README.md#durable-activepassive-uniswap-capture)
for runtime configuration, limits, metrics, alerts, and recovery steps.

## Uniswap market data

QTStreamX can capture read-only Uniswap v2 and v3 swaps from explicitly
configured EVM markets. The EVM source combines WebSocket delivery with HTTP
catch-up, orders and deduplicates logs, and releases events only after the
configured confirmation policy. Both adapters implement `MarketTradeStream`
and normalize confirmed swaps into market-scoped `MarketTrade` records; the
aggregation module derives honest last-price tickers and event-time candles.

Pool token ordering and logical market orientation are separate and explicit.
For example, an on-chain USDC/WETH pool can publish the logical instrument
`WETH/USDC` even when WETH is token1. `MarketId` retains the network, venue, and
contract address so multiple markets or versions for the same instrument never
collide.

`qtstreamx-dex-core` provides the shared `EvmToken` and `EvmTokenPair`
vocabulary. It centralizes address normalization, token ordering, spot-market
validation, and base/quote mapping while versioned modules retain their own ABI,
pricing, fee, and event semantics.

For concise examples and canaries, the versioned adapters expose a deliberately
small immutable set through `KnownUniswapV2Markets` and
`KnownUniswapV3Markets`; discovery exposes matching factory inputs through
`KnownUniswapDeployments`. These are reviewed conveniences, not dynamic
registries, rankings, or automatic subscription lists. Concrete markets remain
outside `qtstreamx-dex-core` because protocol version, factory ABI, fee tier,
and native contract identity belong to their Uniswap modules.

| Version | Configured market | Price source | Version-specific detail |
|---|---|---|---|
| Uniswap v2 | Pair address and token metadata | Actual Swap input/output amounts | Four unsigned amount fields and pair flow |
| Uniswap v3 | Pool address, token metadata, and fee tier | `sqrtPriceX96` from Swap | Signed deltas, liquidity, tick, and fee tier |

`qtstreamx-dex-discovery-uniswap` can scan configured V2 and V3 factories into
typed descriptors through an explicit safe head and address-based orientation.
Per-network token-address allowlists provide trust and base/quote orientation;
optional recent-Swap selection plus scan, metadata, candidate, and output limits
bound the result. Discovery does not rank activity or subscribe to every result
automatically.

`qtstreamx-dex-discovery-cli` packages those read-only capabilities as the
protocol-extensible `qtstreamx-dex-discovery` executable. Shell-wide commands
list installed protocols and networks; the first adapter exposes Uniswap V2/V3
operations under an explicit `uniswap` prefix:

```bash
./gradlew :qtstreamx-dex-discovery-cli:installDist
export QTSTREAMX_EVM_HTTP_URL=https://your-provider.example
./qtstreamx-dex-discovery-cli/build/install/qtstreamx-dex-discovery/bin/qtstreamx-dex-discovery protocols
./qtstreamx-dex-discovery-cli/build/install/qtstreamx-dex-discovery/bin/qtstreamx-dex-discovery \
  uniswap pool --network robinhood 0x52e65b17fb6e5ba00ed806f37afcd2daa50271ca
```

The CLI supports equivalent numbered menus and scriptable commands, plus a
versioned JSON envelope carrying protocol ownership, provenance, and review
status. Discovery and token-name search are explicitly range/result/call
bounded. Provider endpoints remain runtime-only; token metadata is untrusted;
successful ABI inference does not make an unknown factory reviewed. See the
[DEX discovery CLI](qtstreamx-dex-discovery-cli/README.md) for commands, exit
codes, the bounded RUBY example, and intentional V4/indexer exclusions.

The intended DEX path after selection is deliberately market-data focused:
enrich selected pools with native metadata, normalize confirmed swaps as ticks,
derive 1-second event-time candles, and publish them to NATS for downstream
QuestDB persistence. Pool-explorer ranking, fiat TVL, and a search UI are not
QTStreamX requirements.

For a small, local, durable evidence capture, the installed discovery CLI also
offers `uniswap capture` for one reviewed V2 pair or V3 pool. It writes a pure
normalized-trade CSV (`event_id,timestamp_us,price,base_amount,quote_amount,side`)
and a sibling metadata CSV with the immutable market identity and observed time
range. Capture uses active/passive HTTP+WebSocket provider bundles, reconciles
confirmed logs across restart/failover, fsyncs accepted rows before advancing
the checkpoint, and deduplicates replayed event IDs. Provider endpoints remain
runtime-only and are redacted. See the [DEX discovery CLI capture
guide](qtstreamx-dex-discovery-cli/README.md#durable-one-contract-csv-capture)
for its intentionally one-contract command grammar and safety limits.

The canary proves the first half of that path without handwritten market
descriptors: bounded V2/V3 factory discovery feeds the existing confirmed swap
streams and produces normalized trades, tickers, and default 1-second candles.
The unchanged V3 path is live-verified on Ethereum mainnet (`eip155:1`) and
Robinhood Chain mainnet (`eip155:4663`), with full network identity retained in
every market. Production scheduling, NATS publication, and persistence remain
later work.

The integration is deliberately market-data only: it does not route orders,
synthesize bid/ask, sign transactions, or manage liquidity. See the
[Uniswap discovery module](qtstreamx-dex-discovery-uniswap/README.md),
the [Uniswap v2 module](qtstreamx-dex-uniswap-v2/README.md) and
[Uniswap v3 module](qtstreamx-dex-uniswap-v3/README.md) for their concrete
configuration, and the [canary module](qtstreamx-canary/README.md) for
deterministic and opt-in live capture usage.

The opt-in Ethereum mainnet integration tests use the credential-free dRPC
public endpoints `https://eth.drpc.org` and `wss://eth.drpc.org` by default.
Set `QTSTREAMX_EVM_HTTP_URL` and `QTSTREAMX_EVM_WS_URL` to test with another
provider; private endpoints and credentials remain runtime-only inputs.
The Robinhood test uses QuickNode's credential-free documentation endpoint by
default because historical discovery needs archive HTTP state plus a standard
JSON-RPC WebSocket. Dedicated overrides are
`QTSTREAMX_ROBINHOOD_HTTP_URL` and `QTSTREAMX_ROBINHOOD_WS_URL`; see the
[canary module](qtstreamx-canary/README.md#robinhood-chain-mainnet).

## Modules

All 25 modules are implemented. The 22 marked **JitPack** are published as
dependency artifacts; the other three are source and application modules — the
CLI ships as a GitHub Release asset, and the canary and benchmark are
development tools.

| Module | Description | JitPack | Dependencies |
|--------|-------------|:-------:|-------------|
| `qtstreamx-core` | Model records, interfaces (zero heavy deps) | yes | SLF4J |
| `qtstreamx-codec-json` | Jackson-based codec | yes | Jackson |
| `qtstreamx-codec-msgpack` | Hand-tuned MessagePack codec (fastest) | yes | msgpack-core |
| `qtstreamx-ws-native` | `java.net.http.WebSocket` implementation (zero deps) | yes | — |
| `qtstreamx-ws-javaws` | TooTallNate's Java-WebSocket | yes | Java-WebSocket 1.6.0 |
| `qtstreamx-transport-nats` | NATS publisher + subscriber | yes | jnats 2.20.6 |
| `qtstreamx-exchange-binance` | Binance WS: bookTicker, klines, markPrice (spot + futures) | yes | Jackson |
| `qtstreamx-exchange-bybit` | Bybit v5 WS: tickers, klines, funding, and instrument discovery (spot + linear) | yes | Jackson |
| `qtstreamx-exchange-okx` | OKX v5 WS: bbo-tbt, candles, funding, and instrument discovery | yes | Jackson |
| `qtstreamx-exchange-kraken` | Kraken WS: spot v2 plus futures v1 tickers, klines, and funding | yes | Jackson |
| `qtstreamx-exchange-bitget` | Bitget v2 WS: spot and USDT futures tickers, klines, and funding | yes | Jackson |
| `qtstreamx-exchange-gateio` | Gate.io v4 WS: spot and USDT futures tickers, klines, and funding | yes | Jackson |
| `qtstreamx-exchange-htx` | HTX WS: spot and linear-swap tickers, klines, and funding | yes | Jackson |
| [`qtstreamx-link`](qtstreamx-link/README.md) | Connection pool, partitioner, reconnect, instrument refresh | yes | Jackson, SLF4J |
| [`qtstreamx-chain-evm-rpc`](qtstreamx-chain-evm-rpc/README.md) | Bounded EVM reads and confirmed logs with WS/HTTP recovery | yes | Jackson, JDK HTTP |
| `qtstreamx-dex-core` | Shared EVM token metadata and logical pair orientation | yes | core |
| `qtstreamx-dex-uniswap-v2` | Explicit-pair v2 Swap normalization plus reviewed examples | yes | dex-core, chain-evm-rpc |
| `qtstreamx-dex-uniswap-v3` | Explicit-pool v3 Swap normalization plus reviewed examples | yes | dex-core, chain-evm-rpc |
| [`qtstreamx-dex-discovery-uniswap`](qtstreamx-dex-discovery-uniswap/README.md) | Bounded V2/V3 factory discovery, address selection, and known deployments | yes | dex adapters, chain-evm-rpc |
| `qtstreamx-dex-capture-csv` | Durable normalized-trade CSV writer with fsynced checkpoints and replay dedup | yes | dex adapters, chain-evm-rpc |
| `qtstreamx-market-aggregation` | Market-scoped tickers and event-time candles | yes | core |
| `qtstreamx-discovery-binance` | Binance spot + futures `exchangeInfo` → `Instrument` set | yes | Jackson, SLF4J |
| [`qtstreamx-dex-discovery-cli`](qtstreamx-dex-discovery-cli/README.md) | Protocol-aware terminal discovery with an initial Uniswap V2/V3 adapter | no | discovery-uniswap, dex adapters, Jackson |
| [`qtstreamx-canary`](qtstreamx-canary/README.md) | CEX capture plus explicit and discovery-driven Uniswap verification | no | adapters, discovery, Jackson |
| `qtstreamx-benchmark` | JMH benchmarks (Jackson, simdjson, fastjson2, Gson) | no | JMH |

## JitPack and local development

QTStreamX libraries are distributed through JitPack. Add the repository and a
module dependency (replace `0.1.0-rc.1` with a published tag):

```xml
<repositories>
  <repository>
    <id>jitpack.io</id>
    <url>https://jitpack.io</url>
  </repository>
</repositories>

<dependency>
  <groupId>com.qtsurfer.qtstreamx</groupId>
  <artifactId>qtstreamx-link</artifactId>
  <version>0.1.0-rc.1</version>
</dependency>
```

For local development, publish the approved libraries to the local Maven
repository:

```sh
cd qtstreamx
./gradlew publishToMavenLocal
```

The root project is an aggregate and is not a published dependency artifact.
The CLI, canary, and benchmark are source/application modules rather than
JitPack libraries. CLI JVM and native distributions for Linux AMD64, macOS
ARM64, and Windows AMD64 are attached to GitHub Releases.

**Only release tags build on JitPack.** `jitpack.yml` requires the requested
version to match the repository's `VERSION` file exactly, so branch and commit
snapshots such as `main-SNAPSHOT` deliberately fail. This keeps every resolvable
JitPack coordinate tied to a tagged, tested release. To consume unreleased work,
clone the repository and run `./gradlew publishToMavenLocal`.

## Model

All exchange data is normalized into Java records with microsecond timestamps:

```java
public record Ticker(
    Instrument instrument,  // BTC/USDT, BTC/USDT:USDT
    BigDecimal bid, BigDecimal bidSize,
    BigDecimal ask, BigDecimal askSize,
    BigDecimal last,
    BigDecimal open, BigDecimal high, BigDecimal low,
    BigDecimal volume, BigDecimal quoteVolume,
    long timestamp          // epoch µs
) {}

public record Kline(
    Instrument instrument, String interval,
    BigDecimal open, BigDecimal high, BigDecimal low, BigDecimal close,
    BigDecimal volume, BigDecimal quoteVolume,
    boolean closed, long timestamp
) {}

public record FundingRate(
    Instrument instrument, BigDecimal rate, BigDecimal markPrice,
    long nextFundingTime, int intervalHours, long timestamp
) {}

// CCXT-style instrument naming
public record Instrument(String base, String quote, String settle) {
    // BTC/USDT (spot), BTC/USDT:USDT (perp), BTC/USD:BTC (coin-margined)
}
```

## Interfaces

```java
// Codec — swap JSON/MsgPack/Protobuf
public interface StreamCodec<T> {
    byte[] encode(T value);
    T decode(byte[] data, Class<T> type);
    String name();
}

// Exchange client — one per exchange
public interface StreamClient extends AutoCloseable {
    void connect();
    void subscribeTicker(Instrument instrument, Consumer<Ticker> handler);
    void subscribeKline(Instrument instrument, String interval, Consumer<Kline> handler);
    void subscribeFundingRate(Instrument instrument, Consumer<FundingRate> handler);
    boolean isConnected();
}

// Normalized trade stream — shared by Uniswap v2 and v3
public interface MarketTradeStream extends AutoCloseable {
    void onError(Consumer<Throwable> handler);
    void start(Consumer<MarketTrade> handler) throws Exception;
    boolean isConnected();
}

// WebSocket — swap JDK/Java-WebSocket/Netty
public interface WebSocketClient extends AutoCloseable {
    void connect(String url);
    void send(String message);
    void onMessage(Consumer<String> handler);
    void onClose(BiConsumer<Integer, String> handler);
    void onError(Consumer<Throwable> handler);
    boolean isOpen();
}

// Transport — swap NATS/Kafka/Redis
public interface TransportPublisher extends AutoCloseable {
    void publish(String subject, byte[] data);
}

public interface TransportSubscriber extends AutoCloseable {
    void subscribe(String subject, Consumer<byte[]> handler);
}
```

## Build

Requires Java 25. The Gradle wrapper is included, so no Gradle installation is
needed — always invoke `./gradlew`.

```bash
./gradlew build                    # compile + test
./gradlew test                     # tests only
./gradlew :qtstreamx-core:test    # single module tests
./gradlew check                    # tests + repository verifications
```

`check` also verifies that only the approved modules publish, that the resolved
dependency graph and the JARs shipped in the CLI both match
[THIRD_PARTY_LICENSES.md](THIRD_PARTY_LICENSES.md), and that the build version
matches `VERSION`. `build` depends on `check`, so a plain `./gradlew build`
covers all of it.

## Status

Core models, interfaces, codecs, transports, WebSocket clients, and CEX market
data adapters for Binance, Bybit, OKX, Kraken, Bitget, Gate.io, and HTX are
implemented. Binance and Bybit are the current operational focus; the other
adapters remain available for explicit capture and validation. Confirmed EVM log
recovery, Uniswap v2/v3 normalization, and market aggregation are implemented.
The installable DEX discovery CLI exposes the
bounded Uniswap lookup path through menus, commands, and stable JSON. Both
Uniswap paths have deterministic disconnect recovery coverage and opt-in
Ethereum and Robinhood mainnet canaries that produce trade, ticker, and candle
artifacts. Durable capture can select an active/passive provider bundle without
changing the market or stream identity.

## Project documentation

- [CONTRIBUTING.md](CONTRIBUTING.md) — how to propose and verify a change
- [AGENTS.md](AGENTS.md) — conventions and guardrails for automated contributors
- [CHANGELOG.md](CHANGELOG.md) — released changes, Keep a Changelog format
- [SECURITY.md](SECURITY.md) — private vulnerability reporting
- [SUPPORT.md](SUPPORT.md) — how to ask for help
- [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)
- [THIRD_PARTY_LICENSES.md](THIRD_PARTY_LICENSES.md) — dependency attribution

## License

Apache License 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
