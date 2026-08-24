# qtstreamx-core

The dependency-light public vocabulary of QTStreamX. Exchange, DEX, transport,
codec, and aggregation modules depend on this module rather than on each other.

## Main APIs

- Immutable normalized records: `Instrument`, `Ticker`, `Kline`, `FundingRate`,
  `MarketTrade`, `MarketTicker`, and `MarketKline`.
- `StreamClient` for public market-data WebSocket subscriptions.
- `MarketTradeStream` and recovery/batch contracts for normalized DEX trades.
- `StreamCodec`, `WebSocketClient`, `TransportPublisher`, and
  `TransportSubscriber` extension points.

Timestamps are epoch microseconds and instruments use CCXT-style notation:
`BTC/USDT` for spot and `BTC/USDT:USDT` for a linear perpetual. `StreamClient`
callbacks run on WebSocket reader threads, so consumers must be non-blocking and
thread-safe.

## Dependency

```kotlin
implementation(project(":qtstreamx-core"))
```

## Verification

```bash
gradle :qtstreamx-core:test
```
