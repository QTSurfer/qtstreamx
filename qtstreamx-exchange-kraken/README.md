# qtstreamx-exchange-kraken

Kraken public-market adapters for spot v2 and futures v1.

`KrakenSpotStreamClient` provides spot ticker and kline streams; it maps
Kraken's `XBT` symbol to normalized `BTC`. `KrakenFuturesStreamClient` provides
futures ticker, kline, and hourly funding-rate streams. Both use
`KrakenSymbols` and `KrakenAdapters` for isolated normalization.

`KrakenInstrumentsCache` discovers tradable spot or futures instruments for
`LinkManager`. The module is a validated reserve adapter and should receive a
live canary capture before becoming a production route.

## Dependency and verification

```kotlin
implementation(project(":qtstreamx-exchange-kraken"))
```

```bash
gradle :qtstreamx-exchange-kraken:test
```
