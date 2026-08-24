# qtstreamx-discovery-binance

Dynamic Binance instrument discovery for `qtstreamx-link`.

`BinanceInstrumentsCache` calls the public `exchangeInfo` endpoint, filters to
tradable symbols, and exposes an atomic normalized `Instrument` snapshot. Its
`Market` enum supports spot, USDT-margined perpetual futures, and coin-margined
perpetual futures. Futures entries preserve their settlement asset.

Use `refresh()` before reading `snapshot()`, or pass the cache to `LinkManager`
which owns periodic refresh and subscription reconciliation.

## Usage

```java
InstrumentsCache cache = new BinanceInstrumentsCache(
        BinanceInstrumentsCache.Market.FUTURES_USDT);
```

## Dependency and verification

```kotlin
implementation(project(":qtstreamx-discovery-binance"))
```

```bash
gradle :qtstreamx-discovery-binance:test
```
