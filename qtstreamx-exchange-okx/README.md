# qtstreamx-exchange-okx

OKX v5 public-market adapter for spot and linear swaps.

`OkxStreamClient` normalizes best-bid/offer ticker, candle, and swap funding
messages through the common `StreamClient` API. `OkxSymbols` maps normalized
instruments to OKX IDs and `OkxAdapters` keeps exchange JSON separate from the
stream lifecycle.

`OkxInstrumentsCache` discovers tradable spot instruments or linear swaps from
the public instruments endpoint and can feed `qtstreamx-link`. It is retained
as a validated reserve adapter, not the current default production route.

## Dependency and verification

```kotlin
implementation(project(":qtstreamx-exchange-okx"))
```

```bash
gradle :qtstreamx-exchange-okx:test
```
