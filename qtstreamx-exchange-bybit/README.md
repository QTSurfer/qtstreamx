# qtstreamx-exchange-bybit

Bybit v5 public-market adapter for spot and linear USDT perpetuals.

`BybitStreamClient.spot(config)` and `.linear(config)` normalize ticker, kline,
and linear funding-rate topics. The client merges Bybit's partial ticker deltas
and retains funding fields so each emitted `FundingRate` is complete.

`BybitInstrumentsCache` implements `InstrumentsCache` for spot or linear
catalogues. It follows the paginated linear response and records each symbol's
funding interval, avoiding silent omission of later catalogue pages. Use it
with `LinkManager` when the subscription universe should refresh dynamically.

## Operational status

Bybit is a current operational target. The suite includes an opt-in live
catalogue test: set `QTSTREAMX_LIVE_IT=1` to run it.

## Dependency and verification

```kotlin
implementation(project(":qtstreamx-exchange-bybit"))
```

```bash
gradle :qtstreamx-exchange-bybit:test
QTSTREAMX_LIVE_IT=1 gradle :qtstreamx-exchange-bybit:test
```
