# qtstreamx-exchange-gateio

Gate.io v4 public-market adapters for spot and USDT futures.

`GateioSpotStreamClient` emits normalized spot tickers and candles.
`GateioFuturesStreamClient` adds USDT-futures tickers and funding rates; futures
klines use a different Gate.io channel and are intentionally unsupported by
that client. `GateioInstrumentsCache` supplies active spot or futures contracts
to `qtstreamx-link`.

This is a passive adapter retained for explicit validation and comparative
captures; it is not an implicit production-routing choice.

## Dependency and verification

```kotlin
implementation(project(":qtstreamx-exchange-gateio"))
```

```bash
gradle :qtstreamx-exchange-gateio:test
```
