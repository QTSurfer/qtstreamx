# qtstreamx-exchange-htx

HTX public-market adapters for spot and linear swaps.

`HtxSpotStreamClient` and `HtxLinearSwapStreamClient` normalize ticker, kline,
and, for swaps, funding-rate events. HTX sends gzip-compressed frames and
requires protocol-level ping/pong handling; use a gzip-aware WebSocket client
when constructing `StreamClientConfig`. `HtxInstrumentsCache` discovers active
spot or linear-swap instruments for `qtstreamx-link`.

The module is retained for explicit validation only. Do not make it a new
production route without a separate operational and compliance review.

## Dependency and verification

```kotlin
implementation(project(":qtstreamx-exchange-htx"))
```

```bash
gradle :qtstreamx-exchange-htx:test
```
