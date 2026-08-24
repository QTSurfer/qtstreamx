# qtstreamx-exchange-bitget

Bitget v2 public-market adapter for spot and USDT futures.

Use `BitgetStreamClient.spot(config)` or `.usdtFutures(config)` for normalized
ticker, kline, and futures funding-rate streams. The client owns Bitget's
application-level keepalive because the public socket closes after prolonged
client silence. `BitgetInstrumentsCache` discovers the corresponding tradable
catalogue for `qtstreamx-link`.

This is a passive adapter retained for explicit validation and comparative
captures; it is not an implicit production-routing choice.

## Dependency and verification

```kotlin
implementation(project(":qtstreamx-exchange-bitget"))
```

```bash
gradle :qtstreamx-exchange-bitget:test
```
