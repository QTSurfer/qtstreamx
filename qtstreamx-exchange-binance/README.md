# qtstreamx-exchange-binance

Public Binance market-data adapter for spot and USDT-margined futures.

`BinanceStreamClient.spot(config)` and `.futures(config)` implement
`StreamClient` and normalize ticker, book-ticker, kline, and futures mark-price
/ funding-rate messages. `BinanceSymbols` converts CCXT-style `Instrument`
values to Binance symbols; `BinanceAdapters` isolates JSON normalization.

Funding-rate subscriptions are futures-only. The client signals a disconnect
once through `onDisconnect`; `qtstreamx-link` owns pooled reconnection and
resubscription.

## Usage

```java
var client = BinanceStreamClient.spot(StreamClientConfig.withDefaults(JdkWebSocketClient::new));
client.connect();
client.subscribeTicker(new Instrument("BTC", "USDT"), this::onTicker);
```

## Dependency and verification

```kotlin
implementation(project(":qtstreamx-exchange-binance"))
```

```bash
gradle :qtstreamx-exchange-binance:test
```
