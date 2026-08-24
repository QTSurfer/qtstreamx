# qtstreamx-market-aggregation

DEX-neutral, in-process projection of ordered `MarketTrade` values into
market-scoped tickers and fixed event-time candles.

## Interface

```java
MarketDataAggregator aggregator = new MarketDataAggregator(
        CandleInterval.ONE_MINUTE,
        marketTicker -> consumeTicker(marketTicker),
        marketKline -> consumeKline(marketKline));

aggregator.accept(trade);
aggregator.advanceWatermark(watermarkMicros);
```

`accept` synchronously emits one `MarketTicker` and one live `MarketKline` for
each accepted trade. When a later trade enters a new bucket, the previous
candle closes before the new live candle is emitted. `advanceWatermark` closes
quiet buckets whose inclusive `closeTime` is at or behind the watermark. Empty
candles are not synthesized.

The caller supplies serialized calls; the aggregator intentionally owns no
thread, clock, scheduler, network, or persistence dependency.

## Semantics

- State is partitioned by the complete `MarketId`, not just `Instrument`.
- Ticker `last` and timestamp come from the trade. Bid, ask, sizes, and rolling
  CEX statistics remain null.
- Candle timestamps are exact epoch-microsecond boundaries. OHLC follows input
  order; base and quote volumes preserve `BigDecimal` arithmetic.
- Watermarks are inclusive and monotonic. Trades at or behind the watermark are
  late and rejected.
- Duplicate event IDs and decreasing per-market event times are rejected before
  any output. Distinct trades sharing one block timestamp remain valid and are
  aggregated in arrival order.
- Only active market/interval buckets and their event IDs are retained. Closing
  a bucket releases that state.

## Verification

```bash
gradle :qtstreamx-market-aggregation:test
```

The tests include an integration path from a canonical Uniswap v3 `Swap` log,
through `UniswapV3MarketDataStream`, into ticker and candle output.
