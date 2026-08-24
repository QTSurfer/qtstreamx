package com.qtsurfer.qtstreamx.aggregation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.model.MarketId;
import com.qtsurfer.qtstreamx.core.model.MarketKline;
import com.qtsurfer.qtstreamx.core.model.MarketTicker;
import com.qtsurfer.qtstreamx.core.model.MarketTrade;
import com.qtsurfer.qtstreamx.core.model.TradeSide;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class MarketDataAggregatorTest {

    private static final Instrument ETH_USDC = new Instrument("ETH", "USDC");
    private static final MarketId MARKET =
            new MarketId("uniswap-v3", "eip155:1", "0xpool-a", ETH_USDC);

    private final List<MarketTicker> tickers = new ArrayList<>();
    private final List<MarketKline> klines = new ArrayList<>();
    private final MarketDataAggregator aggregator = new MarketDataAggregator(
            CandleInterval.ONE_MINUTE, tickers::add, klines::add);

    @Test
    void emitsOneSemanticallyHonestTickerPerTrade() {
        aggregator.accept(trade(
                MARKET,
                "event-1",
                "3125.250000",
                "1.200000000000000000",
                "3750.300000",
                1_700_000_001_234_567L));

        assertThat(tickers).singleElement().satisfies(marketTicker -> {
            assertThat(marketTicker.market()).isEqualTo(MARKET);
            assertThat(marketTicker.ticker().instrument()).isEqualTo(ETH_USDC);
            assertThat(marketTicker.ticker().last()).isEqualByComparingTo("3125.250000");
            assertThat(marketTicker.ticker().timestamp()).isEqualTo(1_700_000_001_234_567L);
            assertThat(marketTicker.ticker().bid()).isNull();
            assertThat(marketTicker.ticker().ask()).isNull();
            assertThat(marketTicker.ticker().open()).isNull();
            assertThat(marketTicker.ticker().high()).isNull();
            assertThat(marketTicker.ticker().low()).isNull();
            assertThat(marketTicker.ticker().volume()).isNull();
            assertThat(marketTicker.ticker().quoteVolume()).isNull();
        });
    }

    @Test
    void emitsLiveOneMinuteOhlcvUpdatesWithExactBoundaries() {
        aggregator.accept(trade(
                MARKET, "event-1", "100.00", "1.250", "125.000",
                120_000_001L));
        aggregator.accept(trade(
                MARKET, "event-2", "110.00", "2.000", "220.000",
                179_999_999L));

        assertThat(klines).hasSize(2);
        assertThat(klines.getLast().market()).isEqualTo(MARKET);
        assertThat(klines.getLast().kline()).satisfies(kline -> {
            assertThat(kline.interval()).isEqualTo("1m");
            assertThat(kline.open()).isEqualByComparingTo("100.00");
            assertThat(kline.high()).isEqualByComparingTo("110.00");
            assertThat(kline.low()).isEqualByComparingTo("100.00");
            assertThat(kline.close()).isEqualByComparingTo("110.00");
            assertThat(kline.volume()).isEqualByComparingTo("3.250");
            assertThat(kline.quoteVolume()).isEqualByComparingTo("345.000");
            assertThat(kline.numberOfTrades()).isEqualTo(2);
            assertThat(kline.closed()).isFalse();
            assertThat(kline.timestamp()).isEqualTo(120_000_000L);
            assertThat(kline.closeTime()).isEqualTo(179_999_999L);
        });
    }

    @Test
    void closesQuietIntervalExactlyOnceAtInclusiveWatermark() {
        aggregator.accept(trade(
                MARKET, "event-1", "100", "1", "100", 120_000_001L));

        aggregator.advanceWatermark(179_999_998L);
        assertThat(klines).hasSize(1);

        aggregator.advanceWatermark(179_999_999L);
        aggregator.advanceWatermark(240_000_000L);

        assertThat(klines).hasSize(2);
        assertThat(klines.getLast().kline().closed()).isTrue();
        assertThat(klines.getLast().kline().closeTime()).isEqualTo(179_999_999L);
    }

    @Test
    void closesPreviousBucketBeforeOpeningTradeOnExactBoundary() {
        aggregator.accept(trade(
                MARKET, "event-1", "100", "1", "100", 179_999_999L));
        aggregator.accept(trade(
                MARKET, "event-2", "110", "2", "220", 180_000_000L));

        assertThat(klines).hasSize(3);
        assertThat(klines.get(1).kline().closed()).isTrue();
        assertThat(klines.get(1).kline().timestamp()).isEqualTo(120_000_000L);
        assertThat(klines.get(2).kline().closed()).isFalse();
        assertThat(klines.get(2).kline().timestamp()).isEqualTo(180_000_000L);
        assertThat(klines.get(2).kline().numberOfTrades()).isEqualTo(1);
    }

    @Test
    void partitionsSameInstrumentPoolsByFullMarketIdentity() {
        MarketId otherPool =
                new MarketId("uniswap-v3", "eip155:1", "0xpool-b", ETH_USDC);

        aggregator.accept(trade(MARKET, "a-1", "100", "1", "100", 120_000_001L));
        aggregator.accept(trade(otherPool, "b-1", "200", "2", "400", 120_000_002L));
        aggregator.accept(trade(MARKET, "a-2", "110", "3", "330", 120_000_003L));

        assertThat(klines.stream().filter(kline -> kline.market().equals(MARKET)).toList())
                .last()
                .satisfies(kline -> {
                    assertThat(kline.kline().close()).isEqualByComparingTo("110");
                    assertThat(kline.kline().numberOfTrades()).isEqualTo(2);
                });
        assertThat(klines.stream().filter(kline -> kline.market().equals(otherPool)).toList())
                .singleElement()
                .satisfies(kline -> {
                    assertThat(kline.kline().close()).isEqualByComparingTo("200");
                    assertThat(kline.kline().numberOfTrades()).isEqualTo(1);
                });
    }

    @Test
    void rejectsDuplicateEventBeforeProducingOutput() {
        MarketTrade duplicate = trade(
                MARKET, "event-1", "100", "1", "100", 120_000_001L);
        aggregator.accept(duplicate);

        assertThatThrownBy(() -> aggregator.accept(duplicate))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate");
        assertThat(tickers).hasSize(1);
        assertThat(klines).hasSize(1);
    }

    @Test
    void rejectsDecreasingEventTimeBeforeProducingOutput() {
        aggregator.accept(trade(
                MARKET, "event-2", "110", "1", "110", 120_000_002L));

        assertThatThrownBy(() -> aggregator.accept(trade(
                        MARKET, "event-1", "100", "1", "100", 120_000_001L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("out of order");
        assertThat(tickers).hasSize(1);
        assertThat(klines).hasSize(1);
    }

    @Test
    void acceptsDistinctTradesWithSameBlockTimestampInArrivalOrder() {
        aggregator.accept(trade(
                MARKET, "event-1", "100", "1", "100", 120_000_001L));
        aggregator.accept(trade(
                MARKET, "event-2", "110", "2", "220", 120_000_001L));

        assertThat(tickers).hasSize(2);
        assertThat(klines.getLast().kline().close()).isEqualByComparingTo("110");
        assertThat(klines.getLast().kline().numberOfTrades()).isEqualTo(2);
    }

    @Test
    void rejectsLateTradesAndDecreasingWatermarks() {
        aggregator.advanceWatermark(120_000_001L);

        assertThatThrownBy(() -> aggregator.accept(trade(
                        MARKET, "event-1", "100", "1", "100", 120_000_001L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("watermark");
        assertThatThrownBy(() -> aggregator.advanceWatermark(120_000_000L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("decrease");
        assertThat(tickers).isEmpty();
        assertThat(klines).isEmpty();
    }

    @Test
    void supportsOtherFixedMicrosecondIntervals() {
        List<MarketKline> fiveSecondKlines = new ArrayList<>();
        MarketDataAggregator fiveSecondAggregator = new MarketDataAggregator(
                new CandleInterval("5s", 5_000_000L),
                ignored -> {},
                fiveSecondKlines::add);

        fiveSecondAggregator.accept(trade(
                MARKET, "event-1", "100", "1", "100", 7_000_000L));
        fiveSecondAggregator.advanceWatermark(9_999_999L);

        assertThat(fiveSecondKlines.getLast().kline()).satisfies(kline -> {
            assertThat(kline.interval()).isEqualTo("5s");
            assertThat(kline.timestamp()).isEqualTo(5_000_000L);
            assertThat(kline.closeTime()).isEqualTo(9_999_999L);
            assertThat(kline.closed()).isTrue();
        });
    }

    private static MarketTrade trade(
            MarketId market,
            String eventId,
            String price,
            String baseAmount,
            String quoteAmount,
            long timestamp) {
        return new MarketTrade(
                market,
                eventId,
                new BigDecimal(price),
                new BigDecimal(baseAmount),
                new BigDecimal(quoteAmount),
                TradeSide.BUY,
                timestamp);
    }
}
