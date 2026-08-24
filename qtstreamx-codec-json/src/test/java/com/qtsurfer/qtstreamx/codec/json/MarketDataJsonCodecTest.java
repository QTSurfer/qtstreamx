package com.qtsurfer.qtstreamx.codec.json;

import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.model.Kline;
import com.qtsurfer.qtstreamx.core.model.MarketId;
import com.qtsurfer.qtstreamx.core.model.MarketKline;
import com.qtsurfer.qtstreamx.core.model.MarketTicker;
import com.qtsurfer.qtstreamx.core.model.MarketTrade;
import com.qtsurfer.qtstreamx.core.model.Ticker;
import com.qtsurfer.qtstreamx.core.model.TradeSide;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

class MarketDataJsonCodecTest {

    private static final Instrument ETH_USDC = new Instrument("ETH", "USDC");
    private static final MarketId MARKET =
            new MarketId("uniswap-v3", "eip155:1", "0xpool", ETH_USDC);

    @Test
    void roundTripsMarketTradeWithoutLosingDecimalScaleOrTimestamp() {
        MarketTrade value = new MarketTrade(
                MARKET,
                "eip155:1/0xblock/0xtx/7",
                new BigDecimal("3125.250000"),
                new BigDecimal("1.200000000000000000"),
                new BigDecimal("3750.300000"),
                TradeSide.BUY,
                1_700_000_000_123_456L);

        assertThat(roundTrip(value, MarketTrade.class)).isEqualTo(value);
    }

    @Test
    void roundTripsConcreteTickerAndKlineWrappers() {
        Ticker ticker = new Ticker(
                ETH_USDC, null, null, null, null, new BigDecimal("3125.250000"),
                null, null, null, null, null, 1_700_000_000_123_456L);
        BigDecimal price = new BigDecimal("3125.250000");
        Kline kline = new Kline(
                ETH_USDC, "1m", price, price, price, price,
                new BigDecimal("1.200000000000000000"),
                new BigDecimal("3750.300000"), 1, true,
                1_700_000_000_000_000L, 1_700_000_059_999_999L);

        MarketTicker marketTicker = new MarketTicker(MARKET, ticker);
        MarketKline marketKline = new MarketKline(MARKET, kline);

        assertThat(roundTrip(marketTicker, MarketTicker.class)).isEqualTo(marketTicker);
        assertThat(roundTrip(marketKline, MarketKline.class)).isEqualTo(marketKline);
    }

    @Test
    void serializesSameInstrumentPoolsWithDistinctMarketIdentity() {
        JsonCodec<MarketId> codec = new JsonCodec<>();
        MarketId otherPool = new MarketId("uniswap-v3", "eip155:1", "0xother", ETH_USDC);

        assertThat(codec.encode(MARKET)).isNotEqualTo(codec.encode(otherPool));
        assertThat(roundTrip(otherPool, MarketId.class)).isEqualTo(otherPool);
    }

    private static <T> T roundTrip(T value, Class<T> type) {
        JsonCodec<T> codec = new JsonCodec<>();
        return codec.decode(codec.encode(value), type);
    }
}
