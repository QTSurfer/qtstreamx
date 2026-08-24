package com.qtsurfer.qtstreamx.core.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

class MarketDataContractTest {

    private static final Instrument ETH_USDC = new Instrument("ETH", "USDC");

    @Test
    void keepsPoolsForTheSameInstrumentDistinct() {
        MarketId first = new MarketId("uniswap-v3", "eip155:1", "0xpool-a", ETH_USDC);
        MarketId second = new MarketId("uniswap-v3", "eip155:1", "0xpool-b", ETH_USDC);

        assertThat(first).isNotEqualTo(second);
        assertThat(first.instrument()).isEqualTo(second.instrument());
    }

    @Test
    void keepsEquivalentMarketsOnDifferentNetworksDistinct() {
        MarketId ethereum = new MarketId("uniswap-v3", "eip155:1", "0xpool", ETH_USDC);
        MarketId robinhood = new MarketId("uniswap-v3", "eip155:4663", "0xpool", ETH_USDC);

        assertThat(ethereum).isNotEqualTo(robinhood);
        assertThat(ethereum.instrument()).isEqualTo(robinhood.instrument());
    }

    @Test
    void rejectsIncompleteMarketIdentity() {
        assertThatThrownBy(() -> new MarketId(" ", "eip155:1", "0xpool", ETH_USDC))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("venue");
        assertThatThrownBy(() -> new MarketId("uniswap-v3", "", "0xpool", ETH_USDC))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("network");
        assertThatThrownBy(() -> new MarketId("uniswap-v3", "eip155:1", "", ETH_USDC))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nativeId");
    }

    @Test
    void modelsNormalizedTradeFromBaseAssetPerspective() {
        MarketId market = new MarketId("uniswap-v3", "eip155:1", "0xpool", ETH_USDC);
        MarketTrade trade = new MarketTrade(
                market,
                "eip155:1/0xblock/0xtx/7",
                new BigDecimal("3125.250000"),
                new BigDecimal("1.200000000000000000"),
                new BigDecimal("3750.300000"),
                TradeSide.BUY,
                1_700_000_000_123_456L);

        assertThat(trade.side()).isEqualTo(TradeSide.BUY);
        assertThat(trade.price()).isEqualByComparingTo("3125.250000");
        assertThat(trade.timestamp()).isEqualTo(1_700_000_000_123_456L);
    }

    @Test
    void rejectsInvalidTradeValues() {
        MarketId market = new MarketId("uniswap-v3", "eip155:1", "0xpool", ETH_USDC);

        assertThatThrownBy(() -> new MarketTrade(
                market, "event", BigDecimal.ZERO, BigDecimal.ONE, BigDecimal.ONE,
                TradeSide.SELL, 1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("price");
        assertThatThrownBy(() -> new MarketTrade(
                market, " ", BigDecimal.ONE, BigDecimal.ONE, BigDecimal.ONE,
                TradeSide.SELL, 1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("eventId");
    }

    @Test
    void requiresTickerAndKlineInstrumentToMatchMarket() {
        MarketId market = new MarketId("uniswap-v3", "eip155:1", "0xpool", ETH_USDC);
        Ticker ticker = ticker(new Instrument("BTC", "USDC"));
        Kline kline = kline(new Instrument("BTC", "USDC"));

        assertThatThrownBy(() -> new MarketTicker(market, ticker))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("instrument");
        assertThatThrownBy(() -> new MarketKline(market, kline))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("instrument");
    }

    private static Ticker ticker(Instrument instrument) {
        return new Ticker(
                instrument, null, null, null, null, new BigDecimal("3125.25"),
                null, null, null, null, null, 1_700_000_000_123_456L);
    }

    private static Kline kline(Instrument instrument) {
        BigDecimal price = new BigDecimal("3125.25");
        return new Kline(
                instrument, "1m", price, price, price, price,
                BigDecimal.ONE, price, 1, false,
                1_700_000_000_000_000L, 1_700_000_059_999_999L);
    }
}
