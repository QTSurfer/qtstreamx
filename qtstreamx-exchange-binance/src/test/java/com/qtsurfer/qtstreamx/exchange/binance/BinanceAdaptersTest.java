package com.qtsurfer.qtstreamx.exchange.binance;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.qtsurfer.qtstreamx.core.model.FundingRate;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.model.Kline;
import com.qtsurfer.qtstreamx.core.model.Ticker;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

class BinanceAdaptersTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Instrument BTC_USDT_SPOT = new Instrument("BTC", "USDT");
    private static final Instrument BTC_USDT_PERP = new Instrument("BTC", "USDT", "USDT");

    private JsonNode loadFixture(String name) throws Exception {
        return MAPPER.readTree(getClass().getClassLoader().getResourceAsStream(name));
    }

    @Test
    void adaptBookTickerSpot() throws Exception {
        JsonNode root = loadFixture("binance-bookTicker.json");
        Ticker ticker = BinanceAdapters.adaptBookTicker(BTC_USDT_SPOT, root.get("data"));

        assertThat(ticker.instrument().symbol()).isEqualTo("BTC/USDT");
        assertThat(ticker.instrument().isDerivative()).isFalse();
        assertThat(ticker.bid()).isEqualByComparingTo(new BigDecimal("70123.00"));
        assertThat(ticker.bidSize()).isEqualByComparingTo(new BigDecimal("1.500"));
        assertThat(ticker.ask()).isEqualByComparingTo(new BigDecimal("70124.00"));
        assertThat(ticker.askSize()).isEqualByComparingTo(new BigDecimal("2.300"));
        assertThat(ticker.last()).isNull();
        assertThat(ticker.open()).isNull();
        assertThat(ticker.timestamp()).isGreaterThan(0);
    }

    @Test
    void adaptBookTickerFutures() throws Exception {
        JsonNode root = loadFixture("binance-bookTicker.json");
        Ticker ticker = BinanceAdapters.adaptBookTicker(BTC_USDT_PERP, root.get("data"));

        assertThat(ticker.instrument().symbol()).isEqualTo("BTC/USDT:USDT");
        assertThat(ticker.instrument().isDerivative()).isTrue();
    }

    @Test
    void adaptBookTickerExoticQuote() throws Exception {
        // Regression: HEI/TRY — previously misparsed as (HEITRY, UNKNOWN)
        // by the deleted toSpotInstrument heuristic. Adapter now trusts the
        // caller's Instrument, so the quote is preserved correctly.
        JsonNode root = loadFixture("binance-bookTicker.json");
        Instrument heiTry = new Instrument("HEI", "TRY");
        Ticker ticker = BinanceAdapters.adaptBookTicker(heiTry, root.get("data"));

        assertThat(ticker.instrument().symbol()).isEqualTo("HEI/TRY");
        assertThat(ticker.instrument().quote()).isEqualTo("TRY");
    }

    @Test
    void adaptTicker24h() throws Exception {
        JsonNode root = loadFixture("binance-ticker24h.json");
        Ticker ticker = BinanceAdapters.adaptTicker24h(BTC_USDT_SPOT, root.get("data"));

        assertThat(ticker.instrument().symbol()).isEqualTo("BTC/USDT");
        assertThat(ticker.bid()).isEqualByComparingTo("70123.00");
        assertThat(ticker.ask()).isEqualByComparingTo("70124.00");
        assertThat(ticker.last()).isEqualByComparingTo("70123.50");
        assertThat(ticker.open()).isEqualByComparingTo("69500.00");
        assertThat(ticker.high()).isEqualByComparingTo("70500.00");
        assertThat(ticker.low()).isEqualByComparingTo("69200.00");
        assertThat(ticker.volume()).isEqualByComparingTo("12345.670");
        assertThat(ticker.quoteVolume()).isEqualByComparingTo("865432100.00");
        assertThat(ticker.timestamp()).isEqualTo(1711238400123L * 1000L);
    }

    @Test
    void adaptKline() throws Exception {
        JsonNode root = loadFixture("binance-kline.json");
        Kline kline = BinanceAdapters.adaptKline(BTC_USDT_SPOT, root.get("data"));

        assertThat(kline.instrument().symbol()).isEqualTo("BTC/USDT");
        assertThat(kline.interval()).isEqualTo("1m");
        assertThat(kline.open()).isEqualByComparingTo("16850.50");
        assertThat(kline.high()).isEqualByComparingTo("16855.00");
        assertThat(kline.low()).isEqualByComparingTo("16849.10");
        assertThat(kline.close()).isEqualByComparingTo("16852.30");
        assertThat(kline.volume()).isEqualByComparingTo("23.456");
        assertThat(kline.quoteVolume()).isEqualByComparingTo("395123.45");
        assertThat(kline.closed()).isTrue();
        assertThat(kline.timestamp()).isEqualTo(1672515780000L * 1000L);
    }

    @Test
    void adaptKline1s() throws Exception {
        JsonNode root = loadFixture("binance-kline-1s.json");
        Kline kline = BinanceAdapters.adaptKline(BTC_USDT_SPOT, root.get("data"));

        assertThat(kline.interval()).isEqualTo("1s");
        assertThat(kline.closed()).isFalse();
        assertThat(kline.volume()).isEqualByComparingTo("0.456");
    }

    @Test
    void adaptKlineFutures() throws Exception {
        JsonNode root = loadFixture("binance-kline.json");
        Kline kline = BinanceAdapters.adaptKline(BTC_USDT_PERP, root.get("data"));

        assertThat(kline.instrument().symbol()).isEqualTo("BTC/USDT:USDT");
        assertThat(kline.instrument().isDerivative()).isTrue();
    }

    @Test
    void adaptMarkPrice() throws Exception {
        JsonNode root = loadFixture("binance-markPrice.json");
        FundingRate fr = BinanceAdapters.adaptMarkPrice(BTC_USDT_PERP, root.get("data"));

        assertThat(fr.instrument().symbol()).isEqualTo("BTC/USDT:USDT");
        assertThat(fr.rate()).isEqualByComparingTo("0.00010000");
        assertThat(fr.markPrice()).isEqualByComparingTo("70122.50");
        assertThat(fr.nextFundingTime()).isEqualTo(1711267200000L * 1000L);
        assertThat(fr.intervalHours()).isEqualTo(8);
        assertThat(fr.timestamp()).isEqualTo(1711238400123L * 1000L);
    }
}
