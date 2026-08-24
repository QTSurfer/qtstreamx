package com.qtsurfer.qtstreamx.exchange.bybit;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.qtsurfer.qtstreamx.core.model.FundingRate;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.model.Kline;
import com.qtsurfer.qtstreamx.core.model.Ticker;
import java.io.InputStream;
import java.util.Objects;
import org.junit.jupiter.api.Test;

/**
 * Adapter tests use canned Bybit v5 payloads captured from the public API docs. They verify the
 * field mapping only — live-WS behaviour (subscribe/routing/reconnect) is covered by
 * {@code BybitStreamClientTest}.
 */
class BybitAdaptersTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Instrument BTC_USDT_SPOT = new Instrument("BTC", "USDT", null);
    private static final Instrument BTC_USDT_PERP = new Instrument("BTC", "USDT", "USDT");

    @Test
    void adaptSpotTickerHasNoBboButCarriesOhlcAndVolume() throws Exception {
        JsonNode root = loadFixture("bybit-ticker-spot.json");
        Ticker ticker =
                BybitAdapters.adaptTickerSpot(BTC_USDT_SPOT, root.get("data"), root.get("ts").asLong());

        assertThat(ticker.instrument().symbol()).isEqualTo("BTC/USDT");
        assertThat(ticker.bid()).isNull();
        assertThat(ticker.ask()).isNull();
        assertThat(ticker.last()).isEqualByComparingTo("21109.77");
        assertThat(ticker.open()).isEqualByComparingTo("20704.93"); // prevPrice24h
        assertThat(ticker.high()).isEqualByComparingTo("21426.99");
        assertThat(ticker.low()).isEqualByComparingTo("20575");
        assertThat(ticker.volume()).isEqualByComparingTo("6780.866843");
        assertThat(ticker.quoteVolume()).isEqualByComparingTo("141946527.22907118");
        assertThat(ticker.timestamp()).isEqualTo(1673853746003L * 1_000L);
    }

    @Test
    void adaptLinearTickerFillsEveryField() throws Exception {
        JsonNode root = loadFixture("bybit-ticker-linear.json");
        Ticker ticker =
                BybitAdapters.adaptTickerLinear(BTC_USDT_PERP, root.get("data"), root.get("ts").asLong());

        assertThat(ticker.instrument().symbol()).isEqualTo("BTC/USDT:USDT");
        assertThat(ticker.bid()).isEqualByComparingTo("17215.50");
        assertThat(ticker.bidSize()).isEqualByComparingTo("84.489");
        assertThat(ticker.ask()).isEqualByComparingTo("17216.00");
        assertThat(ticker.askSize()).isEqualByComparingTo("83.020");
        assertThat(ticker.last()).isEqualByComparingTo("17216.00");
        assertThat(ticker.open()).isEqualByComparingTo("16926.50");
        assertThat(ticker.high()).isEqualByComparingTo("17281.50");
        assertThat(ticker.low()).isEqualByComparingTo("16915.00");
        assertThat(ticker.volume()).isEqualByComparingTo("91705.276");
        assertThat(ticker.quoteVolume()).isEqualByComparingTo("1570383121.943499");
        assertThat(ticker.timestamp()).isEqualTo(1673853746003L * 1_000L);
    }

    @Test
    void adaptFundingRateFromLinearTickerUsesNegativeRate() throws Exception {
        JsonNode root = loadFixture("bybit-ticker-linear.json");
        FundingRate fr =
                BybitAdapters.adaptFundingRate(
                        BTC_USDT_PERP, root.get("data"), /* intervalHours */ 8, root.get("ts").asLong());

        assertThat(fr).isNotNull();
        assertThat(fr.rate()).isEqualByComparingTo("-0.000212");
        assertThat(fr.markPrice()).isEqualByComparingTo("17217.33");
        assertThat(fr.nextFundingTime()).isEqualTo(1673852400000L * 1_000L);
        assertThat(fr.intervalHours()).isEqualTo(8);
        assertThat(fr.timestamp()).isEqualTo(1673853746003L * 1_000L);
    }

    @Test
    void adaptFundingRateReturnsNullWhenRateAbsent() throws Exception {
        // Spot ticker has no fundingRate field — adapter must bail out rather than publish a
        // zero-rate we'd have to distinguish from a real zero.
        JsonNode root = loadFixture("bybit-ticker-spot.json");
        FundingRate fr =
                BybitAdapters.adaptFundingRate(
                        BTC_USDT_PERP, root.get("data"), 8, root.get("ts").asLong());

        assertThat(fr).isNull();
    }

    @Test
    void adaptKlineUsesStartTimestampAndEndClose() throws Exception {
        JsonNode root = loadFixture("bybit-kline.json");
        Kline kline = BybitAdapters.adaptKline(BTC_USDT_SPOT, root.get("data").get(0));

        assertThat(kline.instrument().symbol()).isEqualTo("BTC/USDT");
        assertThat(kline.interval()).isEqualTo("1");
        assertThat(kline.open()).isEqualByComparingTo("16649.5");
        assertThat(kline.high()).isEqualByComparingTo("16677");
        assertThat(kline.low()).isEqualByComparingTo("16608");
        assertThat(kline.close()).isEqualByComparingTo("16677");
        assertThat(kline.volume()).isEqualByComparingTo("2.081");
        assertThat(kline.quoteVolume()).isEqualByComparingTo("34666.4005");
        assertThat(kline.closed()).isFalse();
        // Bybit reports start/end in ms; we upscale to µs.
        assertThat(kline.timestamp()).isEqualTo(1672324800000L * 1_000L);
        assertThat(kline.closeTime()).isEqualTo(1672324859999L * 1_000L);
    }

    private JsonNode loadFixture(String name) throws Exception {
        try (InputStream in = getClass().getResourceAsStream("/" + name)) {
            Objects.requireNonNull(in, name + " missing from classpath");
            return MAPPER.readTree(in);
        }
    }
}
