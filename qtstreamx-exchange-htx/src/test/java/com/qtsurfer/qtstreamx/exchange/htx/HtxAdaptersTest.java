package com.qtsurfer.qtstreamx.exchange.htx;

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

class HtxAdaptersTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Instrument BTC_USDT_SPOT = new Instrument("BTC", "USDT", null);
    private static final Instrument BTC_USDT_PERP = new Instrument("BTC", "USDT", "USDT");

    @Test
    void spotDetailMapsCorrectFields() throws Exception {
        JsonNode root = loadFixture("htx-ticker-spot.json");
        long ts = root.path("ts").asLong();
        Ticker ticker = HtxAdapters.adaptSpotDetail(BTC_USDT_SPOT, root.path("tick"), ts);

        assertThat(ticker.bid()).isNull();
        assertThat(ticker.ask()).isNull();
        assertThat(ticker.last()).isEqualByComparingTo("65010.5");
        assertThat(ticker.open()).isEqualByComparingTo("64200.0");
        assertThat(ticker.high()).isEqualByComparingTo("65200.0");
        assertThat(ticker.low()).isEqualByComparingTo("64000.0");
        // amount=base, vol=quote (HTX convention)
        assertThat(ticker.volume()).isEqualByComparingTo("1234.56");
        assertThat(ticker.quoteVolume()).isEqualByComparingTo("80234000.12");
        assertThat(ticker.timestamp()).isEqualTo(1713715200123L * 1_000L);
    }

    @Test
    void linearDetailMapsCorrectFields() throws Exception {
        JsonNode root = loadFixture("htx-ticker-linear.json");
        long ts = root.path("ts").asLong();
        Ticker ticker = HtxAdapters.adaptLinearDetail(BTC_USDT_PERP, root.path("tick"), ts);

        assertThat(ticker.last()).isEqualByComparingTo("65020.5");
        assertThat(ticker.volume()).isEqualByComparingTo("4321.0");
        assertThat(ticker.quoteVolume()).isEqualByComparingTo("281234000.0");
        assertThat(ticker.timestamp()).isEqualTo(1713715200456L * 1_000L);
    }

    @Test
    void klineSynthesizesCloseTime() throws Exception {
        JsonNode root = loadFixture("htx-kline.json");
        Kline kline = HtxAdapters.adaptKline(BTC_USDT_SPOT, "1min", root.path("tick"));

        assertThat(kline.open()).isEqualByComparingTo("65000.0");
        assertThat(kline.close()).isEqualByComparingTo("65040.0");
        assertThat(kline.volume()).isEqualByComparingTo("12.34");
        assertThat(kline.quoteVolume()).isEqualByComparingTo("802345.0");
        assertThat(kline.numberOfTrades()).isEqualTo(77);
        // id=1713715140 (seconds) → startMs=1713715140000, interval=60_000
        assertThat(kline.timestamp()).isEqualTo(1713715140000L * 1_000L);
        assertThat(kline.closeTime()).isEqualTo((1713715140000L + 60_000L) * 1_000L - 1_000L);
    }

    @Test
    void fundingRateFromNotifyPayload() throws Exception {
        JsonNode root = loadFixture("htx-funding-rate.json");
        long ts = root.path("ts").asLong();
        JsonNode payload = root.path("data").get(0);
        FundingRate fr = HtxAdapters.adaptFundingRate(BTC_USDT_PERP, payload, ts);

        assertThat(fr).isNotNull();
        assertThat(fr.rate()).isEqualByComparingTo("0.00009876");
        assertThat(fr.markPrice()).isNull();
        assertThat(fr.intervalHours()).isEqualTo(8);
        // next_funding_time preferred over funding_time
        assertThat(fr.nextFundingTime()).isEqualTo(1713744000000L * 1_000L);
        assertThat(fr.timestamp()).isEqualTo(1713715200123L * 1_000L);
    }

    @Test
    void fundingRateFallsBackToFundingTimeWhenNextAbsent() {
        com.fasterxml.jackson.databind.node.ObjectNode payload = MAPPER.createObjectNode();
        payload.put("funding_rate", "0.0001");
        payload.put("funding_time", "1713715200000");
        FundingRate fr = HtxAdapters.adaptFundingRate(BTC_USDT_PERP, payload, 1713715200123L);
        assertThat(fr).isNotNull();
        assertThat(fr.nextFundingTime()).isEqualTo(1713715200000L * 1_000L);
    }

    @Test
    void fundingRateNullWhenRateMissing() {
        com.fasterxml.jackson.databind.node.ObjectNode payload = MAPPER.createObjectNode();
        payload.put("next_funding_time", "1713744000000");
        FundingRate fr = HtxAdapters.adaptFundingRate(BTC_USDT_PERP, payload, 1713715200123L);
        assertThat(fr).isNull();
    }

    private static JsonNode loadFixture(String name) throws Exception {
        try (InputStream in = HtxAdaptersTest.class.getResourceAsStream("/" + name)) {
            Objects.requireNonNull(in, name);
            return MAPPER.readTree(in);
        }
    }
}
