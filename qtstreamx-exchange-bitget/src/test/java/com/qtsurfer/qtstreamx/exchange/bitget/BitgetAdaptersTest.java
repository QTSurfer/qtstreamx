package com.qtsurfer.qtstreamx.exchange.bitget;

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

class BitgetAdaptersTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Instrument BTC_USDT_SPOT = new Instrument("BTC", "USDT", null);
    private static final Instrument BTC_USDT_PERP = new Instrument("BTC", "USDT", "USDT");

    @Test
    void tickerMapsAllFieldsForSpot() throws Exception {
        JsonNode root = loadFixture("bitget-ticker-spot.json");
        Ticker ticker = BitgetAdapters.adaptTicker(BTC_USDT_SPOT, root.path("data").get(0));

        assertThat(ticker.bid()).isEqualByComparingTo("65009.0");
        assertThat(ticker.bidSize()).isEqualByComparingTo("1.234");
        assertThat(ticker.ask()).isEqualByComparingTo("65011.0");
        assertThat(ticker.askSize()).isEqualByComparingTo("2.345");
        assertThat(ticker.last()).isEqualByComparingTo("65010.5");
        assertThat(ticker.open()).isEqualByComparingTo("64200.0");
        assertThat(ticker.high()).isEqualByComparingTo("65200.0");
        assertThat(ticker.low()).isEqualByComparingTo("64000.0");
        assertThat(ticker.volume()).isEqualByComparingTo("1234.56");
        assertThat(ticker.quoteVolume()).isEqualByComparingTo("80234000.12");
        assertThat(ticker.timestamp()).isEqualTo(1713715200123L * 1_000L);
    }

    @Test
    void fundingRateFromFuturesTicker() throws Exception {
        JsonNode root = loadFixture("bitget-ticker-futures.json");
        FundingRate fr =
                BitgetAdapters.adaptFundingRate(BTC_USDT_PERP, root.path("data").get(0));

        assertThat(fr).isNotNull();
        assertThat(fr.rate()).isEqualByComparingTo("0.00009876");
        assertThat(fr.markPrice()).isEqualByComparingTo("65010.0");
        assertThat(fr.intervalHours()).isEqualTo(8);
        assertThat(fr.nextFundingTime()).isEqualTo(1713744000000L * 1_000L);
    }

    @Test
    void fundingRateNullWhenMissing() throws Exception {
        JsonNode root = loadFixture("bitget-ticker-spot.json");
        FundingRate fr =
                BitgetAdapters.adaptFundingRate(BTC_USDT_PERP, root.path("data").get(0));
        assertThat(fr).isNull();
    }

    @Test
    void klineSynthesizesCloseTime() throws Exception {
        JsonNode root = loadFixture("bitget-candle.json");
        Kline kline =
                BitgetAdapters.adaptKline(BTC_USDT_SPOT, "1m", root.path("data").get(0));

        assertThat(kline).isNotNull();
        assertThat(kline.open()).isEqualByComparingTo("65000.0");
        assertThat(kline.close()).isEqualByComparingTo("65040.0");
        assertThat(kline.volume()).isEqualByComparingTo("12.34");
        assertThat(kline.quoteVolume()).isEqualByComparingTo("802345.0");
        assertThat(kline.timestamp()).isEqualTo(1713715140000L * 1_000L);
        assertThat(kline.closeTime()).isEqualTo((1713715140000L + 60_000L) * 1_000L - 1_000L);
    }

    @Test
    void klineRejectsShortArray() {
        JsonNode short1 = MAPPER.createArrayNode().add("1").add("2");
        assertThat(BitgetAdapters.adaptKline(BTC_USDT_SPOT, "1m", short1)).isNull();
    }

    private static JsonNode loadFixture(String name) throws Exception {
        try (InputStream in = BitgetAdaptersTest.class.getResourceAsStream("/" + name)) {
            Objects.requireNonNull(in, name);
            return MAPPER.readTree(in);
        }
    }
}
