package com.qtsurfer.qtstreamx.exchange.okx;

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

class OkxAdaptersTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Instrument BTC_USDT_SPOT = new Instrument("BTC", "USDT", null);
    private static final Instrument BTC_USDT_SWAP = new Instrument("BTC", "USDT", "USDT");

    @Test
    void adaptTickerMapsAllFields() throws Exception {
        JsonNode root = loadFixture("okx-ticker-spot.json");
        Ticker ticker = OkxAdapters.adaptTicker(BTC_USDT_SPOT, root.get("data").get(0));

        assertThat(ticker.instrument().symbol()).isEqualTo("BTC/USDT");
        assertThat(ticker.bid()).isEqualByComparingTo("9999.98");
        assertThat(ticker.bidSize()).isEqualByComparingTo("20");
        assertThat(ticker.ask()).isEqualByComparingTo("10000.00");
        assertThat(ticker.askSize()).isEqualByComparingTo("11");
        assertThat(ticker.last()).isEqualByComparingTo("9999.99");
        assertThat(ticker.open()).isEqualByComparingTo("9000");
        assertThat(ticker.high()).isEqualByComparingTo("10000.55");
        assertThat(ticker.low()).isEqualByComparingTo("8888.88");
        assertThat(ticker.volume()).isEqualByComparingTo("2222.5");
        assertThat(ticker.quoteVolume()).isEqualByComparingTo("22200000");
        assertThat(ticker.timestamp()).isEqualTo(1597026383085L * 1_000L);
    }

    @Test
    void adaptFundingRateUsesNextFundingTimeAs8hDefault() throws Exception {
        JsonNode root = loadFixture("okx-funding-rate.json");
        FundingRate fr =
                OkxAdapters.adaptFundingRate(BTC_USDT_SWAP, root.get("data").get(0));

        assertThat(fr).isNotNull();
        assertThat(fr.rate()).isEqualByComparingTo("0.0012");
        assertThat(fr.nextFundingTime()).isEqualTo(1597029983085L * 1_000L);
        assertThat(fr.intervalHours()).isEqualTo(8);
        assertThat(fr.markPrice()).isNull(); // OKX funding-rate channel carries no mark price
        assertThat(fr.timestamp()).isEqualTo(1597026380000L * 1_000L);
    }

    @Test
    void adaptFundingRateReturnsNullWhenRateMissing() {
        JsonNode stub = MAPPER.createObjectNode().put("ts", "1");
        assertThat(OkxAdapters.adaptFundingRate(BTC_USDT_SWAP, stub)).isNull();
    }

    @Test
    void adaptKlineSynthesisesCloseTimeFromInterval() throws Exception {
        JsonNode root = loadFixture("okx-kline.json");
        Kline kline =
                OkxAdapters.adaptKline(BTC_USDT_SPOT, "1s", root.get("data").get(0));

        assertThat(kline).isNotNull();
        assertThat(kline.instrument().symbol()).isEqualTo("BTC/USDT");
        assertThat(kline.interval()).isEqualTo("1s");
        assertThat(kline.open()).isEqualByComparingTo("9999.99");
        assertThat(kline.high()).isEqualByComparingTo("10001.50");
        assertThat(kline.low()).isEqualByComparingTo("9998.00");
        assertThat(kline.close()).isEqualByComparingTo("10000.25");
        assertThat(kline.volume()).isEqualByComparingTo("200.5");
        assertThat(kline.quoteVolume()).isEqualByComparingTo("2.005");
        assertThat(kline.closed()).isFalse(); // confirm=0
        // OKX reports the candle open timestamp only. closeTime = open + 1s - 1ms.
        assertThat(kline.timestamp()).isEqualTo(1597026383000L * 1_000L);
        assertThat(kline.closeTime()).isEqualTo((1597026383000L + 1_000L - 1L) * 1_000L);
    }

    @Test
    void adaptKlineRejectsShortArray() {
        JsonNode short1 = MAPPER.createArrayNode().add("1").add("2");
        assertThat(OkxAdapters.adaptKline(BTC_USDT_SPOT, "1m", short1)).isNull();
    }

    private JsonNode loadFixture(String name) throws Exception {
        try (InputStream in = getClass().getResourceAsStream("/" + name)) {
            Objects.requireNonNull(in, name + " missing from classpath");
            return MAPPER.readTree(in);
        }
    }
}
