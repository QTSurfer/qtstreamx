package com.qtsurfer.qtstreamx.exchange.kraken;

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

class KrakenAdaptersTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Instrument BTC_USD_SPOT = new Instrument("BTC", "USD", null);
    private static final Instrument BTC_USD_PERP = new Instrument("BTC", "USD", "USD");

    @Test
    void spotTickerHasBboAndOhlcWithoutOpenOrQuoteVolume() throws Exception {
        JsonNode root = loadFixture("kraken-ticker-spot.json");
        long tsMs = KrakenAdapters.parseIsoMs(root.path("timestamp").asText());
        Ticker ticker = KrakenAdapters.adaptTickerSpot(BTC_USD_SPOT, root.path("data").get(0), tsMs);

        assertThat(ticker.bid()).isEqualByComparingTo("65000.0");
        assertThat(ticker.bidSize()).isEqualByComparingTo("0.5");
        assertThat(ticker.ask()).isEqualByComparingTo("65001.5");
        assertThat(ticker.askSize()).isEqualByComparingTo("0.75");
        assertThat(ticker.last()).isEqualByComparingTo("65000.5");
        assertThat(ticker.open()).isNull();
        assertThat(ticker.high()).isEqualByComparingTo("65100.0");
        assertThat(ticker.low()).isEqualByComparingTo("64500.0");
        assertThat(ticker.volume()).isEqualByComparingTo("1234.56");
        assertThat(ticker.quoteVolume()).isNull();
        assertThat(ticker.timestamp()).isEqualTo(tsMs * 1_000L);
    }

    @Test
    void spotOhlcSynthesizesCloseTimeFromIntervalMinutes() throws Exception {
        JsonNode root = loadFixture("kraken-ohlc-spot.json");
        Kline kline = KrakenAdapters.adaptKlineSpot(BTC_USD_SPOT, "1m", root.path("data").get(0));

        assertThat(kline.interval()).isEqualTo("1m");
        assertThat(kline.open()).isEqualByComparingTo("64980.5");
        assertThat(kline.high()).isEqualByComparingTo("65020.0");
        assertThat(kline.low()).isEqualByComparingTo("64975.0");
        assertThat(kline.close()).isEqualByComparingTo("65010.25");
        assertThat(kline.volume()).isEqualByComparingTo("3.4567");
        assertThat(kline.quoteVolume()).isNull();
        assertThat(kline.numberOfTrades()).isEqualTo(52L);
        assertThat(kline.closed()).isTrue();
        long openMs = KrakenAdapters.parseIsoMs("2026-04-21T16:00:00.000000Z");
        assertThat(kline.timestamp()).isEqualTo(openMs * 1_000L);
        assertThat(kline.closeTime()).isEqualTo((openMs + 60_000L) * 1_000L - 1_000L);
    }

    @Test
    void futuresTickerFillsBboPlusHighLow() throws Exception {
        JsonNode root = loadFixture("kraken-ticker-futures.json");
        Ticker ticker = KrakenAdapters.adaptTickerFutures(BTC_USD_PERP, root);

        assertThat(ticker.bid()).isEqualByComparingTo("65000.0");
        assertThat(ticker.ask()).isEqualByComparingTo("65001.5");
        assertThat(ticker.last()).isEqualByComparingTo("65000.5");
        assertThat(ticker.high()).isEqualByComparingTo("65500.0");
        assertThat(ticker.low()).isEqualByComparingTo("64000.0");
        assertThat(ticker.volume()).isEqualByComparingTo("5678.9");
        assertThat(ticker.timestamp()).isEqualTo(1713715200123L * 1_000L);
    }

    @Test
    void fundingRateUsesHourlyInterval() throws Exception {
        JsonNode root = loadFixture("kraken-ticker-futures.json");
        FundingRate fr = KrakenAdapters.adaptFundingRate(BTC_USD_PERP, root);

        assertThat(fr).isNotNull();
        assertThat(fr.rate()).isEqualByComparingTo("0.00001235");
        assertThat(fr.markPrice()).isEqualByComparingTo("65000.25");
        assertThat(fr.nextFundingTime()).isEqualTo(1713718800000L * 1_000L);
        assertThat(fr.intervalHours()).isEqualTo(1);
    }

    @Test
    void fundingRateReturnsNullWhenFieldAbsent() throws Exception {
        JsonNode stub = MAPPER.createObjectNode().put("time", 1);
        assertThat(KrakenAdapters.adaptFundingRate(BTC_USD_PERP, stub)).isNull();
    }

    @Test
    void parseIsoMsHandlesMicros() {
        assertThat(KrakenAdapters.parseIsoMs("2026-04-21T16:00:00.000000Z"))
                .isEqualTo(KrakenAdapters.parseIsoMs("2026-04-21T16:00:00Z"));
        assertThat(KrakenAdapters.parseIsoMs("not-a-timestamp")).isZero();
        assertThat(KrakenAdapters.parseIsoMs("")).isZero();
    }

    private static JsonNode loadFixture(String name) throws Exception {
        try (InputStream in = KrakenAdaptersTest.class.getResourceAsStream("/" + name)) {
            Objects.requireNonNull(in, name + " missing from classpath");
            return MAPPER.readTree(in);
        }
    }
}
