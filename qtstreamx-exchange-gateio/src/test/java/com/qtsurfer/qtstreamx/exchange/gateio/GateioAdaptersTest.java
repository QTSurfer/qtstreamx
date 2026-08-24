package com.qtsurfer.qtstreamx.exchange.gateio;

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

class GateioAdaptersTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Instrument BTC_USDT_SPOT = new Instrument("BTC", "USDT", null);
    private static final Instrument BTC_USDT_PERP = new Instrument("BTC", "USDT", "USDT");

    @Test
    void spotTickerHasBidAskButNoSizesOrOpen() throws Exception {
        JsonNode root = loadFixture("gateio-ticker-spot.json");
        long tsMs = root.path("time_ms").asLong();
        Ticker ticker =
                GateioAdapters.adaptSpotTicker(BTC_USDT_SPOT, root.path("result"), tsMs);
        assertThat(ticker.bid()).isEqualByComparingTo("65009.0");
        assertThat(ticker.bidSize()).isNull();
        assertThat(ticker.ask()).isEqualByComparingTo("65011.0");
        assertThat(ticker.askSize()).isNull();
        assertThat(ticker.last()).isEqualByComparingTo("65010.5");
        assertThat(ticker.open()).isNull();
        assertThat(ticker.high()).isEqualByComparingTo("65200.0");
        assertThat(ticker.low()).isEqualByComparingTo("64000.0");
        assertThat(ticker.volume()).isEqualByComparingTo("1234.56");
        assertThat(ticker.quoteVolume()).isEqualByComparingTo("80234000.12");
        assertThat(ticker.timestamp()).isEqualTo(tsMs * 1_000L);
    }

    @Test
    void spotKlineSynthesisesCloseTime() throws Exception {
        JsonNode root = loadFixture("gateio-candlestick-spot.json");
        Kline kline =
                GateioAdapters.adaptSpotKline(BTC_USDT_SPOT, "1m", root.path("result"));
        assertThat(kline.close()).isEqualByComparingTo("65040.0");
        assertThat(kline.volume()).isEqualByComparingTo("12.34");
        assertThat(kline.quoteVolume()).isEqualByComparingTo("802345.0");
        // Gate.io emits seconds-since-epoch for 't'; our record stores µs.
        long expectedOpenMs = 1713715140L * 1_000L;
        assertThat(kline.timestamp()).isEqualTo(expectedOpenMs * 1_000L);
        assertThat(kline.closeTime())
                .isEqualTo((expectedOpenMs + 60_000L) * 1_000L - 1_000L);
    }

    @Test
    void futuresTickerAndFundingShareMessage() throws Exception {
        JsonNode root = loadFixture("gateio-ticker-futures.json");
        long tsMs = root.path("time_ms").asLong();
        JsonNode entry = root.path("result").get(0);
        Ticker ticker = GateioAdapters.adaptFuturesTicker(BTC_USDT_PERP, entry, tsMs);
        assertThat(ticker.last()).isEqualByComparingTo("65010.5");
        assertThat(ticker.high()).isEqualByComparingTo("65200.0");
        assertThat(ticker.low()).isEqualByComparingTo("64000.0");
        assertThat(ticker.volume()).isEqualByComparingTo("1234.56");
        assertThat(ticker.quoteVolume()).isEqualByComparingTo("80234000.12");

        FundingRate fr =
                GateioAdapters.adaptFuturesFundingRate(BTC_USDT_PERP, entry, tsMs);
        assertThat(fr).isNotNull();
        assertThat(fr.rate()).isEqualByComparingTo("0.00009876");
        assertThat(fr.markPrice()).isEqualByComparingTo("65010.0");
        assertThat(fr.intervalHours()).isEqualTo(8);
    }

    @Test
    void futuresFundingNullWhenAbsent() {
        JsonNode stub = MAPPER.createObjectNode();
        assertThat(GateioAdapters.adaptFuturesFundingRate(BTC_USDT_PERP, stub, 0L)).isNull();
    }

    private static JsonNode loadFixture(String name) throws Exception {
        try (InputStream in =
                GateioAdaptersTest.class.getResourceAsStream("/" + name)) {
            Objects.requireNonNull(in, name);
            return MAPPER.readTree(in);
        }
    }
}
