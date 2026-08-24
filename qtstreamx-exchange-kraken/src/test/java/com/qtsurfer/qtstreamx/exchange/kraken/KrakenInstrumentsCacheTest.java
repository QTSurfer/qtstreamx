package com.qtsurfer.qtstreamx.exchange.kraken;

import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.exchange.kraken.KrakenInstrumentsCache.Market;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Set;
import org.junit.jupiter.api.Test;

class KrakenInstrumentsCacheTest {

    @Test
    void spotRewritesXbtToBtcAndFiltersDelisted() throws Exception {
        Set<Instrument> instruments =
                KrakenInstrumentsCache.parse(
                        loadFixture("kraken-instruments-spot.json"), Market.SPOT);
        assertThat(instruments)
                .containsExactlyInAnyOrder(
                        new Instrument("BTC", "USD", null),
                        new Instrument("ETH", "USD", null),
                        new Instrument("SOL", "USDT", null));
    }

    @Test
    void futuresKeepsPfOnlyAndRewritesXbt() throws Exception {
        Set<Instrument> instruments =
                KrakenInstrumentsCache.parse(
                        loadFixture("kraken-instruments-futures.json"), Market.FUTURES_LINEAR);
        // PF_XBTUSD, PF_ETHUSD, PF_SOLUSDT kept; PI_*, FF_*, tradeable=false dropped.
        assertThat(instruments)
                .containsExactlyInAnyOrder(
                        new Instrument("BTC", "USD", "USD"),
                        new Instrument("ETH", "USD", "USD"),
                        new Instrument("SOL", "USDT", "USDT"));
    }

    @Test
    void emptyResponsesGiveEmptySet() {
        assertThat(KrakenInstrumentsCache.parse("{\"result\":{}}", Market.SPOT)).isEmpty();
        assertThat(KrakenInstrumentsCache.parse("{\"instruments\":[]}", Market.FUTURES_LINEAR))
                .isEmpty();
    }

    @Test
    void invalidJsonRaises() {
        try {
            KrakenInstrumentsCache.parse("not json", Market.SPOT);
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).contains("Parsing Kraken instruments response");
            return;
        }
        throw new AssertionError("expected RuntimeException");
    }

    private static String loadFixture(String name) throws Exception {
        try (InputStream in =
                KrakenInstrumentsCacheTest.class.getResourceAsStream("/" + name)) {
            Objects.requireNonNull(in, name + " missing from classpath");
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
