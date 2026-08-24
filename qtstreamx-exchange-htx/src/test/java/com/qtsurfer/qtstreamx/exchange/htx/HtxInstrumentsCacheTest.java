package com.qtsurfer.qtstreamx.exchange.htx;

import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.exchange.htx.HtxInstrumentsCache.Market;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Set;
import org.junit.jupiter.api.Test;

class HtxInstrumentsCacheTest {

    @Test
    void spotFiltersOffline() throws Exception {
        Set<Instrument> ins =
                HtxInstrumentsCache.parse(loadFixture("htx-instruments-spot.json"), Market.SPOT);
        assertThat(ins)
                .containsExactlyInAnyOrder(
                        new Instrument("BTC", "USDT", null),
                        new Instrument("ETH", "USDT", null),
                        new Instrument("SOL", "USDC", null));
    }

    @Test
    void linearSwapFiltersDelistedAndDatedFutures() throws Exception {
        Set<Instrument> ins =
                HtxInstrumentsCache.parse(
                        loadFixture("htx-instruments-futures.json"), Market.LINEAR_SWAP);
        // OLD-USDT filtered by status=2; BTC-USDT-240628 filtered by business_type=futures
        assertThat(ins)
                .containsExactlyInAnyOrder(
                        new Instrument("BTC", "USDT", "USDT"),
                        new Instrument("ETH", "USDT", "USDT"),
                        new Instrument("SOL", "USDT", "USDT"));
    }

    @Test
    void invalidBodyRaises() {
        try {
            HtxInstrumentsCache.parse("not json", Market.SPOT);
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).contains("Parsing HTX instruments response");
            return;
        }
        throw new AssertionError("expected RuntimeException");
    }

    @Test
    void emptyArrayReturnsEmpty() {
        Set<Instrument> ins = HtxInstrumentsCache.parse("{\"data\":[]}", Market.SPOT);
        assertThat(ins).isEmpty();
    }

    @Test
    void nonArrayDataReturnsEmpty() {
        Set<Instrument> ins = HtxInstrumentsCache.parse("{\"data\":null}", Market.SPOT);
        assertThat(ins).isEmpty();
    }

    private static String loadFixture(String name) throws Exception {
        try (InputStream in =
                HtxInstrumentsCacheTest.class.getResourceAsStream("/" + name)) {
            Objects.requireNonNull(in, name);
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
