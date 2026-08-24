package com.qtsurfer.qtstreamx.exchange.gateio;

import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.exchange.gateio.GateioInstrumentsCache.Market;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Set;
import org.junit.jupiter.api.Test;

class GateioInstrumentsCacheTest {

    @Test
    void spotFiltersUntradable() throws Exception {
        Set<Instrument> ins =
                GateioInstrumentsCache.parse(loadFixture("gateio-instruments-spot.json"), Market.SPOT);
        assertThat(ins)
                .containsExactlyInAnyOrder(
                        new Instrument("BTC", "USDT", null),
                        new Instrument("ETH", "USDT", null),
                        new Instrument("SOL", "USDC", null));
    }

    @Test
    void futuresFiltersDelisting() throws Exception {
        Set<Instrument> ins =
                GateioInstrumentsCache.parse(
                        loadFixture("gateio-instruments-futures.json"), Market.FUTURES_USDT);
        assertThat(ins)
                .containsExactlyInAnyOrder(
                        new Instrument("BTC", "USDT", "USDT"),
                        new Instrument("ETH", "USDT", "USDT"),
                        new Instrument("SOL", "USDT", "USDT"));
    }

    @Test
    void invalidBodyRaises() {
        try {
            GateioInstrumentsCache.parse("not json", Market.SPOT);
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).contains("Parsing Gate.io instruments response");
            return;
        }
        throw new AssertionError("expected RuntimeException");
    }

    private static String loadFixture(String name) throws Exception {
        try (InputStream in =
                GateioInstrumentsCacheTest.class.getResourceAsStream("/" + name)) {
            Objects.requireNonNull(in, name);
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
