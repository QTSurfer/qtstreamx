package com.qtsurfer.qtstreamx.exchange.bitget;

import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.exchange.bitget.BitgetInstrumentsCache.Market;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Set;
import org.junit.jupiter.api.Test;

class BitgetInstrumentsCacheTest {

    @Test
    void spotFiltersOffline() throws Exception {
        Set<Instrument> ins =
                BitgetInstrumentsCache.parse(loadFixture("bitget-instruments-spot.json"), Market.SPOT);
        assertThat(ins)
                .containsExactlyInAnyOrder(
                        new Instrument("BTC", "USDT", null),
                        new Instrument("ETH", "USDT", null),
                        new Instrument("SOL", "USDC", null));
    }

    @Test
    void futuresFiltersNonNormalStatus() throws Exception {
        Set<Instrument> ins =
                BitgetInstrumentsCache.parse(
                        loadFixture("bitget-instruments-futures.json"), Market.USDT_FUTURES);
        assertThat(ins)
                .containsExactlyInAnyOrder(
                        new Instrument("BTC", "USDT", "USDT"),
                        new Instrument("ETH", "USDT", "USDT"));
    }

    @Test
    void emptyAndInvalid() {
        assertThat(BitgetInstrumentsCache.parse("{\"data\":[]}", Market.SPOT)).isEmpty();
        try {
            BitgetInstrumentsCache.parse("not json", Market.SPOT);
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).contains("Parsing Bitget instruments response");
            return;
        }
        throw new AssertionError("expected RuntimeException");
    }

    private static String loadFixture(String name) throws Exception {
        try (InputStream in =
                BitgetInstrumentsCacheTest.class.getResourceAsStream("/" + name)) {
            Objects.requireNonNull(in, name);
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
