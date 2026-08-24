package com.qtsurfer.qtstreamx.exchange.okx;

import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.exchange.okx.OkxInstrumentsCache.InstType;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Set;
import org.junit.jupiter.api.Test;

class OkxInstrumentsCacheTest {

    @Test
    void parseSpotFiltersNonLive() throws Exception {
        Set<Instrument> instruments =
                OkxInstrumentsCache.parse(loadFixture("okx-instruments-spot.json"), InstType.SPOT);

        assertThat(instruments)
                .containsExactlyInAnyOrder(
                        new Instrument("BTC", "USDT", null),
                        new Instrument("ETH", "USDT", null),
                        new Instrument("SOL", "USDC", null));
        assertThat(instruments).doesNotContain(new Instrument("DELISTED", "USDT", null));
    }

    @Test
    void parseSwapKeepsLinearOnly() throws Exception {
        Set<Instrument> instruments =
                OkxInstrumentsCache.parse(loadFixture("okx-instruments-swap.json"), InstType.SWAP);

        // Only linear USDⓈ-M perps; BTC-USD (inverse) and SUSPENDED (state != live) are filtered.
        assertThat(instruments)
                .containsExactlyInAnyOrder(
                        new Instrument("BTC", "USDT", "USDT"),
                        new Instrument("ETH", "USDT", "USDT"));
    }

    @Test
    void parseReturnsEmptyForNoData() {
        Set<Instrument> instruments =
                OkxInstrumentsCache.parse("{\"code\":\"0\",\"data\":[]}", InstType.SPOT);
        assertThat(instruments).isEmpty();
    }

    @Test
    void invalidJsonRaises() {
        try {
            OkxInstrumentsCache.parse("not json", InstType.SPOT);
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).contains("Parsing OKX instruments response");
            return;
        }
        throw new AssertionError("expected RuntimeException");
    }

    private static String loadFixture(String name) throws Exception {
        try (InputStream in =
                OkxInstrumentsCacheTest.class.getResourceAsStream("/" + name)) {
            Objects.requireNonNull(in, name + " missing from classpath");
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
