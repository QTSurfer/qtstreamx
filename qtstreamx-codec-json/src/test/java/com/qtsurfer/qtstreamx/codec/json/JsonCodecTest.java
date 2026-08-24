package com.qtsurfer.qtstreamx.codec.json;

import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class JsonCodecTest {

    @Test
    void roundTripsDerivativeInstrument() {
        JsonCodec<Instrument> codec = new JsonCodec<>();
        Instrument instrument = new Instrument("BTC", "USDT", "USDT");

        Instrument decoded = codec.decode(codec.encode(instrument), Instrument.class);

        assertThat(decoded).isEqualTo(instrument);
    }

    @Test
    void preservesDerivativeFlagInJson() {
        JsonCodec<Instrument> codec = new JsonCodec<>();
        Instrument derivative = new Instrument("BTC", "USDT", "USDT");
        Instrument spot = new Instrument("BTC", "USDT");

        String derivativeJson = new String(codec.encode(derivative), StandardCharsets.UTF_8);
        String spotJson = new String(codec.encode(spot), StandardCharsets.UTF_8);

        assertThat(derivativeJson).contains("\"derivative\":true");
        assertThat(spotJson).contains("\"derivative\":false");
    }
}
