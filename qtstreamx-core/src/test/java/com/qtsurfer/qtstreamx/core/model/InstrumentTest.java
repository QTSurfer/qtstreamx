package com.qtsurfer.qtstreamx.core.model;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;

class InstrumentTest {

    @Test
    void spotSymbol() {
        var inst = new Instrument("BTC", "USDT");
        assertThat(inst.symbol()).isEqualTo("BTC/USDT");
        assertThat(inst.isDerivative()).isFalse();
        assertThat(inst.settle()).isNull();
    }

    @Test
    void perpetualSymbol() {
        var inst = new Instrument("BTC", "USDT", "USDT");
        assertThat(inst.symbol()).isEqualTo("BTC/USDT:USDT");
        assertThat(inst.isDerivative()).isTrue();
    }

    @Test
    void coinMarginedSymbol() {
        var inst = new Instrument("BTC", "USD", "BTC");
        assertThat(inst.symbol()).isEqualTo("BTC/USD:BTC");
        assertThat(inst.isDerivative()).isTrue();
    }

    @Test
    void parseSpot() {
        var inst = Instrument.parse("ETH/USDT");
        assertThat(inst.base()).isEqualTo("ETH");
        assertThat(inst.quote()).isEqualTo("USDT");
        assertThat(inst.settle()).isNull();
    }

    @Test
    void parsePerpetual() {
        var inst = Instrument.parse("BTC/USDT:USDT");
        assertThat(inst.base()).isEqualTo("BTC");
        assertThat(inst.quote()).isEqualTo("USDT");
        assertThat(inst.settle()).isEqualTo("USDT");
    }

    @Test
    void parseRoundTrip() {
        var original = new Instrument("SOL", "USDT", "USDT");
        var parsed = Instrument.parse(original.symbol());
        assertThat(parsed).isEqualTo(original);
    }

    @Test
    void toStringIsSymbol() {
        var inst = new Instrument("BTC", "USDT", "USDT");
        assertThat(inst.toString()).isEqualTo("BTC/USDT:USDT");
    }
}
