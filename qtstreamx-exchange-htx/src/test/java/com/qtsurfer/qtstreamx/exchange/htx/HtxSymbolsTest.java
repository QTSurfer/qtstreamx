package com.qtsurfer.qtstreamx.exchange.htx;

import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import org.junit.jupiter.api.Test;

class HtxSymbolsTest {

    @Test
    void spotLowercaseConcat() {
        assertThat(HtxSymbols.toSpotSymbol(new Instrument("BTC", "USDT", null)))
                .isEqualTo("btcusdt");
    }

    @Test
    void linearDashedUppercase() {
        assertThat(HtxSymbols.toLinearContract(new Instrument("eth", "usdt", "usdt")))
                .isEqualTo("ETH-USDT");
    }
}
