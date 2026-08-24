package com.qtsurfer.qtstreamx.exchange.binance;

import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import org.junit.jupiter.api.Test;

class BinanceSymbolsTest {

    @Test
    void toStreamSpot() {
        var inst = new Instrument("BTC", "USDT");
        assertThat(BinanceSymbols.toStream(inst)).isEqualTo("btcusdt");
    }

    @Test
    void toStreamFutures() {
        var inst = new Instrument("ETH", "USDT", "USDT");
        assertThat(BinanceSymbols.toStream(inst)).isEqualTo("ethusdt");
    }

    @Test
    void toStreamExoticQuote() {
        // Regression: HEI/TRY used to be misparsed by the deleted
        // toSpotInstrument heuristic (no TRY in QUOTES); toStream is pure
        // concatenation so it is unaffected by quote-list completeness.
        var inst = new Instrument("HEI", "TRY");
        assertThat(BinanceSymbols.toStream(inst)).isEqualTo("heitry");
    }
}
