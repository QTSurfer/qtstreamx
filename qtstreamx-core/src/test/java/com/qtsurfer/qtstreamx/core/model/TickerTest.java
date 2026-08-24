package com.qtsurfer.qtstreamx.core.model;

import static org.assertj.core.api.Assertions.assertThat;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

class TickerTest {

    @Test
    void midPrice() {
        var ticker = new Ticker(
                new Instrument("BTC", "USDT"),
                new BigDecimal("70000.00"),
                new BigDecimal("1.5"),
                new BigDecimal("70002.00"),
                new BigDecimal("2.0"),
                new BigDecimal("70001.00"),
                null, null, null, null, null,
                1711238400000000L
        );
        assertThat(ticker.mid()).isEqualByComparingTo(new BigDecimal("70001.00"));
    }

    @Test
    void spread() {
        var ticker = new Ticker(
                new Instrument("ETH", "USDT", "USDT"),
                new BigDecimal("3500.00"),
                new BigDecimal("10"),
                new BigDecimal("3500.50"),
                new BigDecimal("5"),
                new BigDecimal("3500.25"),
                null, null, null, null, null,
                1711238400000000L
        );
        assertThat(ticker.spread()).isEqualByComparingTo(new BigDecimal("0.50"));
    }

    @Test
    void nullBidAskHandledGracefully() {
        var ticker = new Ticker(
                new Instrument("SOL", "USDT"),
                null, null, null, null,
                new BigDecimal("150.00"),
                null, null, null, null, null,
                1711238400000000L
        );
        assertThat(ticker.mid()).isNull();
        assertThat(ticker.spread()).isNull();
    }
}
