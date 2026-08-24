package com.qtsurfer.qtstreamx.core.model;

import java.math.BigDecimal;

/**
 * Normalized ticker representing a point-in-time market snapshot.
 *
 * <p>This is the central data contract of QTStreamX. All exchange-specific
 * formats are normalized into this record before publishing.
 *
 * @param instrument  normalized instrument
 * @param bid         best bid price
 * @param bidSize     size at best bid
 * @param ask         best ask price
 * @param askSize     size at best ask
 * @param last        last traded price
 * @param open        24h open price (nullable)
 * @param high        24h high price (nullable)
 * @param low         24h low price (nullable)
 * @param volume      24h volume in base currency (nullable)
 * @param quoteVolume 24h volume in quote currency (nullable)
 * @param timestamp   epoch microseconds (µs) — QuestDB native resolution
 */
public record Ticker(
        Instrument instrument,
        BigDecimal bid,
        BigDecimal bidSize,
        BigDecimal ask,
        BigDecimal askSize,
        BigDecimal last,
        BigDecimal open,
        BigDecimal high,
        BigDecimal low,
        BigDecimal volume,
        BigDecimal quoteVolume,
        long timestamp
) {
    /** Convenience: mid price = (bid + ask) / 2. */
    public BigDecimal mid() {
        if (bid != null && ask != null) {
            return bid.add(ask).divide(BigDecimal.TWO, bid.scale(), java.math.RoundingMode.HALF_UP);
        }
        return null;
    }

    /** Convenience: spread = ask - bid. */
    public BigDecimal spread() {
        if (bid != null && ask != null) {
            return ask.subtract(bid);
        }
        return null;
    }
}
