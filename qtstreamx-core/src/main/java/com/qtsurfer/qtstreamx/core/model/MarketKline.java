package com.qtsurfer.qtstreamx.core.model;

import java.util.Objects;

/**
 * Kline payload associated with its venue-native market identity.
 *
 * @param market stable venue-native market identity
 * @param kline normalized kline payload
 */
public record MarketKline(MarketId market, Kline kline) {
    /** Validates that market and payload identify the same instrument. */
    public MarketKline {
        Objects.requireNonNull(market, "market");
        Objects.requireNonNull(kline, "kline");
        if (!market.instrument().equals(kline.instrument())) {
            throw new IllegalArgumentException("kline instrument must match market instrument");
        }
    }
}
