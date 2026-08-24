package com.qtsurfer.qtstreamx.core.model;

import java.util.Objects;

/**
 * Ticker payload associated with its venue-native market identity.
 *
 * @param market stable venue-native market identity
 * @param ticker normalized ticker payload
 */
public record MarketTicker(MarketId market, Ticker ticker) {
    /** Validates that market and payload identify the same instrument. */
    public MarketTicker {
        Objects.requireNonNull(market, "market");
        Objects.requireNonNull(ticker, "ticker");
        if (!market.instrument().equals(ticker.instrument())) {
            throw new IllegalArgumentException("ticker instrument must match market instrument");
        }
    }
}
