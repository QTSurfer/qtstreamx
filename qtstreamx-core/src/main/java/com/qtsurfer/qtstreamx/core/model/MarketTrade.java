package com.qtsurfer.qtstreamx.core.model;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * Normalized trade emitted by one identified market.
 *
 * @param market stable venue-native market identity
 * @param eventId opaque event identifier stable across replay
 * @param price quote units per one base unit
 * @param baseAmount positive base quantity
 * @param quoteAmount positive quote quantity
 * @param side trade direction from the base asset's perspective
 * @param timestamp event time in epoch microseconds
 */
public record MarketTrade(
        MarketId market,
        String eventId,
        BigDecimal price,
        BigDecimal baseAmount,
        BigDecimal quoteAmount,
        TradeSide side,
        long timestamp
) {
    /** Validates normalized trade invariants. */
    public MarketTrade {
        Objects.requireNonNull(market, "market");
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId must not be blank");
        }
        requirePositive(price, "price");
        requirePositive(baseAmount, "baseAmount");
        requirePositive(quoteAmount, "quoteAmount");
        Objects.requireNonNull(side, "side");
        if (timestamp < 0) {
            throw new IllegalArgumentException("timestamp must be non-negative");
        }
    }

    private static void requirePositive(BigDecimal value, String name) {
        Objects.requireNonNull(value, name);
        if (value.signum() <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }
}
