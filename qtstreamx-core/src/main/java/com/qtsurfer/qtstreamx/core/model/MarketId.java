package com.qtsurfer.qtstreamx.core.model;

import java.util.Objects;

/**
 * Stable identity for a venue-native market.
 *
 * @param venue venue and protocol name, such as {@code uniswap-v3}
 * @param network network identity independent from transport endpoints
 * @param nativeId venue-native market identifier, such as a pool address
 * @param instrument normalized logical instrument traded by the market
 */
public record MarketId(String venue, String network, String nativeId, Instrument instrument) {

    /** Validates that the identity is complete while preserving its exact text. */
    public MarketId {
        requireText(venue, "venue");
        requireText(network, "network");
        requireText(nativeId, "nativeId");
        Objects.requireNonNull(instrument, "instrument");
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
