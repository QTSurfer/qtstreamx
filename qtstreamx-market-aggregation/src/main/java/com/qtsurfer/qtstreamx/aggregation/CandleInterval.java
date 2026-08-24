package com.qtsurfer.qtstreamx.aggregation;

/**
 * Fixed event-time candle interval.
 *
 * @param name normalized interval name used by {@code Kline}
 * @param durationMicros interval duration in microseconds
 */
public record CandleInterval(String name, long durationMicros) {
    /** One-minute fixed interval. */
    public static final CandleInterval ONE_MINUTE =
            new CandleInterval("1m", 60_000_000L);

    /**
     * Validates the fixed interval.
     *
     * @throws IllegalArgumentException if the name is blank or duration is not positive
     */
    public CandleInterval {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        if (durationMicros <= 0) {
            throw new IllegalArgumentException("durationMicros must be positive");
        }
    }
}
