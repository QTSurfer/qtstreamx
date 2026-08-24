package com.qtsurfer.qtstreamx.core.model;

import java.math.BigDecimal;

/**
 * Normalized funding rate for perpetual instruments.
 *
 * @param instrument      normalized instrument (must be derivative)
 * @param rate            funding rate as decimal (e.g. 0.0001 = 0.01%)
 * @param markPrice       mark price at funding time (nullable)
 * @param nextFundingTime next funding event, epoch microseconds (µs)
 * @param intervalHours   funding interval in hours (typically 1, 4, or 8)
 * @param timestamp       observation time, epoch microseconds (µs)
 */
public record FundingRate(
        Instrument instrument,
        BigDecimal rate,
        BigDecimal markPrice,
        long nextFundingTime,
        int intervalHours,
        long timestamp
) {}
