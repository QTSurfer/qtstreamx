package com.qtsurfer.qtstreamx.core.model;

import java.math.BigDecimal;

/**
 * Normalized OHLCV candle.
 *
 * @param instrument     normalized instrument
 * @param interval       candle interval (e.g. "1s", "1m", "5m", "1h", "1d")
 * @param open           open price
 * @param high           high price
 * @param low            low price
 * @param close          close price
 * @param volume         volume in base currency
 * @param quoteVolume    volume in quote currency
 * @param numberOfTrades trade count accumulated in this bucket (0 when not reported)
 * @param closed         true if candle is finalized
 * @param timestamp      candle open time, epoch microseconds (µs)
 * @param closeTime      candle close time, epoch microseconds (µs)
 */
public record Kline(
        Instrument instrument,
        String interval,
        BigDecimal open,
        BigDecimal high,
        BigDecimal low,
        BigDecimal close,
        BigDecimal volume,
        BigDecimal quoteVolume,
        long numberOfTrades,
        boolean closed,
        long timestamp,
        long closeTime
) {}
