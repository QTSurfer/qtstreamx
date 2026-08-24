package com.qtsurfer.qtstreamx.exchange.gateio;

import com.fasterxml.jackson.databind.JsonNode;
import com.qtsurfer.qtstreamx.core.model.FundingRate;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.model.Kline;
import com.qtsurfer.qtstreamx.core.model.Ticker;
import java.math.BigDecimal;

/**
 * Parsers for Gate.io WS v4 payloads. Spot and futures wire formats diverge enough to justify
 * separate adapters per channel, but both share the helper scaffolding.
 *
 * <p>Gate.io perps settle funding every 8h uniformly; {@link #adaptFuturesFundingRate} hard-
 * codes {@code intervalHours=8}.
 */
final class GateioAdapters {

    private GateioAdapters() {}

    /**
     * Parse a {@code spot.tickers} update. Gate.io spot tickers carry {@code last},
     * {@code lowest_ask}, {@code highest_bid}, 24h high/low and base/quote volume — no open24h
     * or bid/ask sizes on the wire, so those come back null.
     */
    static Ticker adaptSpotTicker(Instrument instrument, JsonNode result, long tsMs) {
        return new Ticker(
                instrument,
                decimal(result, "highest_bid"),
                /* bidSize */ null,
                decimal(result, "lowest_ask"),
                /* askSize */ null,
                decimal(result, "last"),
                /* open */ null,
                decimal(result, "high_24h"),
                decimal(result, "low_24h"),
                decimal(result, "base_volume"),
                decimal(result, "quote_volume"),
                tsMs * 1_000L);
    }

    /**
     * Parse a {@code spot.candlesticks} update. Gate.io emits single-object candles with short
     * field names {@code t, o, h, l, c, v, a} and {@code n = "<interval>_<pair>"}.
     */
    static Kline adaptSpotKline(Instrument instrument, String interval, JsonNode result) {
        long startMs = longOrZero(result, "t") * 1_000L; // seconds → ms
        return new Kline(
                instrument,
                interval,
                decimal(result, "o"),
                decimal(result, "h"),
                decimal(result, "l"),
                decimal(result, "c"),
                decimal(result, "v"),
                decimal(result, "a"),
                /* numberOfTrades */ 0L,
                /* closed */ false,
                startMs * 1_000L,
                (startMs + intervalMs(interval)) * 1_000L - 1_000L);
    }

    /**
     * Parse a {@code futures.tickers} update entry. Gate.io futures ticker carries funding-rate
     * fields inline, same pattern as Bitget / Bybit.
     */
    static Ticker adaptFuturesTicker(Instrument instrument, JsonNode entry, long tsMs) {
        return new Ticker(
                instrument,
                /* bid */ null, // futures.tickers doesn't publish bid/ask on the base update
                /* bidSize */ null,
                /* ask */ null,
                /* askSize */ null,
                decimal(entry, "last"),
                /* open */ null,
                decimal(entry, "high_24h"),
                decimal(entry, "low_24h"),
                decimal(entry, "volume_24h"),
                decimal(entry, "volume_24h_quote"),
                tsMs * 1_000L);
    }

    static FundingRate adaptFuturesFundingRate(Instrument instrument, JsonNode entry, long tsMs) {
        BigDecimal rate = decimal(entry, "funding_rate");
        if (rate == null) return null;
        long nextMs = longOrZero(entry, "funding_next_apply") * 1_000L; // seconds → ms
        return new FundingRate(
                instrument,
                rate,
                decimal(entry, "mark_price"),
                nextMs * 1_000L,
                /* intervalHours */ 8,
                tsMs * 1_000L);
    }

    /* helpers */

    private static BigDecimal decimal(JsonNode node, String field) {
        JsonNode v = node.get(field);
        if (v == null || v.isNull()) return null;
        String s = v.asText("");
        if (s.isEmpty()) return null;
        try {
            return new BigDecimal(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static long longOrZero(JsonNode node, String field) {
        JsonNode v = node.get(field);
        if (v == null || v.isNull()) return 0L;
        if (v.isNumber()) return v.asLong();
        try {
            return Long.parseLong(v.asText(""));
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    private static long intervalMs(String interval) {
        if (interval == null || interval.length() < 2) return 0L;
        long value;
        try {
            value = Long.parseLong(interval.substring(0, interval.length() - 1));
        } catch (NumberFormatException e) {
            return 0L;
        }
        return switch (interval.charAt(interval.length() - 1)) {
            case 's' -> value * 1_000L;
            case 'm' -> value * 60_000L;
            case 'h' -> value * 3_600_000L;
            case 'd' -> value * 86_400_000L;
            default -> 0L;
        };
    }
}
