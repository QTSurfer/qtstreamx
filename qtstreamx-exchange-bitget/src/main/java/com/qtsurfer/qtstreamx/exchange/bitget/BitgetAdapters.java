package com.qtsurfer.qtstreamx.exchange.bitget;

import com.fasterxml.jackson.databind.JsonNode;
import com.qtsurfer.qtstreamx.core.model.FundingRate;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.model.Kline;
import com.qtsurfer.qtstreamx.core.model.Ticker;
import java.math.BigDecimal;

/**
 * Parsers for Bitget WS v2 payloads ({@code ws.bitget.com/v2/ws/public}).
 *
 * <p>v2 gives the same wire fields for SPOT and USDT-FUTURES tickers; futures tickers add
 * {@code markPrice}, {@code fundingRate} and {@code nextFundingTime}. Adapters take the data
 * entry plus the originating {@link Instrument}.
 *
 * <p>Funding interval on Bitget is uniformly 8h (no per-symbol override like Bybit), so the
 * funding-rate adapter hard-codes {@code intervalHours=8}.
 */
final class BitgetAdapters {

    private BitgetAdapters() {}

    static Ticker adaptTicker(Instrument instrument, JsonNode data) {
        long tsMs = longOrZero(data, "ts");
        return new Ticker(
                instrument,
                decimal(data, "bidPr"),
                decimal(data, "bidSz"),
                decimal(data, "askPr"),
                decimal(data, "askSz"),
                decimal(data, "lastPr"),
                decimal(data, "open24h"),
                decimal(data, "high24h"),
                decimal(data, "low24h"),
                decimal(data, "baseVolume"),
                decimal(data, "quoteVolume"),
                tsMs * 1_000L);
    }

    /**
     * Parse funding rate off a USDT-FUTURES ticker. Returns null when the message is a spot
     * ticker that happens to share the channel routing key (defensive: shouldn't happen in
     * practice, but keeps the dispatcher type-safe).
     */
    static FundingRate adaptFundingRate(Instrument instrument, JsonNode data) {
        BigDecimal rate = decimal(data, "fundingRate");
        if (rate == null) return null;
        long nextMs = longOrZero(data, "nextFundingTime");
        long tsMs = longOrZero(data, "ts");
        return new FundingRate(
                instrument,
                rate,
                decimal(data, "markPrice"),
                nextMs * 1_000L,
                /* intervalHours */ 8,
                tsMs * 1_000L);
    }

    /**
     * Parse a candle entry. Bitget v2 candle data is an array of string arrays:
     * {@code [ts, open, high, low, close, baseVolume, quoteVolume]}; the last element is
     * already "confirmed" when the channel is {@code candle<interval>} and the interval is
     * closed, otherwise it's the in-flight bucket.
     */
    static Kline adaptKline(Instrument instrument, String interval, JsonNode entry) {
        if (!entry.isArray() || entry.size() < 7) return null;
        long startMs = longFrom(entry.get(0));
        return new Kline(
                instrument,
                interval,
                decimalFrom(entry.get(1)),
                decimalFrom(entry.get(2)),
                decimalFrom(entry.get(3)),
                decimalFrom(entry.get(4)),
                decimalFrom(entry.get(5)),
                decimalFrom(entry.get(6)),
                /* numberOfTrades */ 0L,
                /* closed — Bitget doesn't flag confirm on the candle payload itself; upstream
                 * has to treat every message as a best-known snapshot. */
                false,
                startMs * 1_000L,
                /* closeTime — synthesise from start + interval */
                (startMs + intervalMs(interval)) * 1_000L - 1_000L);
    }

    /* ----- helpers ----- */

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

    private static BigDecimal decimalFrom(JsonNode v) {
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
        return longFrom(node.get(field));
    }

    private static long longFrom(JsonNode v) {
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
            case 'H' -> value * 3_600_000L;
            case 'D' -> value * 86_400_000L;
            case 'W' -> value * 7L * 86_400_000L;
            default -> 0L;
        };
    }
}
