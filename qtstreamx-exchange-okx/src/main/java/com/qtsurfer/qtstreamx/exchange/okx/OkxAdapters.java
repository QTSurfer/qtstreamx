package com.qtsurfer.qtstreamx.exchange.okx;

import com.fasterxml.jackson.databind.JsonNode;
import com.qtsurfer.qtstreamx.core.model.FundingRate;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.model.Kline;
import com.qtsurfer.qtstreamx.core.model.Ticker;
import java.math.BigDecimal;

/**
 * Parsers for OKX WebSocket v5 payloads ({@code ws.okx.com/ws/v5/public}).
 *
 * <p>OKX envelopes look like {@code {"arg":{"channel":...,"instId":...},"data":[...]}}. Adapters
 * take a single {@code data[i]} node (spot/swap tickers and funding-rate emit one object per
 * message; klines can emit several).
 */
final class OkxAdapters {

    private OkxAdapters() {}

    /**
     * Parse a {@code tickers} channel entry. OKX tickers carry last, bid/ask with sizes, 24h
     * open/high/low, 24h volume in base and quote currency — enough to fill every field on
     * {@link Ticker}.
     */
    static Ticker adaptTicker(Instrument instrument, JsonNode entry) {
        long tsMs = longOrZero(entry, "ts");
        return new Ticker(
                instrument,
                decimal(entry, "bidPx"),
                decimal(entry, "bidSz"),
                decimal(entry, "askPx"),
                decimal(entry, "askSz"),
                decimal(entry, "last"),
                decimal(entry, "open24h"),
                decimal(entry, "high24h"),
                decimal(entry, "low24h"),
                decimal(entry, "vol24h"),
                decimal(entry, "volCcy24h"),
                tsMs * 1_000L);
    }

    /**
     * Parse a {@code funding-rate} channel entry. OKX perpetual swaps settle every 8h; the
     * protocol sends both the current {@code fundingRate} / {@code fundingTime} and the
     * {@code nextFundingRate} / {@code nextFundingTime}. We surface the <em>current</em>
     * rate (matching Bybit / Binance's model) and use {@code nextFundingTime} for the
     * observation's next-funding marker — that is what downstream analytics use to gate the
     * settlement event.
     *
     * <p>Returns null when {@code fundingRate} is absent, which happens on non-perp messages
     * that share the channel routing layer.
     */
    static FundingRate adaptFundingRate(Instrument instrument, JsonNode entry) {
        BigDecimal rate = decimal(entry, "fundingRate");
        if (rate == null) {
            return null;
        }
        long nextFundingMs = longOrZero(entry, "nextFundingTime");
        // OKX message carries no mark price on the funding-rate channel — callers that need it
        // can join against a separate mark-price channel subscription; passing null keeps the
        // Ticker/Funding split clean.
        return new FundingRate(
                instrument,
                rate,
                /* markPrice */ null,
                nextFundingMs * 1_000L,
                /* intervalHours */ 8,
                longOrZero(entry, "ts") * 1_000L);
    }

    /**
     * Parse a kline array entry. OKX klines are arrays of strings:
     *
     * <pre>["ts","o","h","l","c","vol","volCcy","volCcyQuote","confirm"]</pre>
     *
     * <p>{@code confirm} is "1" for a closed candle, "0" for intermediate. {@code vol} is in
     * contracts on SWAP and in base currency on SPOT; {@code volCcy} is in quote currency — we
     * map volume to {@code vol} (base) and quoteVolume to {@code volCcy} (quote) to match the
     * Binance convention.
     */
    static Kline adaptKline(Instrument instrument, String interval, JsonNode entry) {
        if (!entry.isArray() || entry.size() < 7) {
            return null;
        }
        long startMs = longFrom(entry.get(0));
        boolean confirm = entry.size() > 8 && "1".equals(entry.get(8).asText(""));
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
                confirm,
                startMs * 1_000L,
                /* closeTime µs — OKX gives start only; synthesise from interval */
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

    /** Approximate ms-per-candle for the interval shorthand ("1s","1m","5m","1H","1D","1W"). */
    private static long intervalMs(String interval) {
        if (interval == null || interval.length() < 2) return 0L;
        long value;
        try {
            value = Long.parseLong(interval.substring(0, interval.length() - 1));
        } catch (NumberFormatException e) {
            return 0L;
        }
        char unit = interval.charAt(interval.length() - 1);
        return switch (unit) {
            case 's' -> value * 1_000L;
            case 'm' -> value * 60_000L;
            case 'H' -> value * 3_600_000L;
            case 'D' -> value * 86_400_000L;
            case 'W' -> value * 7L * 86_400_000L;
            default -> 0L;
        };
    }
}
