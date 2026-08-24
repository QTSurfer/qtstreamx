package com.qtsurfer.qtstreamx.exchange.kraken;

import com.fasterxml.jackson.databind.JsonNode;
import com.qtsurfer.qtstreamx.core.model.FundingRate;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.model.Kline;
import com.qtsurfer.qtstreamx.core.model.Ticker;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.format.DateTimeParseException;

/**
 * Parsers for Kraken spot v2 and Kraken futures v1/v3 WS payloads.
 *
 * <p>Spot side uses {@code ws.kraken.com/v2} with JSON-per-channel envelopes. Futures side uses
 * {@code futures.kraken.com/ws/v1} with a flatter message shape (fields sit on the root object,
 * not nested under {@code data}). Both flavours are normalised here to the qtstreamx
 * {@link Ticker}/{@link Kline}/{@link FundingRate} records.
 */
final class KrakenAdapters {

    private KrakenAdapters() {}

    /**
     * Parse a spot v2 ticker {@code data[0]}. Kraken v2 tickers carry last + bid/ask with sizes,
     * 24h high/low, 24h volume, and a VWAP — no explicit 24h open, so we leave that field null.
     * The v2 envelope doesn't include a per-tick timestamp; callers pass in {@code tsMs} from
     * the message-level {@code "ts"} (an ISO-8601 string in the v2 schema, already parsed
     * upstream).
     */
    static Ticker adaptTickerSpot(Instrument instrument, JsonNode entry, long tsMs) {
        return new Ticker(
                instrument,
                decimal(entry, "bid"),
                decimal(entry, "bid_qty"),
                decimal(entry, "ask"),
                decimal(entry, "ask_qty"),
                decimal(entry, "last"),
                /* open */ null, // v2 doesn't emit 24h open
                decimal(entry, "high"),
                decimal(entry, "low"),
                decimal(entry, "volume"),
                // v2 has no quoteVolume; best we can do without polling REST is vwap × volume,
                // which would be a synthesis rather than a real wire value, so we leave null.
                /* quoteVolume */ null,
                tsMs * 1_000L);
    }

    /**
     * Parse a spot v2 OHLC {@code data[0]}. Kraken emits kline updates as one object per
     * interval (no array-of-arrays like OKX).
     */
    static Kline adaptKlineSpot(Instrument instrument, String interval, JsonNode entry) {
        long intervalBegin = parseIsoMs(entry.path("interval_begin").asText(""));
        int intervalMinutes = entry.path("interval").asInt(0);
        return new Kline(
                instrument,
                interval,
                decimal(entry, "open"),
                decimal(entry, "high"),
                decimal(entry, "low"),
                decimal(entry, "close"),
                decimal(entry, "volume"),
                // v2 OHLC does NOT emit quote volume; leave null rather than zero so downstream
                // sinks can distinguish missing data from "zero traded quote".
                /* quoteVolume */ null,
                entry.path("trades").asLong(0L),
                /* closed — v2 streams one update per interval-end only; it never sends partial
                 * updates like Binance does. confirm=true always. */
                true,
                intervalBegin * 1_000L,
                (intervalBegin + intervalMinutes * 60_000L) * 1_000L - 1_000L);
    }

    /**
     * Parse a futures v1 ticker (full message, not a data entry). The futures feed surfaces
     * bid / ask / last / markPrice / index + 24h volume. The same message also carries the
     * funding-rate fields used by {@link #adaptFundingRate(Instrument, JsonNode)} — a pattern
     * that matches the Bybit linear ticker.
     *
     * <p>{@code time} on Kraken futures is epoch milliseconds (long, not string).
     */
    static Ticker adaptTickerFutures(Instrument instrument, JsonNode root) {
        long tsMs = root.path("time").asLong(0L);
        return new Ticker(
                instrument,
                decimal(root, "bid"),
                decimal(root, "bid_size"),
                decimal(root, "ask"),
                decimal(root, "ask_size"),
                decimal(root, "last"),
                /* open */ null, // futures v1 doesn't publish 24h open either
                decimal(root, "high_24h"),
                decimal(root, "low_24h"),
                decimal(root, "volume_24h"),
                /* quoteVolume */ null,
                tsMs * 1_000L);
    }

    /**
     * Parse a funding rate off the futures v1 ticker message. Kraken perps settle
     * <b>hourly</b> — distinct from the 8h default of Binance / OKX / Bybit — so
     * {@code intervalHours=1} is hard-coded. The {@code funding_rate} field on the wire is
     * the current hourly funding rate; {@code next_funding_rate_time} is epoch milliseconds.
     *
     * <p>Returns null when {@code funding_rate} isn't present, which happens on non-perp
     * messages that share the ticker channel (e.g. quarterly futures).
     */
    static FundingRate adaptFundingRate(Instrument instrument, JsonNode root) {
        BigDecimal rate = decimal(root, "funding_rate");
        if (rate == null) {
            return null;
        }
        long nextMs = root.path("next_funding_rate_time").asLong(0L);
        long tsMs = root.path("time").asLong(0L);
        return new FundingRate(
                instrument,
                rate,
                decimal(root, "markPrice"),
                nextMs * 1_000L,
                /* intervalHours */ 1,
                tsMs * 1_000L);
    }

    /* ----- helpers ----- */

    private static BigDecimal decimal(JsonNode node, String field) {
        JsonNode v = node.get(field);
        if (v == null || v.isNull()) return null;
        if (v.isNumber()) {
            return new BigDecimal(v.asText()); // preserve precision from the wire string
        }
        String s = v.asText("");
        if (s.isEmpty()) return null;
        try {
            return new BigDecimal(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** Kraken v2 emits ISO-8601 timestamps like {@code "2026-04-21T10:30:00.000000Z"}. */
    static long parseIsoMs(String iso) {
        if (iso == null || iso.isEmpty()) return 0L;
        try {
            return Instant.parse(iso).toEpochMilli();
        } catch (DateTimeParseException e) {
            return 0L;
        }
    }
}
