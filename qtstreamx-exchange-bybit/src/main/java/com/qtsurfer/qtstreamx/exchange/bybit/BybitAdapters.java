package com.qtsurfer.qtstreamx.exchange.bybit;

import com.fasterxml.jackson.databind.JsonNode;
import com.qtsurfer.qtstreamx.core.model.FundingRate;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.model.Kline;
import com.qtsurfer.qtstreamx.core.model.Ticker;
import java.math.BigDecimal;

/**
 * Parsers for Bybit WebSocket v5 payloads ({@code stream.bybit.com/v5/public/{spot,linear}}).
 *
 * <p>Each adapter takes the {@code data} node of the WS envelope (not the envelope itself) plus
 * the originating {@link Instrument}. The envelope's {@code topic} field is used upstream in
 * {@link BybitStreamClient} to route to the right adapter; subject parsing lives there, not here.
 */
final class BybitAdapters {

    private BybitAdapters() {}

    /**
     * Parse a spot {@code tickers.<symbol>} snapshot or delta. Bybit spot tickers do NOT carry
     * bid/ask — that stream is BBO-free and only emits last/24h stats. Callers that need bid/ask
     * for spot must subscribe to the separate {@code orderbook.1.<symbol>} channel.
     *
     * <p>Bybit sends deltas with only the changed fields populated, so missing fields become null
     * on the {@link Ticker} record rather than 0 — downstream consumers know the difference
     * between "we didn't observe this yet" and "this is zero".
     */
    static Ticker adaptTickerSpot(Instrument instrument, JsonNode data, long tsMs) {
        return new Ticker(
                instrument,
                /* bid */ null,
                /* bidSize */ null,
                /* ask */ null,
                /* askSize */ null,
                decimal(data, "lastPrice"),
                decimal(data, "prevPrice24h"),
                decimal(data, "highPrice24h"),
                decimal(data, "lowPrice24h"),
                decimal(data, "volume24h"),
                decimal(data, "turnover24h"),
                tsMs * 1_000L);
    }

    /**
     * Parse a linear/inverse {@code tickers.<symbol>} message. Linear tickers include bid1/ask1
     * prices and sizes, so we can fill the full {@link Ticker} record. The envelope's top-level
     * {@code ts} is used for the timestamp; the per-tick snapshot doesn't have its own timestamp
     * in the v5 schema.
     */
    static Ticker adaptTickerLinear(Instrument instrument, JsonNode data, long tsMs) {
        return new Ticker(
                instrument,
                decimal(data, "bid1Price"),
                decimal(data, "bid1Size"),
                decimal(data, "ask1Price"),
                decimal(data, "ask1Size"),
                decimal(data, "lastPrice"),
                decimal(data, "prevPrice24h"),
                decimal(data, "highPrice24h"),
                decimal(data, "lowPrice24h"),
                decimal(data, "volume24h"),
                decimal(data, "turnover24h"),
                tsMs * 1_000L);
    }

    /**
     * Parse a {@code tickers.<symbol>} message from the linear stream into a {@link FundingRate}.
     *
     * <p>Bybit USDⓈ-M perps settle funding every 8h by default, but a handful of high-volume
     * pairs (BTCUSDT, ETHUSDT, SOLUSDT, …) settle every 4h. We can't tell from the tickers
     * message alone, so we pass the {@code intervalHours} in — callers look it up in
     * {@link BybitInstrumentsCache} at subscribe time.
     *
     * <p>{@code nextFundingTime} is milliseconds in the wire format; we convert to µs for the
     * record. Returns null when the data doesn't include a funding rate (e.g. inverse markets on
     * a stream that disables funding, or a partial snapshot that omits the field).
     */
    static FundingRate adaptFundingRate(
            Instrument instrument, JsonNode data, int intervalHours, long tsMs) {
        BigDecimal rate = decimal(data, "fundingRate");
        if (rate == null) {
            return null;
        }
        long nextFundingMs = longOrZero(data, "nextFundingTime");
        return new FundingRate(
                instrument,
                rate,
                decimal(data, "markPrice"),
                nextFundingMs * 1_000L,
                intervalHours,
                tsMs * 1_000L);
    }

    /**
     * Parse one element of a {@code kline.<interval>.<symbol>} payload. Bybit's {@code data} is
     * an array — callers pass the single entry they want adapted; the caller decides whether to
     * emit both {@code confirm=true} and intermediate {@code confirm=false} ticks.
     */
    static Kline adaptKline(Instrument instrument, JsonNode entry) {
        long startMs = longOrZero(entry, "start");
        long endMs = longOrZero(entry, "end");
        return new Kline(
                instrument,
                entry.path("interval").asText(""),
                decimal(entry, "open"),
                decimal(entry, "high"),
                decimal(entry, "low"),
                decimal(entry, "close"),
                decimal(entry, "volume"),
                decimal(entry, "turnover"),
                /* numberOfTrades */ 0L,
                entry.path("confirm").asBoolean(false),
                startMs * 1_000L,
                /* closeTime µs */ endMs * 1_000L);
    }

    /* ----- helpers (package-visible for the stream client's delta cache) ----- */

    /** Returns the field as BigDecimal or null when missing/empty/invalid. */
    static BigDecimal optDecimal(JsonNode node, String field) {
        return decimal(node, field);
    }

    /** Returns the field as long or 0 when missing/empty/invalid. */
    static long optLong(JsonNode node, String field) {
        return longOrZero(node, field);
    }

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
}
