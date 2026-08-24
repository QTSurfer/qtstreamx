package com.qtsurfer.qtstreamx.exchange.htx;

import com.fasterxml.jackson.databind.JsonNode;
import com.qtsurfer.qtstreamx.core.model.FundingRate;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.model.Kline;
import com.qtsurfer.qtstreamx.core.model.Ticker;
import java.math.BigDecimal;

/**
 * Parsers for HTX (ex-Huobi) WS payloads. Every HTX WS is gzip-compressed on the wire; we assume
 * the {@link com.qtsurfer.qtstreamx.core.ws.WebSocketClient} implementation inflates
 * frames before calling {@code onMessage}, so adapters get plain JSON.
 *
 * <p>Envelope shape on both spot and linear WS:
 * <pre>{"ch":"market.btcusdt.detail","ts":1234,"tick":{...}}</pre>
 * so callers grab the {@code tick} subtree for tickers and {@code funding_rate}-style channels.
 *
 * <p>Linear swap funding rates settle every 8h on HTX (same as Binance/OKX); hard-coded in
 * {@link #adaptFundingRate}.
 */
final class HtxAdapters {

    private HtxAdapters() {}

    /**
     * Parse a spot {@code market.<sym>.detail} tick. Carries open/close/high/low/vol/amount +
     * count. No BBO on this channel — for bid/ask v2 spot uses {@code market.<sym>.bbo} or
     * {@code market.<sym>.ticker}; this adapter covers the detail channel which is the
     * canonical last-price feed.
     *
     * <p>HTX semantics (counter-intuitive): {@code amount} is traded volume in the BASE currency,
     * {@code vol} is the turnover in the QUOTE currency.
     */
    static Ticker adaptSpotDetail(Instrument instrument, JsonNode tick, long tsMs) {
        return new Ticker(
                instrument,
                /* bid */ null,
                /* bidSize */ null,
                /* ask */ null,
                /* askSize */ null,
                decimal(tick, "close"),
                decimal(tick, "open"),
                decimal(tick, "high"),
                decimal(tick, "low"),
                decimal(tick, "amount"),
                decimal(tick, "vol"),
                tsMs * 1_000L);
    }

    /**
     * Parse a {@code market.<sym>.bbo} tick into a BBO-only {@link Ticker}. This is the sibling
     * of spot detail for strategies that need bid/ask on the spot side.
     */
    static Ticker adaptSpotBbo(Instrument instrument, JsonNode tick, long tsMs) {
        return new Ticker(
                instrument,
                decimal(tick, "bid"),
                decimal(tick, "bidSize"),
                decimal(tick, "ask"),
                decimal(tick, "askSize"),
                /* last */ null,
                /* open */ null,
                /* high */ null,
                /* low */ null,
                /* volume */ null,
                /* quoteVolume */ null,
                tsMs * 1_000L);
    }

    /**
     * Parse a {@code market.<sym>.kline.<period>} tick. HTX kline {@code id} is epoch seconds
     * at the bucket open; {@code amount} is base volume, {@code vol} is quote turnover (same
     * counter-intuitive semantics as spot detail).
     */
    static Kline adaptKline(Instrument instrument, String interval, JsonNode tick) {
        long startSec = longOrZero(tick, "id");
        long startMs = startSec * 1_000L;
        return new Kline(
                instrument,
                interval,
                decimal(tick, "open"),
                decimal(tick, "high"),
                decimal(tick, "low"),
                decimal(tick, "close"),
                decimal(tick, "amount"),
                decimal(tick, "vol"),
                longOrZero(tick, "count"),
                /* closed */ false,
                startMs * 1_000L,
                (startMs + intervalMs(interval)) * 1_000L - 1_000L);
    }

    /**
     * Parse a linear-swap {@code market.<contract>.detail} tick. Same shape as spot but for
     * dash-separated contracts like {@code BTC-USDT}. {@code amount} is base, {@code vol} is
     * quote turnover (also called {@code trade_turnover} in REST).
     */
    static Ticker adaptLinearDetail(Instrument instrument, JsonNode tick, long tsMs) {
        return new Ticker(
                instrument,
                /* bid */ null,
                /* bidSize */ null,
                /* ask */ null,
                /* askSize */ null,
                decimal(tick, "close"),
                decimal(tick, "open"),
                decimal(tick, "high"),
                decimal(tick, "low"),
                decimal(tick, "amount"),
                decimal(tick, "vol"),
                tsMs * 1_000L);
    }

    /**
     * Parse a {@code public.<contract>.funding_rate} payload element. Settlement every 8h on
     * HTX linear swaps. Returns null when {@code funding_rate} is missing (defensive).
     *
     * <p>Payload carries two timestamps: {@code funding_time} (past settlement this rate applies
     * to) and {@code next_funding_time} (upcoming settlement). We surface the upcoming one as
     * {@link FundingRate#nextFundingTime}; when it's missing we fall back to {@code funding_time}.
     */
    static FundingRate adaptFundingRate(Instrument instrument, JsonNode tick, long tsMs) {
        BigDecimal rate = decimal(tick, "funding_rate");
        if (rate == null) return null;
        long nextMs = longOrZero(tick, "next_funding_time");
        if (nextMs == 0L) nextMs = longOrZero(tick, "funding_time");
        return new FundingRate(
                instrument,
                rate,
                /* markPrice — funding channel doesn't carry it; a parallel detail sub fills it */
                null,
                nextMs * 1_000L,
                /* intervalHours */ 8,
                tsMs * 1_000L);
    }

    /* helpers */

    private static BigDecimal decimal(JsonNode node, String field) {
        JsonNode v = node.get(field);
        if (v == null || v.isNull()) return null;
        if (v.isNumber()) return new BigDecimal(v.asText());
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
        // HTX kline intervals: 1min 5min 15min 30min 60min 4hour 1day 1week 1mon
        if (interval == null) return 0L;
        return switch (interval) {
            case "1min" -> 60_000L;
            case "5min" -> 5 * 60_000L;
            case "15min" -> 15 * 60_000L;
            case "30min" -> 30 * 60_000L;
            case "60min" -> 60 * 60_000L;
            case "4hour" -> 4 * 60 * 60_000L;
            case "1day" -> 24 * 60 * 60_000L;
            case "1week" -> 7 * 24 * 60 * 60_000L;
            default -> 0L;
        };
    }
}
