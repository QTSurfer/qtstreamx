package com.qtsurfer.qtstreamx.exchange.binance;

import com.fasterxml.jackson.databind.JsonNode;
import com.qtsurfer.qtstreamx.core.model.FundingRate;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.model.Kline;
import com.qtsurfer.qtstreamx.core.model.Ticker;

import java.math.BigDecimal;

/**
 * Adapts Binance WS JSON messages to QTStreamX model records.
 *
 * <p>Each adapter receives the {@link Instrument} from the caller — typically
 * the {@code BinanceStreamClient} subscription registry whose instruments come
 * from {@code BinanceInstrumentsCache}/{@code exchangeInfo}. We deliberately
 * do <b>not</b> reconstruct the instrument from the WS {@code "s"} field,
 * because that concat'd symbol is ambiguous (e.g. {@code HEITRY} could be
 * {@code HEI/TRY} or {@code HEIT/RY}, and suffix heuristics misfire on every
 * new fiat listing).
 */
final class BinanceAdapters {

    private BinanceAdapters() {}

    /**
     * Parse a bookTicker message into a Ticker.
     *
     * <pre>{@code
     * {"u":12345,"s":"BTCUSDT","b":"70123.00","B":"1.5","a":"70124.00","A":"2.0"}
     * }</pre>
     */
    static Ticker adaptBookTicker(Instrument instrument, JsonNode data) {
        return new Ticker(
                instrument,
                decimal(data, "b"),
                decimal(data, "B"),
                decimal(data, "a"),
                decimal(data, "A"),
                null,   // last — not in bookTicker
                null, null, null, null, null,
                microsNow()
        );
    }

    /**
     * Parse a 24hr ticker message into a Ticker.
     *
     * <pre>{@code
     * {"e":"24hrTicker","s":"BTCUSDT","c":"70123.50","b":"70123.00","B":"1.5",
     *  "a":"70124.00","A":"2.0","o":"69500.00","h":"70500.00","l":"69200.00",
     *  "v":"12345.67","q":"865432100.00","E":1711238400123}
     * }</pre>
     */
    static Ticker adaptTicker24h(Instrument instrument, JsonNode data) {
        return new Ticker(
                instrument,
                decimal(data, "b"),
                decimal(data, "B"),
                decimal(data, "a"),
                decimal(data, "A"),
                decimal(data, "c"),   // last/close
                decimal(data, "o"),
                decimal(data, "h"),
                decimal(data, "l"),
                decimal(data, "v"),
                decimal(data, "q"),
                data.get("E").asLong() * 1000L  // event time ms → µs
        );
    }

    /**
     * Parse a kline message into a Kline.
     *
     * <pre>{@code
     * {"e":"kline","s":"BTCUSDT","k":{"t":1672515780000,"T":1672515839999,
     *  "s":"BTCUSDT","i":"1m","o":"0.0010","c":"0.0020","h":"0.0025",
     *  "l":"0.0015","v":"1000","q":"1.0000","x":false}}
     * }</pre>
     */
    static Kline adaptKline(Instrument instrument, JsonNode data) {
        JsonNode k = data.get("k");
        JsonNode nNode = k.get("n");
        long numberOfTrades = nNode != null && nNode.isNumber() ? nNode.asLong() : 0L;
        return new Kline(
                instrument,
                k.get("i").asText(),
                decimal(k, "o"),
                decimal(k, "h"),
                decimal(k, "l"),
                decimal(k, "c"),
                decimal(k, "v"),
                decimal(k, "q"),
                numberOfTrades,
                k.get("x").asBoolean(),
                k.get("t").asLong() * 1000L,  // open time ms → µs
                k.get("T").asLong() * 1000L   // close time ms → µs
        );
    }

    /**
     * Parse a markPriceUpdate message into a FundingRate.
     *
     * <pre>{@code
     * {"e":"markPriceUpdate","s":"BTCUSDT","p":"70122.50","i":"70120.00",
     *  "r":"0.00010000","T":1711267200000,"E":1711238400123}
     * }</pre>
     */
    static FundingRate adaptMarkPrice(Instrument instrument, JsonNode data) {
        return new FundingRate(
                instrument,
                decimal(data, "r"),
                decimal(data, "p"),   // mark price
                data.get("T").asLong() * 1000L,  // next funding time ms → µs
                8,  // Binance default funding interval
                data.get("E").asLong() * 1000L   // event time ms → µs
        );
    }

    private static BigDecimal decimal(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull() || value.asText().isEmpty()) return null;
        return new BigDecimal(value.asText());
    }

    private static long microsNow() {
        return System.currentTimeMillis() * 1000L;
    }
}
