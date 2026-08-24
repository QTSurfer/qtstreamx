package com.qtsurfer.qtstreamx.exchange.kraken;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.qtsurfer.qtstreamx.core.client.StreamClient;
import com.qtsurfer.qtstreamx.core.client.StreamClientConfig;
import com.qtsurfer.qtstreamx.core.model.FundingRate;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.model.Kline;
import com.qtsurfer.qtstreamx.core.model.Ticker;
import com.qtsurfer.qtstreamx.core.ws.WebSocketClient;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kraken spot streaming client (API v2).
 *
 * <p>URL: {@code wss://ws.kraken.com/v2}. Subscription model differs from Binance/Bybit/OKX — a
 * single {@code subscribe} carries a {@code params} object with {@code channel} and a
 * {@code symbol} array; a separate subscribe is needed per channel (ticker, ohlc), but symbols
 * can batch freely.
 *
 * <p>Example subscribe frame:
 *
 * <pre>{@code
 * {"method":"subscribe","params":{"channel":"ticker","symbol":["BTC/USD","ETH/USD"]}}
 * }</pre>
 *
 * <p>Funding rates don't exist on the spot product, so
 * {@link #subscribeFundingRate(Instrument, Consumer)} rejects unconditionally —
 * callers use {@link KrakenFuturesStreamClient} instead.
 */
public class KrakenSpotStreamClient implements StreamClient {

    private static final Logger log = LoggerFactory.getLogger(KrakenSpotStreamClient.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String WS_URL = "wss://ws.kraken.com/v2";

    private final StreamClientConfig config;
    private WebSocketClient wsClient;

    // Channel → list of symbols queued pre-connect. Kraken accepts multi-symbol subscribes.
    private final List<String> tickerSymbols = new ArrayList<>();
    private final Map<Integer, List<String>> ohlcSymbolsByInterval = new java.util.HashMap<>();

    private final Map<String, Subscription<Ticker>> tickerSubs = new ConcurrentHashMap<>();
    private final Map<String, KlineSubscription> klineSubs = new ConcurrentHashMap<>();

    private volatile Runnable disconnectHandler = () -> {};
    private final AtomicBoolean disconnectFired = new AtomicBoolean(false);

    public KrakenSpotStreamClient(StreamClientConfig config) {
        this.config = config;
    }

    @Override
    public void connect() throws Exception {
        if (tickerSymbols.isEmpty() && ohlcSymbolsByInterval.isEmpty()) {
            throw new IllegalStateException("No subscriptions registered. Subscribe before connecting.");
        }
        wsClient = config.wsClientFactory().get();
        disconnectFired.set(false);
        wsClient.onMessage(this::handleMessage);
        wsClient.onClose((code, reason) -> {
            log.warn("Kraken spot WS closed: {} {}", code, reason);
            fireDisconnect();
        });
        wsClient.onError(err -> {
            log.error("Kraken spot WS error: {}", err.getMessage());
            fireDisconnect();
        });

        log.info("Connecting to Kraken spot v2 ({} tickers, {} ohlc intervals)",
                tickerSymbols.size(), ohlcSymbolsByInterval.size());
        wsClient.connect(WS_URL);

        if (!tickerSymbols.isEmpty()) {
            wsClient.send(subscribeFrame("ticker", tickerSymbols, null));
        }
        for (var e : ohlcSymbolsByInterval.entrySet()) {
            wsClient.send(subscribeFrame("ohlc", e.getValue(), e.getKey()));
        }
    }

    @Override
    public void onDisconnect(Runnable handler) {
        this.disconnectHandler = handler;
    }

    private void fireDisconnect() {
        if (disconnectFired.compareAndSet(false, true)) {
            try {
                disconnectHandler.run();
            } catch (RuntimeException e) {
                log.error("disconnect handler threw: {}", e.getMessage(), e);
            }
        }
    }

    @Override
    public void subscribeTicker(Instrument instrument, Consumer<Ticker> handler) {
        String symbol = KrakenSymbols.toSpotSymbol(instrument);
        if (!tickerSymbols.contains(symbol)) {
            tickerSymbols.add(symbol);
        }
        tickerSubs.put(symbol, new Subscription<>(instrument, handler));
    }

    /**
     * Subscribe to OHLC. Kraken intervals are integer minutes: 1, 5, 15, 30, 60, 240, 1440,
     * 10080, 21600. Callers pass the minutes as a string (e.g. {@code "1"}, {@code "1m"} —
     * the non-digit suffix is stripped).
     */
    @Override
    public void subscribeKline(Instrument instrument, String interval, Consumer<Kline> handler) {
        int minutes = parseMinutes(interval);
        String symbol = KrakenSymbols.toSpotSymbol(instrument);
        ohlcSymbolsByInterval
                .computeIfAbsent(minutes, k -> new ArrayList<>())
                .add(symbol);
        klineSubs.put(symbol + "|" + minutes, new KlineSubscription(instrument, handler, interval));
    }

    @Override
    public void subscribeFundingRate(Instrument instrument, Consumer<FundingRate> handler) {
        throw new UnsupportedOperationException(
                "Kraken spot has no funding rates — use KrakenFuturesStreamClient");
    }

    @Override
    public boolean isConnected() {
        return wsClient != null && wsClient.isOpen();
    }

    @Override
    public void close() throws Exception {
        if (wsClient != null) wsClient.close();
    }

    private void handleMessage(String message) {
        try {
            JsonNode root = MAPPER.readTree(message);
            // Ignore status / subscribe-ack frames — they carry "method":"subscribe" with no data.
            if (!root.has("channel") || !root.has("data")) return;

            String channel = root.get("channel").asText();
            JsonNode data = root.get("data");
            if (!data.isArray() || data.isEmpty()) return;

            switch (channel) {
                case "ticker" -> {
                    long tsMs = KrakenAdapters.parseIsoMs(root.path("timestamp").asText(""));
                    for (JsonNode entry : data) {
                        String symbol = entry.path("symbol").asText("");
                        Subscription<Ticker> sub = tickerSubs.get(symbol);
                        if (sub != null) {
                            sub.handler()
                                    .accept(KrakenAdapters.adaptTickerSpot(sub.instrument(), entry, tsMs));
                        }
                    }
                }
                case "ohlc" -> {
                    for (JsonNode entry : data) {
                        String symbol = entry.path("symbol").asText("");
                        int minutes = entry.path("interval").asInt(0);
                        KlineSubscription sub = klineSubs.get(symbol + "|" + minutes);
                        if (sub != null) {
                            sub.handler()
                                    .accept(
                                            KrakenAdapters.adaptKlineSpot(
                                                    sub.instrument(), sub.interval(), entry));
                        }
                    }
                }
                default -> { /* ignore unsubscribed channels */ }
            }
        } catch (Exception e) {
            log.error("Failed to parse Kraken spot message: {}", e.getMessage());
        }
    }

    private static String subscribeFrame(String channel, List<String> symbols, Integer interval) {
        StringBuilder sb = new StringBuilder(64 + symbols.size() * 16);
        sb.append("{\"method\":\"subscribe\",\"params\":{\"channel\":\"").append(channel);
        sb.append("\",\"symbol\":[");
        for (int i = 0; i < symbols.size(); i++) {
            if (i > 0) sb.append(',');
            sb.append('"').append(symbols.get(i)).append('"');
        }
        sb.append(']');
        if (interval != null) {
            sb.append(",\"interval\":").append(interval);
        }
        sb.append("}}");
        return sb.toString();
    }

    private static int parseMinutes(String interval) {
        if (interval == null || interval.isEmpty()) return 1;
        StringBuilder digits = new StringBuilder();
        for (char c : interval.toCharArray()) {
            if (Character.isDigit(c)) digits.append(c);
        }
        if (digits.length() == 0) return 1;
        try {
            return Integer.parseInt(digits.toString());
        } catch (NumberFormatException e) {
            return 1;
        }
    }

    /* Package-private visibility for tests. */
    List<String> queuedTickerSymbols() {
        return Collections.unmodifiableList(tickerSymbols);
    }

    Map<Integer, List<String>> queuedOhlcSymbols() {
        return Collections.unmodifiableMap(ohlcSymbolsByInterval);
    }

    private record Subscription<T>(Instrument instrument, Consumer<T> handler) {}

    private record KlineSubscription(Instrument instrument, Consumer<Kline> handler, String interval) {}
}
