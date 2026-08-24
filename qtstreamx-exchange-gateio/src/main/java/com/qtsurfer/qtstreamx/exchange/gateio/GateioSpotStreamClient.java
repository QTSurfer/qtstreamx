package com.qtsurfer.qtstreamx.exchange.gateio;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Gate.io spot streaming client (API v4).
 *
 * <p>URL: {@code wss://api.gateio.ws/ws/v4/}. Subscribe frame carries {@code event:"subscribe"},
 * {@code channel} and {@code payload} (an array of symbols for tickers, {@code [interval, pair]}
 * for candlesticks). Ticker + candlestick channels share the same connection.
 *
 * <p>Spot has no funding rates — {@link #subscribeFundingRate(Instrument, Consumer)} rejects.
 */
public class GateioSpotStreamClient implements StreamClient {

    private static final Logger log = LoggerFactory.getLogger(GateioSpotStreamClient.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String WS_URL = "wss://api.gateio.ws/ws/v4/";

    private final StreamClientConfig config;
    private WebSocketClient wsClient;

    private final List<String> tickerSymbols = new ArrayList<>();
    private final Map<String, List<String>> candleSymbolsByInterval = new HashMap<>();

    private final Map<String, Subscription<Ticker>> tickerSubs = new ConcurrentHashMap<>();
    private final Map<String, KlineSubscription> klineSubs = new ConcurrentHashMap<>();

    private volatile Runnable disconnectHandler = () -> {};
    private final AtomicBoolean disconnectFired = new AtomicBoolean(false);

    public GateioSpotStreamClient(StreamClientConfig config) {
        this.config = config;
    }

    @Override
    public void connect() throws Exception {
        if (tickerSymbols.isEmpty() && candleSymbolsByInterval.isEmpty()) {
            throw new IllegalStateException("No subscriptions registered");
        }
        wsClient = config.wsClientFactory().get();
        disconnectFired.set(false);
        wsClient.onMessage(this::handleMessage);
        wsClient.onClose((code, reason) -> {
            log.warn("Gate.io spot WS closed: {} {}", code, reason);
            fireDisconnect();
        });
        wsClient.onError(err -> {
            log.error("Gate.io spot WS error: {}", err.getMessage());
            fireDisconnect();
        });
        log.info("Connecting to Gate.io spot v4 ({} tickers, {} candle intervals)",
                tickerSymbols.size(), candleSymbolsByInterval.size());
        wsClient.connect(WS_URL);

        if (!tickerSymbols.isEmpty()) {
            wsClient.send(subscribeTickers(tickerSymbols));
        }
        // Candles must go out as one WS frame per pair — Gate.io's spot.candlesticks payload
        // shape is [interval, pair], so we can't batch multiple pairs into a single subscribe.
        // Earlier versions concatenated the per-pair frames with '\n' into one send() call,
        // which the server rejected entirely (no ack, no candle updates).
        for (var e : candleSymbolsByInterval.entrySet()) {
            String interval = e.getKey();
            for (String symbol : e.getValue()) {
                wsClient.send(subscribeCandle(interval, symbol));
            }
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
        String symbol = GateioSymbols.toSymbol(instrument);
        if (!tickerSymbols.contains(symbol)) tickerSymbols.add(symbol);
        tickerSubs.put(symbol, new Subscription<>(instrument, handler));
    }

    @Override
    public void subscribeKline(Instrument instrument, String interval, Consumer<Kline> handler) {
        String symbol = GateioSymbols.toSymbol(instrument);
        candleSymbolsByInterval.computeIfAbsent(interval, k -> new ArrayList<>()).add(symbol);
        klineSubs.put(
                interval + "|" + symbol, new KlineSubscription(instrument, handler, interval));
    }

    @Override
    public void subscribeFundingRate(Instrument instrument, Consumer<FundingRate> handler) {
        throw new UnsupportedOperationException(
                "Gate.io spot has no funding rates — use GateioFuturesStreamClient");
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
            // Subscribe/error envelopes carry event="subscribe"/"error" + no result — filter.
            if (!"update".equals(root.path("event").asText(""))) return;
            String channel = root.path("channel").asText("");
            long tsMs = root.path("time_ms").asLong(root.path("time").asLong(0L) * 1_000L);
            JsonNode result = root.get("result");
            if (result == null || result.isNull()) return;

            switch (channel) {
                case "spot.tickers" -> {
                    String pair = result.path("currency_pair").asText("");
                    Subscription<Ticker> sub = tickerSubs.get(pair);
                    if (sub != null) {
                        sub.handler()
                                .accept(GateioAdapters.adaptSpotTicker(sub.instrument(), result, tsMs));
                    }
                }
                case "spot.candlesticks" -> {
                    String nField = result.path("n").asText("");
                    // n format: "<interval>_<PAIR>" e.g. "1m_BTC_USDT"
                    int underscore = nField.indexOf('_');
                    if (underscore < 0) return;
                    String interval = nField.substring(0, underscore);
                    String pair = nField.substring(underscore + 1);
                    KlineSubscription sub = klineSubs.get(interval + "|" + pair);
                    if (sub != null) {
                        sub.handler()
                                .accept(
                                        GateioAdapters.adaptSpotKline(
                                                sub.instrument(), sub.interval(), result));
                    }
                }
                default -> { /* ignore */ }
            }
        } catch (Exception e) {
            log.error("Failed to parse Gate.io spot message: {}", e.getMessage());
        }
    }

    private static String subscribeTickers(List<String> symbols) {
        return "{\"time\":" + (System.currentTimeMillis() / 1_000L)
                + ",\"channel\":\"spot.tickers\",\"event\":\"subscribe\",\"payload\":"
                + jsonArray(symbols) + "}";
    }

    private static String subscribeCandle(String interval, String symbol) {
        return "{\"time\":" + (System.currentTimeMillis() / 1_000L)
                + ",\"channel\":\"spot.candlesticks\",\"event\":\"subscribe\",\"payload\":[\""
                + interval + "\",\"" + symbol + "\"]}";
    }

    private static String jsonArray(List<String> items) {
        StringBuilder sb = new StringBuilder(2 + items.size() * 12);
        sb.append('[');
        for (int i = 0; i < items.size(); i++) {
            if (i > 0) sb.append(',');
            sb.append('"').append(items.get(i)).append('"');
        }
        sb.append(']');
        return sb.toString();
    }

    List<String> queuedTickerSymbols() { return Collections.unmodifiableList(tickerSymbols); }
    Map<String, List<String>> queuedCandles() { return Collections.unmodifiableMap(candleSymbolsByInterval); }

    private record Subscription<T>(Instrument instrument, Consumer<T> handler) {}

    private record KlineSubscription(Instrument instrument, Consumer<Kline> handler, String interval) {}
}
