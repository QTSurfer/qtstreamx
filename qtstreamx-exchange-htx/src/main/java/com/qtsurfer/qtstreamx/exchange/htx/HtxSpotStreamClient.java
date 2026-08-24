package com.qtsurfer.qtstreamx.exchange.htx;

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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HTX spot v2 streaming client.
 *
 * <p>URL: {@code wss://api.huobi.pro/ws} (alias {@code wss://api.htx.com/ws}). Subscribe via
 * one topic per frame: {@code {"sub":"market.<sym>.detail","id":"<n>"}}. HTX compresses every
 * frame with GZIP; the {@code WebSocketClient} factory passed in via {@link StreamClientConfig}
 * is expected to inflate before delivering to {@link #handleMessage} — adapters and the client
 * run on plain JSON strings.
 *
 * <p>Server heartbeats arrive as {@code {"ping":<epoch>}}; the factory-provided WS client
 * should echo {@code {"pong":<epoch>}} back to keep the socket alive (not done in this class,
 * it's a transport concern).
 *
 * <p>Spot has no funding rates — {@link #subscribeFundingRate(Instrument, Consumer)} rejects.
 */
public class HtxSpotStreamClient implements StreamClient {

    private static final Logger log = LoggerFactory.getLogger(HtxSpotStreamClient.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String WS_URL = "wss://api.huobi.pro/ws";

    private final StreamClientConfig config;
    private WebSocketClient wsClient;

    private final List<String> pendingSubs = new ArrayList<>();
    private final Map<String, Subscription<Ticker>> tickerSubs = new ConcurrentHashMap<>();
    private final Map<String, KlineSubscription> klineSubs = new ConcurrentHashMap<>();

    private volatile Runnable disconnectHandler = () -> {};
    private final AtomicBoolean disconnectFired = new AtomicBoolean(false);
    private int nextId = 1;

    // Client-initiated keepalive — HTX idles at 30s and the transport auto-pong path didn't
    // propagate through the Recording decorator in canary observations. Proactive + explicit
    // manual pong in handleMessage guarantees the server sees us every interval.
    private ScheduledExecutorService pingExec;
    private ScheduledFuture<?> pingTask;

    public HtxSpotStreamClient(StreamClientConfig config) {
        this.config = config;
    }

    @Override
    public void connect() throws Exception {
        if (pendingSubs.isEmpty()) {
            throw new IllegalStateException("No subscriptions registered");
        }
        wsClient = config.wsClientFactory().get();
        disconnectFired.set(false);
        wsClient.onMessage(this::handleMessage);
        wsClient.onClose((code, reason) -> {
            log.warn("HTX spot WS closed: {} {}", code, reason);
            fireDisconnect();
        });
        wsClient.onError(err -> {
            log.error("HTX spot WS error: {}", err.getMessage());
            fireDisconnect();
        });
        log.info("Connecting to HTX spot with {} topics", pendingSubs.size());
        wsClient.connect(WS_URL);
        // One frame per sub — v2 doesn't batch.
        for (String topic : pendingSubs) {
            wsClient.send("{\"sub\":\"" + topic + "\",\"id\":\"" + (nextId++) + "\"}");
        }
        startPingLoop();
    }

    private void startPingLoop() {
        stopPingLoop();
        long intervalSec = Math.max(5L, config.pingInterval().getSeconds());
        pingExec = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "htx-spot-ping");
            t.setDaemon(true);
            return t;
        });
        pingTask = pingExec.scheduleAtFixedRate(() -> {
            try {
                if (wsClient != null && wsClient.isOpen()) {
                    wsClient.send("{\"ping\":" + System.currentTimeMillis() + "}");
                }
            } catch (RuntimeException e) {
                log.debug("HTX spot ping failed: {}", e.getMessage());
            }
        }, intervalSec, intervalSec, TimeUnit.SECONDS);
    }

    private void stopPingLoop() {
        if (pingTask != null) { pingTask.cancel(false); pingTask = null; }
        if (pingExec != null) { pingExec.shutdownNow(); pingExec = null; }
    }

    @Override
    public void onDisconnect(Runnable handler) { this.disconnectHandler = handler; }

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
        String sym = HtxSymbols.toSpotSymbol(instrument);
        String topic = "market." + sym + ".detail";
        if (!pendingSubs.contains(topic)) pendingSubs.add(topic);
        tickerSubs.put(topic, new Subscription<>(instrument, handler));
    }

    /**
     * HTX kline periods are {@code 1min 5min 15min 30min 60min 4hour 1day 1week 1mon}.
     * No 1-second cadence on HTX.
     */
    @Override
    public void subscribeKline(Instrument instrument, String interval, Consumer<Kline> handler) {
        String sym = HtxSymbols.toSpotSymbol(instrument);
        String topic = "market." + sym + ".kline." + interval;
        if (!pendingSubs.contains(topic)) pendingSubs.add(topic);
        klineSubs.put(topic, new KlineSubscription(instrument, handler, interval));
    }

    @Override
    public void subscribeFundingRate(Instrument instrument, Consumer<FundingRate> handler) {
        throw new UnsupportedOperationException(
                "HTX spot has no funding rates — use HtxLinearSwapStreamClient");
    }

    @Override
    public boolean isConnected() { return wsClient != null && wsClient.isOpen(); }

    @Override
    public void close() throws Exception {
        stopPingLoop();
        if (wsClient != null) wsClient.close();
    }

    private void handleMessage(String message) {
        try {
            JsonNode root = MAPPER.readTree(message);
            if (root.has("ping")) {
                // Explicit pong via StreamClient.send() so the Recording decorator sees it.
                long pingTs = root.path("ping").asLong(0L);
                if (wsClient != null && wsClient.isOpen()) {
                    wsClient.send("{\"pong\":" + pingTs + "}");
                }
                return;
            }
            String ch = root.path("ch").asText("");
            if (ch.isEmpty()) return; // subscribe ack / status frames
            JsonNode tick = root.path("tick");
            if (tick.isMissingNode()) return;
            long tsMs = root.path("ts").asLong(0L);

            Subscription<Ticker> tsub = tickerSubs.get(ch);
            if (tsub != null) {
                tsub.handler().accept(HtxAdapters.adaptSpotDetail(tsub.instrument(), tick, tsMs));
                return;
            }
            KlineSubscription ksub = klineSubs.get(ch);
            if (ksub != null) {
                ksub.handler()
                        .accept(HtxAdapters.adaptKline(ksub.instrument(), ksub.interval(), tick));
            }
        } catch (Exception e) {
            log.error("Failed to parse HTX spot message: {}", e.getMessage());
        }
    }

    List<String> pendingSubs() { return Collections.unmodifiableList(pendingSubs); }

    private record Subscription<T>(Instrument instrument, Consumer<T> handler) {}

    private record KlineSubscription(Instrument instrument, Consumer<Kline> handler, String interval) {}
}
