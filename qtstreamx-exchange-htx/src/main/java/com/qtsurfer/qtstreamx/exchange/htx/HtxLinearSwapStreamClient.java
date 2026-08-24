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
 * HTX linear swap streaming client (USDⓈ-M perpetuals).
 *
 * <p>HTX splits linear-swap WS across two endpoints:
 * <ul>
 *   <li>{@code wss://api.hbdm.com/linear-swap-ws} — market data ({@code market.<c>.detail},
 *       {@code market.<c>.kline.<p>}). Envelope: {@code {"ch":..,"ts":..,"tick":{..}}}.
 *   <li>{@code wss://api.hbdm.com/linear-swap-notification} — notify-style topics
 *       ({@code public.<c>.funding_rate}). Envelope:
 *       {@code {"op":"notify","topic":..,"ts":..,"data":[{..}]}}.
 * </ul>
 * This client multiplexes both: a WS is opened only if there are subscriptions needing it.
 * Both WS are gzip-compressed; adapter assumes the {@code WebSocketClient} factory inflates
 * frames before dispatch.
 *
 * <p>Funding settles every 8h on HTX linear swaps — hard-coded in
 * {@link HtxAdapters#adaptFundingRate}.
 */
public class HtxLinearSwapStreamClient implements StreamClient {

    private static final Logger log = LoggerFactory.getLogger(HtxLinearSwapStreamClient.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    static final String MARKET_URL = "wss://api.hbdm.com/linear-swap-ws";
    static final String NOTIFY_URL = "wss://api.hbdm.com/linear-swap-notification";

    private final StreamClientConfig config;
    private WebSocketClient marketWs;
    private WebSocketClient notifyWs;

    private final List<String> pendingMarketSubs = new ArrayList<>();
    private final List<String> pendingNotifySubs = new ArrayList<>();
    private final Map<String, Subscription<Ticker>> tickerSubs = new ConcurrentHashMap<>();
    private final Map<String, KlineSubscription> klineSubs = new ConcurrentHashMap<>();
    private final Map<String, Subscription<FundingRate>> fundingSubs = new ConcurrentHashMap<>();

    private volatile Runnable disconnectHandler = () -> {};
    private final AtomicBoolean disconnectFired = new AtomicBoolean(false);
    private int nextId = 1;

    // HTX kicks idle clients after ~30s. The GzipAware transport auto-pongs to the market-style
    // {"ping":ts} pattern but NOT to the notify-style {"op":"ping","ts":ts}, and pongs sent from
    // the transport path don't reach the server reliably through the Recording decorator
    // (observed 23K server pings → 0 pongs visible in canary log, 96 reconnects in 1h).
    // Client-initiated proactive pings on both WS close the gap and ensure keepalive regardless.
    private ScheduledExecutorService pingExec;
    private ScheduledFuture<?> marketPingTask;
    private ScheduledFuture<?> notifyPingTask;

    public HtxLinearSwapStreamClient(StreamClientConfig config) {
        this.config = config;
    }

    @Override
    public void connect() throws Exception {
        if (pendingMarketSubs.isEmpty() && pendingNotifySubs.isEmpty()) {
            throw new IllegalStateException("No subscriptions registered");
        }
        disconnectFired.set(false);
        if (!pendingMarketSubs.isEmpty()) {
            marketWs = config.wsClientFactory().get();
            marketWs.onMessage(this::handleMarketMessage);
            marketWs.onClose((code, reason) -> {
                log.warn("HTX linear-swap market WS closed: {} {}", code, reason);
                fireDisconnect();
            });
            marketWs.onError(err -> {
                log.error("HTX linear-swap market WS error: {}", err.getMessage());
                fireDisconnect();
            });
            log.info("Connecting to HTX linear-swap market with {} topics", pendingMarketSubs.size());
            marketWs.connect(MARKET_URL);
            for (String topic : pendingMarketSubs) {
                marketWs.send("{\"sub\":\"" + topic + "\",\"id\":\"" + (nextId++) + "\"}");
            }
        }
        if (!pendingNotifySubs.isEmpty()) {
            notifyWs = config.wsClientFactory().get();
            notifyWs.onMessage(this::handleNotifyMessage);
            notifyWs.onClose((code, reason) -> {
                log.warn("HTX linear-swap notify WS closed: {} {}", code, reason);
                fireDisconnect();
            });
            notifyWs.onError(err -> {
                log.error("HTX linear-swap notify WS error: {}", err.getMessage());
                fireDisconnect();
            });
            log.info("Connecting to HTX linear-swap notify with {} topics", pendingNotifySubs.size());
            notifyWs.connect(NOTIFY_URL);
            // Notify channel uses "op":"sub" rather than "sub".
            for (String topic : pendingNotifySubs) {
                notifyWs.send(
                        "{\"op\":\"sub\",\"topic\":\"" + topic + "\",\"cid\":\"" + (nextId++) + "\"}");
            }
        }
        startPingLoops();
    }

    private void startPingLoops() {
        stopPingLoops();
        if (marketWs == null && notifyWs == null) return;
        long intervalSec = Math.max(5L, config.pingInterval().getSeconds());
        pingExec = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "htx-linear-ping");
            t.setDaemon(true);
            return t;
        });
        if (marketWs != null) {
            marketPingTask = pingExec.scheduleAtFixedRate(() -> sendMarketPing(),
                    intervalSec, intervalSec, TimeUnit.SECONDS);
        }
        if (notifyWs != null) {
            notifyPingTask = pingExec.scheduleAtFixedRate(() -> sendNotifyPing(),
                    intervalSec, intervalSec, TimeUnit.SECONDS);
        }
    }

    private void stopPingLoops() {
        if (marketPingTask != null) { marketPingTask.cancel(false); marketPingTask = null; }
        if (notifyPingTask != null) { notifyPingTask.cancel(false); notifyPingTask = null; }
        if (pingExec != null) { pingExec.shutdownNow(); pingExec = null; }
    }

    private void sendMarketPing() {
        try {
            if (marketWs != null && marketWs.isOpen()) {
                marketWs.send("{\"ping\":" + System.currentTimeMillis() + "}");
            }
        } catch (RuntimeException e) {
            log.debug("HTX market ping failed: {}", e.getMessage());
        }
    }

    private void sendNotifyPing() {
        try {
            if (notifyWs != null && notifyWs.isOpen()) {
                notifyWs.send("{\"op\":\"ping\",\"ts\":" + System.currentTimeMillis() + "}");
            }
        } catch (RuntimeException e) {
            log.debug("HTX notify ping failed: {}", e.getMessage());
        }
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
        String sym = HtxSymbols.toLinearContract(instrument);
        String topic = "market." + sym + ".detail";
        if (!pendingMarketSubs.contains(topic)) pendingMarketSubs.add(topic);
        tickerSubs.put(topic, new Subscription<>(instrument, handler));
    }

    @Override
    public void subscribeKline(Instrument instrument, String interval, Consumer<Kline> handler) {
        String sym = HtxSymbols.toLinearContract(instrument);
        String topic = "market." + sym + ".kline." + interval;
        if (!pendingMarketSubs.contains(topic)) pendingMarketSubs.add(topic);
        klineSubs.put(topic, new KlineSubscription(instrument, handler, interval));
    }

    @Override
    public void subscribeFundingRate(Instrument instrument, Consumer<FundingRate> handler) {
        if (instrument.settle() == null) {
            throw new UnsupportedOperationException(
                    "Funding rates require a derivatives instrument (settle != null)");
        }
        String sym = HtxSymbols.toLinearContract(instrument);
        String topic = "public." + sym + ".funding_rate";
        if (!pendingNotifySubs.contains(topic)) pendingNotifySubs.add(topic);
        fundingSubs.put(topic, new Subscription<>(instrument, handler));
    }

    @Override
    public boolean isConnected() {
        boolean marketOk = marketWs == null || marketWs.isOpen();
        boolean notifyOk = notifyWs == null || notifyWs.isOpen();
        return (marketWs != null || notifyWs != null) && marketOk && notifyOk;
    }

    @Override
    public void close() throws Exception {
        stopPingLoops();
        if (marketWs != null) marketWs.close();
        if (notifyWs != null) notifyWs.close();
    }

    private void handleMarketMessage(String message) {
        try {
            JsonNode root = MAPPER.readTree(message);
            if (root.has("ping")) {
                // Explicit pong via the StreamClient.send() path so the Recording decorator
                // sees it and the pong routes through the same WS that received the ping.
                long pingTs = root.path("ping").asLong(0L);
                if (marketWs != null && marketWs.isOpen()) {
                    marketWs.send("{\"pong\":" + pingTs + "}");
                }
                return;
            }
            String ch = root.path("ch").asText("");
            if (ch.isEmpty()) return;
            JsonNode tick = root.path("tick");
            if (tick.isMissingNode()) return;
            long tsMs = root.path("ts").asLong(0L);

            Subscription<Ticker> tsub = tickerSubs.get(ch);
            if (tsub != null) {
                tsub.handler().accept(HtxAdapters.adaptLinearDetail(tsub.instrument(), tick, tsMs));
                return;
            }
            KlineSubscription ksub = klineSubs.get(ch);
            if (ksub != null) {
                ksub.handler()
                        .accept(HtxAdapters.adaptKline(ksub.instrument(), ksub.interval(), tick));
            }
        } catch (Exception e) {
            log.error("Failed to parse HTX linear-swap market message: {}", e.getMessage());
        }
    }

    private void handleNotifyMessage(String message) {
        try {
            JsonNode root = MAPPER.readTree(message);
            // Notify channel heartbeat: {"op":"ping","ts":...} — the GzipAware transport's
            // auto-pong only matches {"ping":...}, so we explicitly reply here.
            if ("ping".equals(root.path("op").asText(""))) {
                long pingTs = root.path("ts").asLong(0L);
                if (notifyWs != null && notifyWs.isOpen()) {
                    notifyWs.send("{\"op\":\"pong\",\"ts\":" + pingTs + "}");
                }
                return;
            }
            String topic = root.path("topic").asText("");
            if (topic.isEmpty()) return;
            Subscription<FundingRate> fsub = fundingSubs.get(topic);
            if (fsub == null) return;
            long tsMs = root.path("ts").asLong(0L);
            JsonNode data = root.path("data");
            JsonNode payload;
            if (data.isArray() && data.size() > 0) {
                payload = data.get(0);
            } else if (data.isObject()) {
                payload = data;
            } else {
                return;
            }
            FundingRate fr = HtxAdapters.adaptFundingRate(fsub.instrument(), payload, tsMs);
            if (fr != null) fsub.handler().accept(fr);
        } catch (Exception e) {
            log.error("Failed to parse HTX linear-swap notify message: {}", e.getMessage());
        }
    }

    List<String> pendingMarketSubs() { return Collections.unmodifiableList(pendingMarketSubs); }

    List<String> pendingNotifySubs() { return Collections.unmodifiableList(pendingNotifySubs); }

    private record Subscription<T>(Instrument instrument, Consumer<T> handler) {}

    private record KlineSubscription(Instrument instrument, Consumer<Kline> handler, String interval) {}
}
