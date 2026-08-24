package com.qtsurfer.qtstreamx.exchange.bitget;

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
 * Bitget WS v2 streaming client.
 *
 * <p>One WS URL ({@code wss://ws.bitget.com/v2/ws/public}) for every product type. Each
 * subscribe arg carries {@code instType}, {@code channel} and {@code instId} so SPOT and
 * USDT-FUTURES share the same connection. USDT-FUTURES tickers carry funding-rate fields
 * inline, so a {@code subscribeTicker} + {@code subscribeFundingRate} pair on a perp feeds
 * both handlers from one subscription.
 *
 * <p>Instantiated per category with {@link Category#SPOT} or {@link Category#USDT_FUTURES};
 * the category controls the {@code instType} on the subscribe wire and whether funding-rate
 * subscriptions are allowed.
 */
public class BitgetStreamClient implements StreamClient {

    private static final Logger log = LoggerFactory.getLogger(BitgetStreamClient.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String WS_URL = "wss://ws.bitget.com/v2/ws/public";

    public enum Category {
        SPOT("SPOT"),
        USDT_FUTURES("USDT-FUTURES");

        final String wireName;

        Category(String wireName) {
            this.wireName = wireName;
        }
    }

    private final StreamClientConfig config;
    private final Category category;
    private WebSocketClient wsClient;

    private final List<SubscribeArg> pendingArgs = new ArrayList<>();
    private final Map<Key, Subscription<Ticker>> tickerSubs = new ConcurrentHashMap<>();
    private final Map<Key, Subscription<FundingRate>> fundingSubs = new ConcurrentHashMap<>();
    private final Map<Key, KlineSubscription> klineSubs = new ConcurrentHashMap<>();

    private volatile Runnable disconnectHandler = () -> {};
    private final AtomicBoolean disconnectFired = new AtomicBoolean(false);
    // Bitget v2 drops the socket after ~150s of client silence (server doesn't send its own
    // pings). Spec requires the CLIENT to send plain-text "ping" every 30s; server replies
    // "pong". Without this, the canary saw one reconnect every ~2.5min.
    private ScheduledExecutorService pingExec;
    private ScheduledFuture<?> pingTask;

    public BitgetStreamClient(StreamClientConfig config, Category category) {
        this.config = config;
        this.category = category;
    }

    public static BitgetStreamClient spot(StreamClientConfig config) {
        return new BitgetStreamClient(config, Category.SPOT);
    }

    public static BitgetStreamClient usdtFutures(StreamClientConfig config) {
        return new BitgetStreamClient(config, Category.USDT_FUTURES);
    }

    @Override
    public void connect() throws Exception {
        if (pendingArgs.isEmpty()) {
            throw new IllegalStateException("No subscriptions registered. Subscribe before connecting.");
        }
        wsClient = config.wsClientFactory().get();
        disconnectFired.set(false);
        wsClient.onMessage(this::handleMessage);
        wsClient.onClose((code, reason) -> {
            log.warn("Bitget WS closed: {} {}", code, reason);
            fireDisconnect();
        });
        wsClient.onError(err -> {
            log.error("Bitget WS error: {}", err.getMessage());
            fireDisconnect();
        });

        log.info("Connecting to Bitget {} with {} subs", category, pendingArgs.size());
        wsClient.connect(WS_URL);

        // v2 accepts up to 50 args per frame; chunk at 30 to be comfortable.
        for (int i = 0; i < pendingArgs.size(); i += 30) {
            List<SubscribeArg> chunk = pendingArgs.subList(i, Math.min(i + 30, pendingArgs.size()));
            wsClient.send(buildSubscribeFrame(chunk));
        }
        startPingLoop();
    }

    private void startPingLoop() {
        stopPingLoop();
        long intervalSec = Math.max(5L, config.pingInterval().getSeconds());
        pingExec = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "bitget-ping");
            t.setDaemon(true);
            return t;
        });
        pingTask = pingExec.scheduleAtFixedRate(() -> {
            try {
                if (wsClient != null && wsClient.isOpen()) {
                    wsClient.send("ping");
                }
            } catch (RuntimeException e) {
                log.debug("Bitget ping failed: {}", e.getMessage());
            }
        }, intervalSec, intervalSec, TimeUnit.SECONDS);
    }

    private void stopPingLoop() {
        if (pingTask != null) { pingTask.cancel(false); pingTask = null; }
        if (pingExec != null) { pingExec.shutdownNow(); pingExec = null; }
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
        String instId = BitgetSymbols.toInstId(instrument);
        addArgOnce("ticker", instId);
        tickerSubs.put(new Key("ticker", instId), new Subscription<>(instrument, handler));
    }

    /**
     * Subscribe to candles. Bitget interval codes are lowercase: {@code "1m"}, {@code "5m"},
     * {@code "15m"}, {@code "1H"}, {@code "4H"}, {@code "1D"}, {@code "1W"}, {@code "1M"}.
     * No 1-second klines on Bitget (minimum is 1m, same as Bybit).
     */
    @Override
    public void subscribeKline(Instrument instrument, String interval, Consumer<Kline> handler) {
        String channel = "candle" + interval;
        String instId = BitgetSymbols.toInstId(instrument);
        addArgOnce(channel, instId);
        klineSubs.put(new Key(channel, instId), new KlineSubscription(instrument, handler, interval));
    }

    @Override
    public void subscribeFundingRate(Instrument instrument, Consumer<FundingRate> handler) {
        if (category != Category.USDT_FUTURES) {
            throw new UnsupportedOperationException(
                    "Funding rates only available on Bitget USDT_FUTURES stream");
        }
        String instId = BitgetSymbols.toInstId(instrument);
        addArgOnce("ticker", instId);
        fundingSubs.put(new Key("ticker", instId), new Subscription<>(instrument, handler));
    }

    @Override
    public boolean isConnected() {
        return wsClient != null && wsClient.isOpen();
    }

    @Override
    public void close() throws Exception {
        stopPingLoop();
        if (wsClient != null) wsClient.close();
    }

    private void handleMessage(String message) {
        // Bitget server replies with the plain text "pong" to our keepalive pings; not JSON.
        if (message == null || message.length() < 3 || "pong".equals(message)) return;
        try {
            JsonNode root = MAPPER.readTree(message);
            JsonNode arg = root.get("arg");
            JsonNode data = root.get("data");
            if (arg == null || data == null || !data.isArray() || data.isEmpty()) return;

            String channel = arg.path("channel").asText("");
            String instId = arg.path("instId").asText("");
            Key key = new Key(channel, instId);

            if ("ticker".equals(channel)) {
                Subscription<Ticker> tsub = tickerSubs.get(key);
                Subscription<FundingRate> fsub = fundingSubs.get(key);
                if (tsub == null && fsub == null) return;
                JsonNode entry = data.get(0);
                if (tsub != null) {
                    tsub.handler().accept(BitgetAdapters.adaptTicker(tsub.instrument(), entry));
                }
                if (fsub != null) {
                    FundingRate fr = BitgetAdapters.adaptFundingRate(fsub.instrument(), entry);
                    if (fr != null) fsub.handler().accept(fr);
                }
                return;
            }

            if (channel.startsWith("candle")) {
                KlineSubscription ksub = klineSubs.get(key);
                if (ksub == null) return;
                for (JsonNode entry : data) {
                    Kline kline =
                            BitgetAdapters.adaptKline(ksub.instrument(), ksub.interval(), entry);
                    if (kline != null) ksub.handler().accept(kline);
                }
            }
        } catch (Exception e) {
            log.error("Failed to parse Bitget message: {}", e.getMessage());
        }
    }

    private void addArgOnce(String channel, String instId) {
        SubscribeArg arg = new SubscribeArg(category.wireName, channel, instId);
        if (!pendingArgs.contains(arg)) pendingArgs.add(arg);
    }

    private static String buildSubscribeFrame(List<SubscribeArg> args) {
        StringBuilder sb = new StringBuilder(32 + args.size() * 80);
        sb.append("{\"op\":\"subscribe\",\"args\":[");
        for (int i = 0; i < args.size(); i++) {
            if (i > 0) sb.append(',');
            SubscribeArg a = args.get(i);
            sb.append("{\"instType\":\"")
                    .append(a.instType())
                    .append("\",\"channel\":\"")
                    .append(a.channel())
                    .append("\",\"instId\":\"")
                    .append(a.instId())
                    .append("\"}");
        }
        sb.append("]}");
        return sb.toString();
    }

    List<SubscribeArg> pendingArgs() {
        return Collections.unmodifiableList(pendingArgs);
    }

    private record Subscription<T>(Instrument instrument, Consumer<T> handler) {}

    private record KlineSubscription(Instrument instrument, Consumer<Kline> handler, String interval) {}

    record SubscribeArg(String instType, String channel, String instId) {}

    private record Key(String channel, String instId) {}
}
