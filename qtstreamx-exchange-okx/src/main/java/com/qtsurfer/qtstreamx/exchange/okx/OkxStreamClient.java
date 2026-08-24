package com.qtsurfer.qtstreamx.exchange.okx;

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
 * OKX WebSocket v5 streaming client.
 *
 * <p>OKX v5 splits channels across TWO WS endpoints:
 * <ul>
 *   <li>{@code wss://ws.okx.com:8443/ws/v5/public} — tickers, books, trades, funding-rate.
 *   <li>{@code wss://ws.okx.com:8443/ws/v5/business} — candle*, mark-price-candle*,
 *       tickers-usdt, and other "business" channels.
 * </ul>
 * Trying to subscribe {@code candle1m} on {@code /public} returns error 60018 ("Wrong URL or
 * channel: candle1m, ... doesn't exist"). This client multiplexes both; each WS is opened
 * only if the subscription bucket for it is non-empty.
 *
 * <p>Subscribe payload (same on both endpoints):
 *
 * <pre>{@code
 * {"op":"subscribe","args":[
 *   {"channel":"tickers","instId":"BTC-USDT"},
 *   {"channel":"funding-rate","instId":"BTC-USDT-SWAP"}
 * ]}
 * }</pre>
 *
 * <p>OKX idle timeout is 30s; organic traffic on {@code tickers} keeps the public WS warm.
 * For the business WS, candle updates tick once per minute — a client-initiated ping every 25s
 * would be safer, but the gzip-aware WS factory's transport-level keepalive covers this today.
 */
public class OkxStreamClient implements StreamClient {

    private static final Logger log = LoggerFactory.getLogger(OkxStreamClient.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    static final String PUBLIC_WS = "wss://ws.okx.com:8443/ws/v5/public";
    static final String BUSINESS_WS = "wss://ws.okx.com:8443/ws/v5/business";

    private final StreamClientConfig config;
    private WebSocketClient publicWs;
    private WebSocketClient businessWs;

    private final List<SubscribeArg> pendingPublicArgs = new ArrayList<>();
    private final List<SubscribeArg> pendingBusinessArgs = new ArrayList<>();
    // Routing tables keyed by (channel, instId) → Subscription.
    private final Map<Key, Subscription<Ticker>> tickerSubs = new ConcurrentHashMap<>();
    private final Map<Key, KlineSubscription> klineSubs = new ConcurrentHashMap<>();
    private final Map<Key, Subscription<FundingRate>> fundingSubs = new ConcurrentHashMap<>();

    private volatile Runnable disconnectHandler = () -> {};
    private final AtomicBoolean disconnectFired = new AtomicBoolean(false);

    public OkxStreamClient(StreamClientConfig config) {
        this.config = config;
    }

    @Override
    public void connect() throws Exception {
        if (pendingPublicArgs.isEmpty() && pendingBusinessArgs.isEmpty()) {
            throw new IllegalStateException("No subscriptions registered. Subscribe before connecting.");
        }
        disconnectFired.set(false);
        if (!pendingPublicArgs.isEmpty()) {
            publicWs = openWs("public", PUBLIC_WS, pendingPublicArgs);
        }
        if (!pendingBusinessArgs.isEmpty()) {
            businessWs = openWs("business", BUSINESS_WS, pendingBusinessArgs);
        }
    }

    private WebSocketClient openWs(String tag, String url, List<SubscribeArg> args) throws Exception {
        WebSocketClient ws = config.wsClientFactory().get();
        ws.onMessage(this::handleMessage);
        ws.onClose((code, reason) -> {
            log.warn("OKX {} WS closed: {} {}", tag, code, reason);
            fireDisconnect();
        });
        ws.onError(err -> {
            log.error("OKX {} WS error: {}", tag, err.getMessage());
            fireDisconnect();
        });
        log.info("Connecting to OKX {} with {} topic subscriptions", tag, args.size());
        ws.connect(url);
        // OKX caps subscribe args at ~4096 bytes per frame. 50 topics × ~70 bytes each is
        // comfortably inside; chunk defensively at 50.
        for (int i = 0; i < args.size(); i += 50) {
            List<SubscribeArg> chunk = args.subList(i, Math.min(i + 50, args.size()));
            ws.send(buildSubscribeFrame(chunk));
        }
        return ws;
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
        String instId = OkxSymbols.toInstId(instrument);
        addArg(pendingPublicArgs, "tickers", instId);
        tickerSubs.put(new Key("tickers", instId), new Subscription<>(instrument, handler));
    }

    /**
     * Subscribe to klines. OKX channel names are {@code candle<interval>} where interval is one
     * of {@code 1s, 1m, 3m, 5m, 15m, 30m, 1H, 2H, 4H, 6H, 12H, 1D, 1W, 1M}. Callers pass the
     * full interval spec ({@code "1s"}, {@code "1m"}), not a bare number — the channel name is
     * derived from it. Candle channels are routed to the business WS endpoint.
     */
    @Override
    public void subscribeKline(Instrument instrument, String interval, Consumer<Kline> handler) {
        String channel = "candle" + interval;
        String instId = OkxSymbols.toInstId(instrument);
        addArg(pendingBusinessArgs, channel, instId);
        klineSubs.put(new Key(channel, instId), new KlineSubscription(instrument, handler, interval));
    }

    /**
     * Subscribe to the {@code funding-rate} channel for a perp. OKX perpetual swaps settle
     * every 8h; a single per-instrument subscription is enough, no funding-interval lookup
     * needed.
     */
    @Override
    public void subscribeFundingRate(Instrument instrument, Consumer<FundingRate> handler) {
        if (instrument.settle() == null) {
            throw new UnsupportedOperationException(
                    "Funding rates require a derivatives instrument (settle != null)");
        }
        String instId = OkxSymbols.toInstId(instrument);
        addArg(pendingPublicArgs, "funding-rate", instId);
        fundingSubs.put(new Key("funding-rate", instId), new Subscription<>(instrument, handler));
    }

    @Override
    public boolean isConnected() {
        boolean publicOk = publicWs == null || publicWs.isOpen();
        boolean businessOk = businessWs == null || businessWs.isOpen();
        return (publicWs != null || businessWs != null) && publicOk && businessOk;
    }

    @Override
    public void close() throws Exception {
        if (publicWs != null) publicWs.close();
        if (businessWs != null) businessWs.close();
    }

    private void handleMessage(String message) {
        try {
            JsonNode root = MAPPER.readTree(message);
            JsonNode arg = root.get("arg");
            JsonNode data = root.get("data");
            if (arg == null || data == null || !data.isArray()) return;

            String channel = arg.path("channel").asText("");
            String instId = arg.path("instId").asText("");
            Key key = new Key(channel, instId);

            Subscription<Ticker> tsub = tickerSubs.get(key);
            if (tsub != null && !data.isEmpty()) {
                tsub.handler().accept(OkxAdapters.adaptTicker(tsub.instrument(), data.get(0)));
                return;
            }

            Subscription<FundingRate> fsub = fundingSubs.get(key);
            if (fsub != null && !data.isEmpty()) {
                FundingRate fr =
                        OkxAdapters.adaptFundingRate(fsub.instrument(), data.get(0));
                if (fr != null) fsub.handler().accept(fr);
                return;
            }

            KlineSubscription ksub = klineSubs.get(key);
            if (ksub != null) {
                for (JsonNode entry : data) {
                    Kline kline = OkxAdapters.adaptKline(ksub.instrument(), ksub.interval(), entry);
                    if (kline != null) ksub.handler().accept(kline);
                }
            }
        } catch (Exception e) {
            log.error("Failed to parse OKX message: {}", e.getMessage());
        }
    }

    /** Add a subscribe arg, de-duplicating by (channel, instId). */
    private static void addArg(List<SubscribeArg> bucket, String channel, String instId) {
        SubscribeArg arg = new SubscribeArg(channel, instId);
        if (!bucket.contains(arg)) {
            bucket.add(arg);
        }
    }

    private static String buildSubscribeFrame(List<SubscribeArg> args) {
        StringBuilder sb = new StringBuilder(32 + args.size() * 60);
        sb.append("{\"op\":\"subscribe\",\"args\":[");
        for (int i = 0; i < args.size(); i++) {
            if (i > 0) sb.append(',');
            SubscribeArg a = args.get(i);
            sb.append("{\"channel\":\"")
                    .append(a.channel())
                    .append("\",\"instId\":\"")
                    .append(a.instId())
                    .append("\"}");
        }
        sb.append("]}");
        return sb.toString();
    }

    /** Package-private for tests that inspect queued args. */
    List<SubscribeArg> pendingPublicArgs() {
        return Collections.unmodifiableList(pendingPublicArgs);
    }

    /** Package-private for tests that inspect queued args. */
    List<SubscribeArg> pendingBusinessArgs() {
        return Collections.unmodifiableList(pendingBusinessArgs);
    }

    private record Subscription<T>(Instrument instrument, Consumer<T> handler) {}

    private record KlineSubscription(Instrument instrument, Consumer<Kline> handler, String interval) {}

    record SubscribeArg(String channel, String instId) {}

    private record Key(String channel, String instId) {}
}
