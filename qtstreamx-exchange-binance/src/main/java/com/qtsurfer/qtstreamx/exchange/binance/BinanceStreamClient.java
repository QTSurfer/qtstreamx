package com.qtsurfer.qtstreamx.exchange.binance;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.qtsurfer.qtstreamx.core.client.StreamClient;
import com.qtsurfer.qtstreamx.core.client.StreamClientConfig;
import com.qtsurfer.qtstreamx.core.model.FundingRate;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.model.Kline;
import com.qtsurfer.qtstreamx.core.model.Ticker;
import com.qtsurfer.qtstreamx.core.ws.WebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Binance WebSocket streaming client.
 *
 * <p>Supports spot and USDT-M futures via combined streams. Subscribes to
 * individual streams per instrument and dispatches normalized records to handlers.
 *
 * <p>The subscription registry keeps the originating {@link Instrument} paired
 * with its handler, so the dispatch path never has to reconstruct the
 * instrument from the WS {@code "s"} field. This avoids the base/quote
 * ambiguity inherent in concat'd symbols like {@code HEITRY} or {@code PEPEBRL}.
 *
 * <p>Binance combined stream URL: {@code wss://stream.binance.com:9443/stream?streams=s1/s2/...}
 *
 * <p>Futures URL: {@code wss://fstream.binance.com/stream?streams=s1/s2/...}
 */
public class BinanceStreamClient implements StreamClient {

    private static final Logger log = LoggerFactory.getLogger(BinanceStreamClient.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String SPOT_WS = "wss://stream.binance.com:9443/stream?streams=";
    // Binance decommissioned the unrouted /stream path on 2026-04-23 — connections without a
    // routed prefix (/public, /market, /private) get only Public endpoint data, which excludes
    // markPrice. /market is the replacement for regular market-data streams on USDⓈ-M futures.
    // Spot uses a different host and was not affected by the change.
    private static final String FUTURES_WS = "wss://fstream.binance.com/market/stream?streams=";

    private final StreamClientConfig config;
    private final boolean futures;
    private WebSocketClient wsClient;

    private final List<String> pendingStreams = new ArrayList<>();
    private final Map<String, Subscription<Ticker>> tickerSubs = new ConcurrentHashMap<>();
    private final Map<String, Subscription<Ticker>> bookTickerSubs = new ConcurrentHashMap<>();
    private final Map<String, Subscription<Kline>> klineSubs = new ConcurrentHashMap<>();
    private final Map<String, Subscription<FundingRate>> fundingSubs = new ConcurrentHashMap<>();
    private volatile Runnable disconnectHandler = () -> {};
    private final AtomicBoolean disconnectFired = new AtomicBoolean(false);

    public BinanceStreamClient(StreamClientConfig config, boolean futures) {
        this.config = config;
        this.futures = futures;
    }

    public static BinanceStreamClient spot(StreamClientConfig config) {
        return new BinanceStreamClient(config, false);
    }

    public static BinanceStreamClient futures(StreamClientConfig config) {
        return new BinanceStreamClient(config, true);
    }

    @Override
    public void connect() throws Exception {
        if (pendingStreams.isEmpty()) {
            throw new IllegalStateException("No subscriptions registered. Subscribe before connecting.");
        }

        wsClient = config.wsClientFactory().get();
        disconnectFired.set(false);
        wsClient.onMessage(this::handleMessage);
        wsClient.onClose((code, reason) -> {
            log.warn("Binance WS closed: {} {}", code, reason);
            fireDisconnect();
        });
        wsClient.onError(err -> {
            log.error("Binance WS error: {}", err.getMessage());
            fireDisconnect();
        });

        String baseUrl = futures ? FUTURES_WS : SPOT_WS;
        String url = baseUrl + String.join("/", pendingStreams);
        log.info("Connecting to Binance {} with {} streams", futures ? "futures" : "spot", pendingStreams.size());
        wsClient.connect(url);
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

    /**
     * Subscribe to the 24hr rolling ticker stream ({@code <symbol>@ticker}, 1000ms rate).
     *
     * <p>Use this as the default ticker source: it includes last, bid/ask with sizes, OHLC and
     * 24h volumes in both base and quote currency. This is the stream that drives downstream
     * OHLC storage and analytics, and the one the existing XChange-based publisher emits today,
     * so wire-level parity is preserved.
     *
     * <p>For an event-driven best-bid/best-ask stream (no rate limit, BBO only), use
     * {@link #subscribeBookTicker(Instrument, Consumer)} — it writes to the same {@link Ticker}
     * record but leaves the OHLC / volume fields null.
     */
    @Override
    public void subscribeTicker(Instrument instrument, Consumer<Ticker> handler) {
        String stream = BinanceSymbols.toStream(instrument) + "@ticker";
        pendingStreams.add(stream);
        tickerSubs.put(stream, new Subscription<>(instrument, handler));
    }

    /**
     * Subscribe to the bookTicker stream ({@code <symbol>@bookTicker}), which fires on every
     * best-bid or best-ask change. High frequency and small payload — bid/bidSize/ask/askSize
     * only, last / OHLC / volume are null. Intended for the future high-frequency BBO channel,
     * not a drop-in replacement for {@link #subscribeTicker}.
     */
    public void subscribeBookTicker(Instrument instrument, Consumer<Ticker> handler) {
        String stream = BinanceSymbols.toStream(instrument) + "@bookTicker";
        pendingStreams.add(stream);
        bookTickerSubs.put(stream, new Subscription<>(instrument, handler));
    }

    @Override
    public void subscribeKline(Instrument instrument, String interval, Consumer<Kline> handler) {
        String stream = BinanceSymbols.toStream(instrument) + "@kline_" + interval;
        pendingStreams.add(stream);
        klineSubs.put(stream, new Subscription<>(instrument, handler));
    }

    @Override
    public void subscribeFundingRate(Instrument instrument, Consumer<FundingRate> handler) {
        if (!futures) {
            throw new UnsupportedOperationException("Funding rates only available on futures streams");
        }
        String stream = BinanceSymbols.toStream(instrument) + "@markPrice@1s";
        pendingStreams.add(stream);
        fundingSubs.put(stream, new Subscription<>(instrument, handler));
    }

    @Override
    public boolean isConnected() {
        return wsClient != null && wsClient.isOpen();
    }

    @Override
    public void close() throws Exception {
        if (wsClient != null) {
            wsClient.close();
        }
    }

    private void handleMessage(String message) {
        try {
            JsonNode root = MAPPER.readTree(message);
            String stream = root.has("stream") ? root.get("stream").asText() : null;
            JsonNode data = root.has("data") ? root.get("data") : root;

            if (stream == null) return;

            Subscription<Ticker> tsub = tickerSubs.get(stream);
            if (tsub != null) {
                tsub.handler().accept(BinanceAdapters.adaptTicker24h(tsub.instrument(), data));
                return;
            }

            Subscription<Ticker> btsub = bookTickerSubs.get(stream);
            if (btsub != null) {
                btsub.handler().accept(BinanceAdapters.adaptBookTicker(btsub.instrument(), data));
                return;
            }

            Subscription<Kline> ksub = klineSubs.get(stream);
            if (ksub != null) {
                ksub.handler().accept(BinanceAdapters.adaptKline(ksub.instrument(), data));
                return;
            }

            Subscription<FundingRate> fsub = fundingSubs.get(stream);
            if (fsub != null) {
                fsub.handler().accept(BinanceAdapters.adaptMarkPrice(fsub.instrument(), data));
                return;
            }

            log.debug("Unhandled stream: {}", stream);
        } catch (Exception e) {
            log.error("Failed to parse Binance message: {}", e.getMessage());
        }
    }

    private record Subscription<T>(Instrument instrument, Consumer<T> handler) {}
}
