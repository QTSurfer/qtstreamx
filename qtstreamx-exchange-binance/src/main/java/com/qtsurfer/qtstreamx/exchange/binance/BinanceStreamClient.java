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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
 *
 * <p>The stream list is read from the connection URL, so a stream registered while the socket is
 * already open would otherwise take effect only at the next reconnection. Such streams are sent as
 * a live {@code SUBSCRIBE} request instead: coalesced for a short moment, split into frames of at
 * most {@value #LIVE_CHUNK} streams and spaced so the connection stays well under Binance's cap of
 * five control messages per second. A request that cannot be sent (socket closed) is dropped on
 * purpose: the stream is already in the registry and joins the URL of the next connection.
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
    private volatile WebSocketClient wsClient;

    /** Streams per live SUBSCRIBE frame. */
    static final int LIVE_CHUNK = 50;

    private final Object streamsLock = new Object();
    // Number of entries of pendingStreams already asked of the server, by the URL or by a live
    // request. -1 until connect() builds the URL.
    private int requestedStreamCount = -1;
    private final Set<String> requestedNames = ConcurrentHashMap.newKeySet();
    // Live frames sent and not yet answered, by request id, to name the streams of a rejected one.
    private final Map<Long, List<String>> liveInFlight = new ConcurrentHashMap<>();
    private final Queue<String> liveQueue = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean liveFlushRunning = new AtomicBoolean(false);
    private final AtomicLong requestId = new AtomicLong();
    private final Duration liveCoalesce;
    private final Duration liveFrameGap;

    private final List<String> pendingStreams = new ArrayList<>();
    private final Map<String, Subscription<Ticker>> tickerSubs = new ConcurrentHashMap<>();
    private final Map<String, Subscription<Ticker>> bookTickerSubs = new ConcurrentHashMap<>();
    private final Map<String, Subscription<Kline>> klineSubs = new ConcurrentHashMap<>();
    private final Map<String, Subscription<FundingRate>> fundingSubs = new ConcurrentHashMap<>();
    private volatile Runnable disconnectHandler = () -> {};
    private final AtomicBoolean disconnectFired = new AtomicBoolean(false);

    public BinanceStreamClient(StreamClientConfig config, boolean futures) {
        this(config, futures, Duration.ofMillis(200), Duration.ofMillis(250));
    }

    /** Test seam: live frames sent and not yet answered. */
    int liveInFlightSize() {
        return liveInFlight.size();
    }

    /** Test seam: the coalescing delay and the spacing between live frames. */
    BinanceStreamClient(StreamClientConfig config, boolean futures, Duration liveCoalesce, Duration liveFrameGap) {
        this.config = config;
        this.futures = futures;
        this.liveCoalesce = liveCoalesce;
        this.liveFrameGap = liveFrameGap;
    }

    public static BinanceStreamClient spot(StreamClientConfig config) {
        return new BinanceStreamClient(config, false);
    }

    public static BinanceStreamClient futures(StreamClientConfig config) {
        return new BinanceStreamClient(config, true);
    }

    @Override
    public void connect() throws Exception {
        synchronized (streamsLock) {
            if (pendingStreams.isEmpty()) {
                throw new IllegalStateException("No subscriptions registered. Subscribe before connecting.");
            }
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
        String url;
        int inUrl;
        synchronized (streamsLock) {
            url = baseUrl + String.join("/", pendingStreams);
            inUrl = pendingStreams.size();
            requestedStreamCount = inUrl;
            requestedNames.clear();
            requestedNames.addAll(pendingStreams);
            // ids of the previous socket never get a reply
            liveInFlight.clear();
        }
        log.info("Connecting to Binance {} with {} streams", futures ? "futures" : "spot", inUrl);
        wsClient.connect(url);

        // Streams registered while the handshake was running are in neither the URL nor a live
        // request yet.
        List<String> late;
        synchronized (streamsLock) {
            late = new ArrayList<>(pendingStreams.subList(requestedStreamCount, pendingStreams.size()));
            requestedStreamCount = pendingStreams.size();
        }
        late.stream().filter(requestedNames::add).forEach(this::enqueueLive);
    }

    /**
     * Registers a stream. Before the socket is open it joins the URL built by {@link #connect()};
     * once it is open the stream is requested with a live {@code SUBSCRIBE}.
     */
    private void addStream(String stream) {
        boolean live = false;
        synchronized (streamsLock) {
            pendingStreams.add(stream);
            if (requestedStreamCount >= 0 && isConnected()) {
                requestedStreamCount = pendingStreams.size();
                // add() is false when the stream is already part of the URL or was requested live
                live = requestedNames.add(stream);
            }
        }
        if (live) {
            enqueueLive(stream);
        }
    }

    private void enqueueLive(String stream) {
        liveQueue.add(stream);
        if (liveFlushRunning.compareAndSet(false, true)) {
            Thread.ofVirtual().name("binance-live-subscribe").start(this::flushLive);
        }
    }

    /**
     * Drains the live queue into SUBSCRIBE frames. A single runner at a time, so frames are never
     * sent concurrently and never faster than one per {@code liveFrameGap}.
     */
    private void flushLive() {
        try {
            sleep(liveCoalesce);
            while (true) {
                List<String> chunk = new ArrayList<>(LIVE_CHUNK);
                String next;
                while (chunk.size() < LIVE_CHUNK && (next = liveQueue.poll()) != null) {
                    chunk.add(next);
                }
                if (chunk.isEmpty()) {
                    break;
                }
                WebSocketClient ws = wsClient;
                if (ws == null || !ws.isOpen()) {
                    log.info("Binance live SUBSCRIBE dropped for {} streams: socket closed; they join the next connection",
                            chunk.size() + liveQueue.size());
                    requestedNames.removeAll(chunk);
                    dropQueued();
                    break;
                }
                long id = requestId.incrementAndGet();
                liveInFlight.put(id, chunk);
                try {
                    ws.send(liveFrame(chunk, id));
                } catch (RuntimeException e) {
                    liveInFlight.remove(id);
                    requestedNames.removeAll(chunk);
                    throw e;
                }
                log.info("Binance live SUBSCRIBE id={} streams={}", id, chunk.size());
                if (!liveQueue.isEmpty()) {
                    sleep(liveFrameGap);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            dropQueued();
        } catch (RuntimeException e) {
            log.warn("Binance live SUBSCRIBE failed: {}; the streams join the next connection", e.getMessage());
            dropQueued();
        } finally {
            liveFlushRunning.set(false);
            // A stream queued just as the runner was finishing would otherwise wait for the next
            // registration or reconnection; this re-check closes that window of a few
            // microseconds. It cannot be exercised deterministically, so it has no test of its own.
            if (!liveQueue.isEmpty() && liveFlushRunning.compareAndSet(false, true)) {
                Thread.ofVirtual().name("binance-live-subscribe").start(this::flushLive);
            }
        }
    }

    /** Forgets the requests still waiting: they were never sent, so a later registration may ask again. */
    private void dropQueued() {
        String stream;
        while ((stream = liveQueue.poll()) != null) {
            requestedNames.remove(stream);
        }
    }

    private static void sleep(Duration d) throws InterruptedException {
        if (!d.isZero()) {
            Thread.sleep(d);
        }
    }

    static String liveFrame(List<String> streams, long id) {
        var root = MAPPER.createObjectNode();
        root.put("method", "SUBSCRIBE");
        var params = root.putArray("params");
        streams.forEach(params::add);
        root.put("id", id);
        return root.toString();
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
        tickerSubs.put(stream, new Subscription<>(instrument, handler));
        addStream(stream);
    }

    /**
     * Subscribe to the bookTicker stream ({@code <symbol>@bookTicker}), which fires on every
     * best-bid or best-ask change. High frequency and small payload — bid/bidSize/ask/askSize
     * only, last / OHLC / volume are null. Intended for the future high-frequency BBO channel,
     * not a drop-in replacement for {@link #subscribeTicker}.
     */
    public void subscribeBookTicker(Instrument instrument, Consumer<Ticker> handler) {
        String stream = BinanceSymbols.toStream(instrument) + "@bookTicker";
        bookTickerSubs.put(stream, new Subscription<>(instrument, handler));
        addStream(stream);
    }

    @Override
    public void subscribeKline(Instrument instrument, String interval, Consumer<Kline> handler) {
        String stream = BinanceSymbols.toStream(instrument) + "@kline_" + interval;
        klineSubs.put(stream, new Subscription<>(instrument, handler));
        addStream(stream);
    }

    @Override
    public void subscribeFundingRate(Instrument instrument, Consumer<FundingRate> handler) {
        if (!futures) {
            throw new UnsupportedOperationException("Funding rates only available on futures streams");
        }
        String stream = BinanceSymbols.toStream(instrument) + "@markPrice@1s";
        fundingSubs.put(stream, new Subscription<>(instrument, handler));
        addStream(stream);
    }

    @Override
    public boolean isConnected() {
        return wsClient != null && wsClient.isOpen();
    }

    @Override
    public void close() throws Exception {
        dropQueued();
        if (wsClient != null) {
            wsClient.close();
        }
    }

    private void handleMessage(String message) {
        try {
            JsonNode root = MAPPER.readTree(message);
            String stream = root.has("stream") ? root.get("stream").asText() : null;
            JsonNode data = root.has("data") ? root.get("data") : root;

            if (stream == null) {
                handleControlReply(root);
                return;
            }

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

    /**
     * The answer to a live SUBSCRIBE: {@code {"result":null,"id":n}} on success, an {@code error}
     * object when Binance refuses. A refused stream is taken off the "requested" list so a later
     * registration may try again; it stays in the registry and joins the next connection's URL.
     */
    private void handleControlReply(JsonNode root) {
        if (!root.has("id")) {
            if (root.has("error")) {
                log.warn("Binance control reply without an id: {}", root.get("error"));
            }
            return;
        }
        List<String> streams = liveInFlight.remove(root.get("id").asLong());
        if (root.has("error")) {
            log.warn("Binance refused live SUBSCRIBE id={} streams={}: {}", root.get("id").asLong(),
                    streams == null ? "?" : streams.size() + " " + streams, root.get("error"));
            if (streams != null) {
                requestedNames.removeAll(streams);
            }
        }
    }

    private record Subscription<T>(Instrument instrument, Consumer<T> handler) {}
}
