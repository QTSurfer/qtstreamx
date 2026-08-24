package com.qtsurfer.qtstreamx.exchange.bybit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.qtsurfer.qtstreamx.core.client.StreamClient;
import com.qtsurfer.qtstreamx.core.client.StreamClientConfig;
import com.qtsurfer.qtstreamx.core.model.FundingRate;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.model.Kline;
import com.qtsurfer.qtstreamx.core.model.Ticker;
import com.qtsurfer.qtstreamx.core.ws.WebSocketClient;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
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
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bybit WebSocket v5 streaming client.
 *
 * <p>One client handles one category — spot or linear (USDⓈ-M perps). Inverse markets use the
 * same linear URL with different symbols but aren't enabled in the helper factories below.
 *
 * <p>Unlike Binance combined streams, Bybit subscribes after the WS is open: on connect we send
 * a single {@code {"op":"subscribe","args":[...]}} with every topic we collected during
 * {@code subscribeXxx} calls. Ping/pong keepalive is required every 20s on the v5 public
 * endpoints and IS sent, on {@link StreamClientConfig#pingInterval()} halved
 * drives it.
 *
 * <p>Linear tickers carry both BBO + OHLC + funding rate in the same topic, so
 * {@link #subscribeTicker} and {@link #subscribeFundingRate} share one WS subscription under the
 * hood; the client dispatches both handlers from the same message.
 */
public class BybitStreamClient implements StreamClient {

    private static final Logger log = LoggerFactory.getLogger(BybitStreamClient.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String SPOT_WS = "wss://stream.bybit.com/v5/public/spot";
    private static final String LINEAR_WS = "wss://stream.bybit.com/v5/public/linear";

    /** Linear perps only — interval comes from {@link BybitInstrumentsCache} per instrument. */
    private static final int DEFAULT_FUNDING_INTERVAL_HOURS = 8;

    public enum Category {
        SPOT,
        LINEAR
    }

    private final StreamClientConfig config;
    private final Category category;
    private WebSocketClient wsClient;

    // Subscribe requests queued before connect.
    private final List<String> pendingTopics = new ArrayList<>();
    // Routing tables keyed by topic (e.g. "tickers.BTCUSDT", "kline.1.BTCUSDT").
    private final Map<String, Subscription<Ticker>> tickerSubs = new ConcurrentHashMap<>();
    private final Map<String, FundingSubscription> fundingSubs = new ConcurrentHashMap<>();
    private final Map<String, Subscription<Kline>> klineSubs = new ConcurrentHashMap<>();
    // Last-seen funding fields per topic. Bybit v5 linear tickers snapshots carry fundingRate +
    // nextFundingTime; subsequent deltas only include fields that changed. Without caching,
    // adaptFundingRate returns null for 99.99% of deltas and downstream sees only ~1 frate
    // emission per reconnect. We cache and emit on every tickers delta using the last known
    // rate/nextFundingTime + the current markPrice/ts.
    private final Map<String, FundingCache> fundingCache = new ConcurrentHashMap<>();

    private volatile Runnable disconnectHandler = () -> {};
    private final AtomicBoolean disconnectFired = new AtomicBoolean(false);

    // Funding REST re-seed. Bybit linear funding lives ONLY in the tickers SNAPSHOT (deltas omit it);
    // if the WS reader stalls during the snapshot burst (e.g. backpressure from a downstream consumer
    // under a high-rate sibling feed) the snapshot is missed and cache.rate stays null forever, so
    // that perp never emits funding while its ticker keeps flowing off deltas — leaving roughly half
    // the perpetual universe without funding. Seeding cache.rate from the REST tickers endpoint on
    // connect + every 5 min makes funding independent of catching the fragile one-shot snapshot.
    private static final String LINEAR_TICKERS_REST =
            "https://api.bybit.com/v5/market/tickers?category=linear";
    private static final long FUNDING_RESEED_PERIOD_S = 300L; // 5 min
    private final HttpClient http =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    private final Supplier<String> linearTickersFetcher;
    private ScheduledExecutorService fundingReseedExec;

    /**
     * Keepalive ping. Bybit v5 public endpoints close an idle socket after 20s, and this client
     * documented that requirement since it was written without ever implementing it — the javadoc
     * called the executor "optional". The connection therefore survived only while inbound traffic
     * happened to keep it warm, and any link whose instruments went quiet for 20s was closed with
     * 1006 and reconnected.
     *
     * <p>Measured on ingS3, 2026-08-01: bybit-linear reported drops=1676 and reconnects=1676 — the
     * SAME number, because every "drop" was the gap of a reconnect, not a buffer overflow. Roughly
     * one a minute per replica. bybit-spot on the same process showed 16, which is what ruled out
     * the host: same machine, same CPU, same NIC (0 errors in 1.75e9 packets), different topic mix.
     * Raising CPU headroom changed the rate by nothing at all.
     */
    private ScheduledExecutorService pingExec;

    private ScheduledFuture<?> pingTask;
    private ScheduledFuture<?> fundingReseedTask;

    public BybitStreamClient(StreamClientConfig config, Category category) {
        this(config, category, null);
    }

    /** Test seam: inject the raw {@code /v5/market/tickers?category=linear} JSON body fetcher (null =
     *  the real HTTP GET) to exercise the funding REST re-seed without network. */
    BybitStreamClient(
            StreamClientConfig config, Category category, Supplier<String> linearTickersFetcher) {
        this.config = config;
        this.category = category;
        this.linearTickersFetcher =
                linearTickersFetcher != null ? linearTickersFetcher : this::httpGetLinearTickers;
    }

    public static BybitStreamClient spot(StreamClientConfig config) {
        return new BybitStreamClient(config, Category.SPOT);
    }

    public static BybitStreamClient linear(StreamClientConfig config) {
        return new BybitStreamClient(config, Category.LINEAR);
    }

    @Override
    public void connect() throws Exception {
        if (pendingTopics.isEmpty()) {
            throw new IllegalStateException("No subscriptions registered. Subscribe before connecting.");
        }
        wsClient = config.wsClientFactory().get();
        disconnectFired.set(false);
        wsClient.onMessage(this::handleMessage);
        wsClient.onClose((code, reason) -> {
            log.warn("Bybit WS closed: {} {}", code, reason);
            fireDisconnect();
        });
        wsClient.onError(err -> {
            log.error("Bybit WS error: {}", err.getMessage());
            fireDisconnect();
        });

        String url = category == Category.SPOT ? SPOT_WS : LINEAR_WS;
        log.info("Connecting to Bybit {} with {} topics", category, pendingTopics.size());
        wsClient.connect(url);

        // Subscribe op sends all topics in one frame. Bybit caps each frame at 21k bytes and 10
        // topics per subscribe on some endpoints; our typical pool stays well below that, but
        // split into chunks of 10 defensively — the WS accepts multiple subscribe frames.
        for (int i = 0; i < pendingTopics.size(); i += 10) {
            List<String> chunk = pendingTopics.subList(i, Math.min(i + 10, pendingTopics.size()));
            wsClient.send("{\"op\":\"subscribe\",\"args\":" + toJsonArray(chunk) + "}");
        }

        schedulePing();
        scheduleFundingReseed();
    }

    /**
     * Send {@code {"op":"ping"}} on a fixed schedule.
     *
     * <p>Interval is {@link StreamClientConfig#pingInterval()}, which already defaults to 20s — the
     * value was sitting there unused. It is halved here: pinging AT the timeout leaves no room for
     * one lost frame or a slow round trip from a distant region, and the node that surfaced this
     * runs in Tokyo. Floor of 5s mirrors the HTX and Bitget clients, which have always done this.
     */
    private synchronized void schedulePing() {
        long intervalSec = Math.max(5L, config.pingInterval().getSeconds() / 2);
        if (pingExec == null) {
            pingExec = Executors.newSingleThreadScheduledExecutor(
                    r -> {
                        Thread t = new Thread(r, "bybit-ping-" + category);
                        t.setDaemon(true);
                        return t;
                    });
        }
        if (pingTask != null) {
            pingTask.cancel(false);
        }
        pingTask = pingExec.scheduleAtFixedRate(
                () -> {
                    try {
                        wsClient.send("{\"op\":\"ping\"}");
                    } catch (Exception e) {
                        // Never let a failed ping kill the scheduler: the socket is closing anyway
                        // and onClose already drives the reconnect.
                        log.debug("Bybit ping failed ({}): {}", category, e.getMessage());
                    }
                },
                intervalSec, intervalSec, java.util.concurrent.TimeUnit.SECONDS);
        log.info("Bybit {} keepalive ping every {}s", category, intervalSec);
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
     * Subscribe to the {@code tickers.<symbol>} topic. On the linear stream this topic carries
     * bid/ask + OHLC + funding rate; the handler only sees the ticker fields. Pair with
     * {@link #subscribeFundingRate} on the same instrument to extract funding — both handlers
     * are driven by the same incoming message.
     */
    @Override
    public void subscribeTicker(Instrument instrument, Consumer<Ticker> handler) {
        String topic = "tickers." + BybitSymbols.toStream(instrument);
        addTopicOnce(topic);
        tickerSubs.put(topic, new Subscription<>(instrument, handler));
    }

    /**
     * Subscribe to kline updates. Bybit intervals are numeric minutes (e.g. {@code "1"},
     * {@code "5"}, {@code "60"}) or {@code "D"}, {@code "W"}, {@code "M"} — notably no
     * 1-second equivalent. Callers with {@code "1s"} should map to {@code "1"} and accept the
     * coarser cadence.
     */
    @Override
    public void subscribeKline(Instrument instrument, String interval, Consumer<Kline> handler) {
        // Bybit intervals are numeric minutes or D/W/M — no "1s" / "1m" etc. Callers shaped
        // around the Binance convention ("1s", "1m", "5m", …) get re-mapped here so the
        // subscribe op doesn't receive an invalid topic. An invalid topic in a mixed-chunk
        // subscribe frame causes Bybit to reject the whole chunk → WS idles → 20 s timeout
        // → 1006 abnormal closure on the link. 2026-04-24.
        String bybitInterval = remapKlineInterval(interval);
        String topic = "kline." + bybitInterval + "." + BybitSymbols.toStream(instrument);
        addTopicOnce(topic);
        klineSubs.put(topic, new Subscription<>(instrument, handler));
    }

    private static final AtomicBoolean warnedOneSecondMapped = new AtomicBoolean(false);

    private static String remapKlineInterval(String interval) {
        if (interval == null) throw new IllegalArgumentException("Bybit kline interval is null");
        switch (interval) {
            case "1s":
                // Bybit spot klines are minute-granular; upstream callers shaped around the
                // Binance "1s" convention get the coarsest Bybit bucket (1-minute). Log at WARN
                // once per client instance so it's visible on startup without flooding the log
                // with one line per instrument.
                if (warnedOneSecondMapped.compareAndSet(false, true)) {
                    log.warn("Bybit has no 1-second klines — mapping '1s' → '1' (1-minute) for all subscribes");
                }
                return "1";
            case "1m": return "1";
            case "3m": return "3";
            case "5m": return "5";
            case "15m": return "15";
            case "30m": return "30";
            case "1h": return "60";
            case "2h": return "120";
            case "4h": return "240";
            case "6h": return "360";
            case "12h": return "720";
            case "1d": return "D";
            case "1w": return "W";
            case "1M": return "M";
            default:
                return interval; // assume caller passed a native Bybit value like "1" or "D"
        }
    }

    /**
     * Subscribe to funding rate updates derived from the linear {@code tickers.<symbol>} topic.
     * Rejected on spot because funding rates don't exist there.
     *
     * <p>If {@link #subscribeTicker} is also called for this instrument, both handlers fire on
     * every incoming linear-ticker message — the same topic under the hood.
     */
    @Override
    public void subscribeFundingRate(Instrument instrument, Consumer<FundingRate> handler) {
        if (category != Category.LINEAR) {
            throw new UnsupportedOperationException(
                    "Funding rates only available on Bybit linear streams");
        }
        subscribeFundingRate(instrument, DEFAULT_FUNDING_INTERVAL_HOURS, handler);
    }

    /**
     * Same as {@link #subscribeFundingRate(Instrument, Consumer)} but with an explicit funding
     * interval for pairs that settle at a non-default cadence (e.g. BTCUSDT settles every 4h).
     * The interval is passed through to the {@link FundingRate#intervalHours} field so downstream
     * scaling (r1/r8 in the NATS bridge) works correctly.
     */
    public void subscribeFundingRate(
            Instrument instrument, int intervalHours, Consumer<FundingRate> handler) {
        if (category != Category.LINEAR) {
            throw new UnsupportedOperationException(
                    "Funding rates only available on Bybit linear streams");
        }
        String topic = "tickers." + BybitSymbols.toStream(instrument);
        addTopicOnce(topic);
        fundingSubs.put(topic, new FundingSubscription(instrument, handler, intervalHours));
    }

    @Override
    public boolean isConnected() {
        return wsClient != null && wsClient.isOpen();
    }

    @Override
    public void close() throws Exception {
        if (fundingReseedTask != null) {
            fundingReseedTask.cancel(false);
        }
        if (fundingReseedExec != null) {
            fundingReseedExec.shutdownNow();
        }
        if (pingTask != null) {
            pingTask.cancel(false);
        }
        if (pingExec != null) {
            pingExec.shutdownNow();
        }
        if (wsClient != null) {
            wsClient.close();
        }
    }

    void handleMessage(String message) { // package-private: tests feed canned frames
        try {
            JsonNode root = MAPPER.readTree(message);
            // Subscribe ack / pong responses arrive with {"success":true, "op":"subscribe"/"pong"}
            // — skip anything without a topic.
            if (!root.has("topic")) return;

            String topic = root.get("topic").asText();
            long tsMs = root.path("ts").asLong(0L);
            JsonNode data = root.path("data");
            if (data.isMissingNode()) return;

            Subscription<Ticker> tsub = tickerSubs.get(topic);
            FundingSubscription fsub = fundingSubs.get(topic);
            if (tsub != null || fsub != null) {
                // Tickers arrive as an object; linear perps include bid/ask + funding, spot does
                // not. The same payload feeds both ticker and funding subscribers when wired.
                Ticker ticker = category == Category.LINEAR
                        ? BybitAdapters.adaptTickerLinear(
                                tsub != null ? tsub.instrument() : fsub.instrument(), data, tsMs)
                        : BybitAdapters.adaptTickerSpot(tsub.instrument(), data, tsMs);
                if (tsub != null) tsub.handler().accept(ticker);
                if (fsub != null) {
                    FundingRate fr = buildFundingRate(topic, fsub, data, tsMs);
                    if (fr != null) fsub.handler().accept(fr);
                }
                return;
            }

            Subscription<Kline> ksub = klineSubs.get(topic);
            if (ksub != null && data.isArray()) {
                for (JsonNode entry : data) {
                    ksub.handler().accept(BybitAdapters.adaptKline(ksub.instrument(), entry));
                }
            }
        } catch (Exception e) {
            log.error("Failed to parse Bybit message: {}", e.getMessage());
        }
    }

    /**
     * Emit a {@link FundingRate} using the latest-known funding fields for the topic. Bybit
     * deltas usually omit {@code fundingRate} and {@code nextFundingTime}; we cache whatever
     * the snapshot (or occasional delta) carries and replay those values on each subsequent
     * ticker update, refreshing {@code markPrice} + timestamp each time. Returns null only
     * until the first snapshot has arrived with a real fundingRate.
     */
    private FundingRate buildFundingRate(String topic, FundingSubscription fsub, JsonNode data, long tsMs) {
        FundingCache cache = fundingCache.computeIfAbsent(topic, k -> new FundingCache());
        java.math.BigDecimal rateFromMsg = BybitAdapters.optDecimal(data, "fundingRate");
        if (rateFromMsg != null) cache.rate = rateFromMsg;
        long nextFromMsg = BybitAdapters.optLong(data, "nextFundingTime");
        if (nextFromMsg > 0) cache.nextFundingMs = nextFromMsg;
        java.math.BigDecimal markFromMsg = BybitAdapters.optDecimal(data, "markPrice");
        if (markFromMsg != null) cache.markPrice = markFromMsg;
        if (cache.rate == null) return null;
        return new FundingRate(
                fsub.instrument(),
                cache.rate,
                cache.markPrice,
                cache.nextFundingMs * 1_000L,
                fsub.intervalHours(),
                tsMs * 1_000L);
    }

    /** LINEAR only: (re)start the periodic REST funding re-seed — a first run a few seconds after
     *  connect, then every {@link #FUNDING_RESEED_PERIOD_S}s. connect() can be called again on
     *  reconnect, so cancel any prior schedule first. */
    private synchronized void scheduleFundingReseed() {
        if (category != Category.LINEAR || fundingSubs.isEmpty()) {
            return;
        }
        if (fundingReseedExec == null) {
            fundingReseedExec =
                    Executors.newSingleThreadScheduledExecutor(
                            r -> {
                                Thread t = new Thread(r, "bybit-funding-reseed");
                                t.setDaemon(true);
                                return t;
                            });
        }
        if (fundingReseedTask != null) {
            fundingReseedTask.cancel(false);
        }
        fundingReseedTask =
                fundingReseedExec.scheduleWithFixedDelay(
                        this::reseedFundingFromRest, 3, FUNDING_RESEED_PERIOD_S, TimeUnit.SECONDS);
    }

    /**
     * Seed {@link FundingCache#rate} (+ nextFundingTime / markPrice) for every subscribed linear topic
     * from the REST tickers endpoint, so a perp whose WS funding SNAPSHOT was missed still emits
     * funding on its next ticker delta instead of staying dark. Best-effort: a failed fetch leaves the
     * cache as-is until the next run. Package-private for tests.
     */
    void reseedFundingFromRest() {
        try {
            JsonNode list = MAPPER.readTree(linearTickersFetcher.get()).path("result").path("list");
            int seeded = 0;
            for (JsonNode t : list) {
                String topic = "tickers." + t.path("symbol").asText();
                if (!fundingSubs.containsKey(topic)) {
                    continue;
                }
                BigDecimal rate = BybitAdapters.optDecimal(t, "fundingRate");
                if (rate == null) {
                    continue;
                }
                FundingCache cache = fundingCache.computeIfAbsent(topic, k -> new FundingCache());
                cache.rate = rate;
                long next = BybitAdapters.optLong(t, "nextFundingTime");
                if (next > 0) {
                    cache.nextFundingMs = next;
                }
                BigDecimal mark = BybitAdapters.optDecimal(t, "markPrice");
                if (mark != null) {
                    cache.markPrice = mark;
                }
                seeded++;
            }
            log.info(
                    "Bybit linear funding REST-reseed: seeded {} of {} subscribed topics",
                    seeded,
                    fundingSubs.size());
        } catch (Exception e) {
            log.warn("Bybit funding REST-reseed failed: {}", e.getMessage());
        }
    }

    private String httpGetLinearTickers() {
        try {
            HttpRequest req =
                    HttpRequest.newBuilder(URI.create(LINEAR_TICKERS_REST))
                            .timeout(Duration.ofSeconds(15))
                            .GET()
                            .build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                throw new IllegalStateException("HTTP " + resp.statusCode());
            }
            return resp.body();
        } catch (Exception e) {
            throw new RuntimeException(
                    "bybit linear tickers REST fetch failed: " + e.getMessage(), e);
        }
    }

    private void addTopicOnce(String topic) {
        if (!pendingTopics.contains(topic)) {
            pendingTopics.add(topic);
        }
    }

    private static String toJsonArray(List<String> items) {
        StringBuilder sb = new StringBuilder(items.size() * 32);
        sb.append('[');
        for (int i = 0; i < items.size(); i++) {
            if (i > 0) sb.append(',');
            sb.append('"').append(items.get(i)).append('"');
        }
        sb.append(']');
        return sb.toString();
    }

    /** Package-private exposure for tests that inspect queued topics. */
    List<String> pendingTopics() {
        return Collections.unmodifiableList(pendingTopics);
    }

    private record Subscription<T>(Instrument instrument, Consumer<T> handler) {}

    private record FundingSubscription(
            Instrument instrument, Consumer<FundingRate> handler, int intervalHours) {}

    private static final class FundingCache {
        volatile BigDecimal rate;
        volatile BigDecimal markPrice;
        volatile long nextFundingMs;
    }
}
