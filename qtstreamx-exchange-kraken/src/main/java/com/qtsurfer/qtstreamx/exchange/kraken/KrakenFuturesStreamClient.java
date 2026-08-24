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
 * Kraken futures streaming client (legacy v1 WS at {@code futures.kraken.com/ws/v1}).
 *
 * <p>Kraken futures is a separate API from spot v2 — different host, different protocol. The
 * {@code ticker} feed carries funding rate fields inline (funding_rate, next_funding_rate_time,
 * markPrice), so a single {@link #subscribeTicker} subscription can feed both
 * {@link #subscribeFundingRate} and ticker handlers without a second WS topic.
 *
 * <p>Funding interval on Kraken perps is <b>1 hour</b> (not 8h like the other venues). The
 * funding adapter hard-codes that so downstream scaling (r1 / r8 in the NATS bridge) is correct.
 *
 * <p>Kline ({@code ohlc}) is <i>not</i> available on Kraken futures WS — callers requiring
 * candles on perps have to REST-poll {@code /derivatives/api/v4/history}; not in scope for this
 * client.
 */
public class KrakenFuturesStreamClient implements StreamClient {

    private static final Logger log = LoggerFactory.getLogger(KrakenFuturesStreamClient.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String WS_URL = "wss://futures.kraken.com/ws/v1";

    private final StreamClientConfig config;
    private WebSocketClient wsClient;

    // One subscribe per feed; symbols batch inside product_ids.
    private final List<String> tickerProductIds = new ArrayList<>();
    private final Map<String, Subscription<Ticker>> tickerSubs = new ConcurrentHashMap<>();
    private final Map<String, Subscription<FundingRate>> fundingSubs = new ConcurrentHashMap<>();

    private volatile Runnable disconnectHandler = () -> {};
    private final AtomicBoolean disconnectFired = new AtomicBoolean(false);

    public KrakenFuturesStreamClient(StreamClientConfig config) {
        this.config = config;
    }

    @Override
    public void connect() throws Exception {
        if (tickerProductIds.isEmpty()) {
            throw new IllegalStateException("No subscriptions registered. Subscribe before connecting.");
        }
        wsClient = config.wsClientFactory().get();
        disconnectFired.set(false);
        wsClient.onMessage(this::handleMessage);
        wsClient.onClose((code, reason) -> {
            log.warn("Kraken futures WS closed: {} {}", code, reason);
            fireDisconnect();
        });
        wsClient.onError(err -> {
            log.error("Kraken futures WS error: {}", err.getMessage());
            fireDisconnect();
        });

        log.info("Connecting to Kraken futures v1 with {} ticker products", tickerProductIds.size());
        wsClient.connect(WS_URL);
        wsClient.send(subscribeFrame("ticker", tickerProductIds));
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
        String productId = KrakenSymbols.toFuturesProductId(instrument);
        addProductOnce(productId);
        tickerSubs.put(productId, new Subscription<>(instrument, handler));
    }

    /**
     * Kraken futures WS doesn't expose a kline/ohlc feed — {@link KrakenSpotStreamClient}
     * covers spot candles, and derivative candles come from the REST history endpoint.
     */
    @Override
    public void subscribeKline(Instrument instrument, String interval, Consumer<Kline> handler) {
        throw new UnsupportedOperationException(
                "Kraken futures WS has no ohlc feed; use spot v2 client or REST history");
    }

    /**
     * Subscribe to funding rate derived from the futures ticker feed. Requires a derivative
     * instrument ({@code settle != null}); Kraken spot has no funding.
     */
    @Override
    public void subscribeFundingRate(Instrument instrument, Consumer<FundingRate> handler) {
        if (instrument.settle() == null) {
            throw new UnsupportedOperationException(
                    "Funding rates require a derivatives instrument (settle != null)");
        }
        String productId = KrakenSymbols.toFuturesProductId(instrument);
        addProductOnce(productId);
        fundingSubs.put(productId, new Subscription<>(instrument, handler));
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
            // Subscribe-ack / heartbeat frames carry feed="subscribed" / "info" — only the
            // "ticker" feed has the fields we care about.
            if (!"ticker".equals(root.path("feed").asText(""))) return;

            String productId = root.path("product_id").asText("");
            if (productId.isEmpty()) return;

            Subscription<Ticker> tsub = tickerSubs.get(productId);
            Subscription<FundingRate> fsub = fundingSubs.get(productId);
            if (tsub == null && fsub == null) return;

            if (tsub != null) {
                tsub.handler().accept(KrakenAdapters.adaptTickerFutures(tsub.instrument(), root));
            }
            if (fsub != null) {
                FundingRate fr = KrakenAdapters.adaptFundingRate(fsub.instrument(), root);
                if (fr != null) fsub.handler().accept(fr);
            }
        } catch (Exception e) {
            log.error("Failed to parse Kraken futures message: {}", e.getMessage());
        }
    }

    private void addProductOnce(String productId) {
        if (!tickerProductIds.contains(productId)) tickerProductIds.add(productId);
    }

    private static String subscribeFrame(String feed, List<String> productIds) {
        StringBuilder sb = new StringBuilder(32 + productIds.size() * 16);
        sb.append("{\"event\":\"subscribe\",\"feed\":\"").append(feed).append("\",\"product_ids\":[");
        for (int i = 0; i < productIds.size(); i++) {
            if (i > 0) sb.append(',');
            sb.append('"').append(productIds.get(i)).append('"');
        }
        sb.append("]}");
        return sb.toString();
    }

    List<String> queuedProductIds() {
        return Collections.unmodifiableList(tickerProductIds);
    }

    private record Subscription<T>(Instrument instrument, Consumer<T> handler) {}
}
