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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Gate.io USDⓈ-M futures streaming client (API v4).
 *
 * <p>URL: {@code wss://fx-ws.gateio.ws/v4/ws/usdt}. Same {@code event:"subscribe"} envelope as
 * spot but on a different host; futures tickers carry funding-rate fields inline
 * ({@code funding_rate}, {@code funding_next_apply}, {@code mark_price}), so a ticker +
 * funding subscription pair on the same instrument share one WS topic.
 *
 * <p>Kline is out of scope on this client — Gate.io futures klines live on a different
 * channel ({@code futures.candlesticks}) we haven't needed yet.
 */
public class GateioFuturesStreamClient implements StreamClient {

    private static final Logger log = LoggerFactory.getLogger(GateioFuturesStreamClient.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String WS_URL = "wss://fx-ws.gateio.ws/v4/ws/usdt";

    private final StreamClientConfig config;
    private WebSocketClient wsClient;

    private final List<String> tickerContracts = new ArrayList<>();
    private final Map<String, Subscription<Ticker>> tickerSubs = new ConcurrentHashMap<>();
    private final Map<String, Subscription<FundingRate>> fundingSubs = new ConcurrentHashMap<>();

    private volatile Runnable disconnectHandler = () -> {};
    private final AtomicBoolean disconnectFired = new AtomicBoolean(false);

    public GateioFuturesStreamClient(StreamClientConfig config) {
        this.config = config;
    }

    @Override
    public void connect() throws Exception {
        if (tickerContracts.isEmpty()) {
            throw new IllegalStateException("No subscriptions registered");
        }
        wsClient = config.wsClientFactory().get();
        disconnectFired.set(false);
        wsClient.onMessage(this::handleMessage);
        wsClient.onClose((code, reason) -> {
            log.warn("Gate.io futures WS closed: {} {}", code, reason);
            fireDisconnect();
        });
        wsClient.onError(err -> {
            log.error("Gate.io futures WS error: {}", err.getMessage());
            fireDisconnect();
        });
        log.info("Connecting to Gate.io futures v4 USDT with {} contracts", tickerContracts.size());
        wsClient.connect(WS_URL);
        wsClient.send(buildSubscribe("futures.tickers", tickerContracts));
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
        String contract = GateioSymbols.toSymbol(instrument);
        addContractOnce(contract);
        tickerSubs.put(contract, new Subscription<>(instrument, handler));
    }

    @Override
    public void subscribeKline(Instrument instrument, String interval, Consumer<Kline> handler) {
        throw new UnsupportedOperationException(
                "Gate.io futures kline not yet supported on this client");
    }

    @Override
    public void subscribeFundingRate(Instrument instrument, Consumer<FundingRate> handler) {
        if (instrument.settle() == null) {
            throw new UnsupportedOperationException(
                    "Funding rates require a derivatives instrument (settle != null)");
        }
        String contract = GateioSymbols.toSymbol(instrument);
        addContractOnce(contract);
        fundingSubs.put(contract, new Subscription<>(instrument, handler));
    }

    @Override
    public boolean isConnected() { return wsClient != null && wsClient.isOpen(); }

    @Override
    public void close() throws Exception {
        if (wsClient != null) wsClient.close();
    }

    private void handleMessage(String message) {
        try {
            JsonNode root = MAPPER.readTree(message);
            if (!"update".equals(root.path("event").asText(""))) return;
            if (!"futures.tickers".equals(root.path("channel").asText(""))) return;
            JsonNode result = root.get("result");
            if (result == null || !result.isArray()) return;
            long tsMs = root.path("time_ms").asLong(root.path("time").asLong(0L) * 1_000L);
            for (JsonNode entry : result) {
                String contract = entry.path("contract").asText("");
                Subscription<Ticker> tsub = tickerSubs.get(contract);
                Subscription<FundingRate> fsub = fundingSubs.get(contract);
                if (tsub != null) {
                    tsub.handler()
                            .accept(GateioAdapters.adaptFuturesTicker(tsub.instrument(), entry, tsMs));
                }
                if (fsub != null) {
                    FundingRate fr =
                            GateioAdapters.adaptFuturesFundingRate(fsub.instrument(), entry, tsMs);
                    if (fr != null) fsub.handler().accept(fr);
                }
            }
        } catch (Exception e) {
            log.error("Failed to parse Gate.io futures message: {}", e.getMessage());
        }
    }

    private void addContractOnce(String contract) {
        if (!tickerContracts.contains(contract)) tickerContracts.add(contract);
    }

    private static String buildSubscribe(String channel, List<String> contracts) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"time\":").append(System.currentTimeMillis() / 1_000L)
                .append(",\"channel\":\"").append(channel)
                .append("\",\"event\":\"subscribe\",\"payload\":[");
        for (int i = 0; i < contracts.size(); i++) {
            if (i > 0) sb.append(',');
            sb.append('"').append(contracts.get(i)).append('"');
        }
        sb.append("]}");
        return sb.toString();
    }

    List<String> queuedContracts() {
        return Collections.unmodifiableList(tickerContracts);
    }

    private record Subscription<T>(Instrument instrument, Consumer<T> handler) {}
}
