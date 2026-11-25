package com.qtsurfer.qtstreamx.exchange.binance;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.qtsurfer.qtstreamx.core.client.StreamClientConfig;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.ws.WebSocketClient;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

/**
 * A stream registered after the socket is open must be requested with a live {@code SUBSCRIBE};
 * before that it joins the connection URL as it always did. The frames are coalesced, bounded in
 * size, never repeated, and a request that cannot be sent is dropped without harming the client.
 */
class BinanceStreamClientLiveSubscribeTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Instrument BTC = new Instrument("BTC", "USDT", null);
    private static final Instrument ETH = new Instrument("ETH", "USDT", null);
    private static final Instrument SOL = new Instrument("SOL", "USDT", null);
    private static final Instrument ETH_PERP = new Instrument("ETH", "USDT", "USDT");

    // ------------------------------------------------------------------ connected link

    @Test
    void streamsRegisteredBeforeConnectGoInTheUrlAndSendNoFrame() throws Exception {
        FakeWs ws = new FakeWs();
        BinanceStreamClient client = client(ws, Duration.ZERO);
        client.subscribeKline(BTC, "1s", k -> {});
        client.subscribeTicker(BTC, t -> {});
        client.connect();

        assertThat(ws.url).isEqualTo("wss://stream.binance.com:9443/stream?streams=btcusdt@kline_1s/btcusdt@ticker");
        pause();
        assertThat(ws.sent).isEmpty();
    }

    @Test
    void streamRegisteredOnAnOpenSocketIsRequestedWithOneLiveFrame() throws Exception {
        FakeWs ws = new FakeWs();
        BinanceStreamClient client = client(ws, Duration.ZERO);
        client.subscribeKline(BTC, "1s", k -> {});
        client.connect();

        client.subscribeKline(ETH, "1s", k -> {});

        awaitTrue(() -> ws.sent.size() == 1);
        JsonNode frame = MAPPER.readTree(ws.sent.get(0));
        assertThat(frame.get("method").asText()).isEqualTo("SUBSCRIBE");
        assertThat(frame.get("params")).extracting(JsonNode::asText).containsExactly("ethusdt@kline_1s");
        assertThat(frame.get("id").asLong()).isEqualTo(1);
        // the registry keeps it too, so the next connection's URL carries it
        assertThat(pendingStreams(client)).containsExactly("btcusdt@kline_1s", "ethusdt@kline_1s");
    }

    @Test
    void aBurstOfRegistrationsIsCoalescedIntoOneFrame() throws Exception {
        FakeWs ws = new FakeWs();
        BinanceStreamClient client = client(ws, Duration.ofMillis(300));
        client.subscribeKline(BTC, "1s", k -> {});
        client.connect();

        client.subscribeKline(ETH, "1s", k -> {});
        client.subscribeTicker(ETH, t -> {});
        client.subscribeKline(SOL, "1s", k -> {});
        client.subscribeTicker(SOL, t -> {});

        awaitTrue(() -> !ws.sent.isEmpty());
        pause();
        assertThat(ws.sent).hasSize(1);
        assertThat(MAPPER.readTree(ws.sent.get(0)).get("params")).extracting(JsonNode::asText)
                .containsExactly("ethusdt@kline_1s", "ethusdt@ticker", "solusdt@kline_1s", "solusdt@ticker");
    }

    @Test
    void aLargeBatchIsSplitIntoBoundedFramesInOrderWithIncreasingIds() throws Exception {
        FakeWs ws = new FakeWs();
        BinanceStreamClient client = client(ws, Duration.ofMillis(500));
        client.subscribeKline(BTC, "1s", k -> {});
        client.connect();

        int n = BinanceStreamClient.LIVE_CHUNK * 2 + 20;
        for (int i = 0; i < n; i++) {
            client.subscribeKline(new Instrument("T" + i, "USDT", null), "1s", k -> {});
        }

        awaitTrue(() -> ws.sent.size() == 3);
        List<Integer> sizes = new ArrayList<>();
        List<String> all = new ArrayList<>();
        List<Long> ids = new ArrayList<>();
        for (String raw : ws.sent) {
            JsonNode f = MAPPER.readTree(raw);
            sizes.add(f.get("params").size());
            f.get("params").forEach(p -> all.add(p.asText()));
            ids.add(f.get("id").asLong());
        }
        assertThat(sizes).containsExactly(BinanceStreamClient.LIVE_CHUNK, BinanceStreamClient.LIVE_CHUNK, 20);
        assertThat(all).hasSize(n).doesNotHaveDuplicates();
        assertThat(all.get(0)).isEqualTo("t0usdt@kline_1s");
        assertThat(all.get(n - 1)).isEqualTo("t" + (n - 1) + "usdt@kline_1s");
        assertThat(ids).containsExactly(1L, 2L, 3L);
    }

    @Test
    void framesAreSpacedByTheConfiguredGap() throws Exception {
        FakeWs ws = new FakeWs();
        Duration gap = Duration.ofMillis(120);
        BinanceStreamClient client = new BinanceStreamClient(config(ws), false, Duration.ofMillis(300), gap);
        client.subscribeKline(BTC, "1s", k -> {});
        client.connect();
        for (int i = 0; i < BinanceStreamClient.LIVE_CHUNK + 1; i++) {
            client.subscribeKline(new Instrument("G" + i, "USDT", null), "1s", k -> {});
        }

        awaitTrue(() -> ws.sent.size() == 2);
        assertThat(ws.sentAtNanos.get(1) - ws.sentAtNanos.get(0)).isGreaterThanOrEqualTo(gap.toNanos() - 5_000_000L);
    }

    // ------------------------------------------------------------------ idempotence

    @Test
    void aStreamAlreadyInTheUrlIsNeverRequestedAgain() throws Exception {
        FakeWs ws = new FakeWs();
        BinanceStreamClient client = client(ws, Duration.ZERO);
        client.subscribeKline(BTC, "1s", k -> {});
        client.connect();

        client.subscribeKline(BTC, "1s", k -> {});
        pause();

        assertThat(ws.sent).isEmpty();
    }

    @Test
    void aStreamRequestedLiveIsNotRequestedTwice() throws Exception {
        FakeWs ws = new FakeWs();
        BinanceStreamClient client = client(ws, Duration.ZERO);
        client.subscribeKline(BTC, "1s", k -> {});
        client.connect();

        client.subscribeKline(ETH, "1s", k -> {});
        awaitTrue(() -> ws.sent.size() == 1);
        client.subscribeKline(ETH, "1s", k -> {});
        pause();

        assertThat(ws.sent).hasSize(1);
    }

    @Test
    void beforeTheSocketIsOpenNothingIsSent() {
        FakeWs ws = new FakeWs();
        BinanceStreamClient client = client(ws, Duration.ZERO);
        client.subscribeKline(BTC, "1s", k -> {});
        client.subscribeKline(ETH, "1s", k -> {});
        pause();

        assertThat(ws.sent).isEmpty();
        assertThat(pendingStreams(client)).containsExactly("btcusdt@kline_1s", "ethusdt@kline_1s");
    }

    @Test
    void streamsRegisteredWhileTheHandshakeRunsAreRequestedOnceItIsOpen() throws Exception {
        FakeWs ws = new FakeWs();
        BinanceStreamClient client = client(ws, Duration.ZERO);
        client.subscribeKline(BTC, "1s", k -> {});
        // the handshake is slow: someone registers a stream before the socket reports open
        ws.duringConnect = () -> client.subscribeKline(ETH, "1s", k -> {});

        client.connect();

        assertThat(ws.url).isEqualTo("wss://stream.binance.com:9443/stream?streams=btcusdt@kline_1s");
        awaitTrue(() -> ws.sent.size() == 1);
        assertThat(MAPPER.readTree(ws.sent.get(0)).get("params")).extracting(JsonNode::asText)
                .containsExactly("ethusdt@kline_1s");
    }

    // ------------------------------------------------------------------ failures

    @Test
    void aLinkThatDropsBetweenTheQueueAndTheSendSendsNothingAndKeepsTheStream() throws Exception {
        FakeWs ws = new FakeWs();
        BinanceStreamClient client = client(ws, Duration.ofMillis(300));
        client.subscribeKline(BTC, "1s", k -> {});
        client.connect();

        client.subscribeKline(ETH, "1s", k -> {});
        ws.open = false;                      // the socket closes while the request waits to be sent
        Thread.sleep(700);

        assertThat(ws.sent).isEmpty();
        // the stream stays in the registry: the next connection's URL carries it
        assertThat(pendingStreams(client)).containsExactly("btcusdt@kline_1s", "ethusdt@kline_1s");

        // and it was forgotten as "requested", so registering it again on a reopened socket asks for it
        ws.open = true;
        client.subscribeKline(ETH, "1s", k -> {});
        awaitTrue(() -> ws.sent.size() == 1);
    }

    @Test
    void aSendThatFailsIsContainedAndTheClientKeepsWorking() throws Exception {
        FakeWs ws = new FakeWs();
        BinanceStreamClient client = client(ws, Duration.ZERO);
        client.subscribeKline(BTC, "1s", k -> {});
        client.connect();

        ws.failNextSend = true;
        client.subscribeKline(ETH, "1s", k -> {});
        awaitTrue(() -> ws.failedSends == 1);
        pause();
        assertThat(ws.sent).isEmpty();

        client.subscribeKline(SOL, "1s", k -> {});
        awaitTrue(() -> ws.sent.size() == 1);
        assertThat(MAPPER.readTree(ws.sent.get(0)).get("params")).extracting(JsonNode::asText)
                .containsExactly("solusdt@kline_1s");

        // the stream whose send failed was forgotten as "requested": registering it again asks for it
        client.subscribeKline(ETH, "1s", k -> {});
        awaitTrue(() -> ws.sent.size() == 2);
    }

    @Test
    void aRejectedSubscribeIsLoggedAndTheStreamMayBeRequestedAgain() throws Exception {
        FakeWs ws = new FakeWs();
        BinanceStreamClient client = client(ws, Duration.ZERO);
        client.subscribeKline(BTC, "1s", k -> {});
        client.connect();
        client.subscribeKline(ETH, "1s", k -> {});
        awaitTrue(() -> ws.sent.size() == 1);

        ws.receive("{\"error\":{\"code\":2,\"msg\":\"Invalid request\"},\"id\":1}");
        client.subscribeKline(ETH, "1s", k -> {});

        awaitTrue(() -> ws.sent.size() == 2);
        assertThat(MAPPER.readTree(ws.sent.get(1)).get("id").asLong()).isEqualTo(2);
    }

    @Test
    void anAcceptedSubscribeKeepsTheStreamAsRequested() throws Exception {
        FakeWs ws = new FakeWs();
        BinanceStreamClient client = client(ws, Duration.ZERO);
        client.subscribeKline(BTC, "1s", k -> {});
        client.connect();
        client.subscribeKline(ETH, "1s", k -> {});
        awaitTrue(() -> ws.sent.size() == 1);

        ws.receive("{\"result\":null,\"id\":1}");
        client.subscribeKline(ETH, "1s", k -> {});
        pause();

        assertThat(ws.sent).hasSize(1);
    }

    @Test
    void anAnsweredRequestIsNoLongerTracked() throws Exception {
        FakeWs ws = new FakeWs();
        BinanceStreamClient client = client(ws, Duration.ZERO);
        client.subscribeKline(BTC, "1s", k -> {});
        client.connect();

        client.subscribeKline(ETH, "1s", k -> {});
        awaitTrue(() -> ws.sent.size() == 1);
        assertThat(client.liveInFlightSize()).isEqualTo(1);
        ws.receive("{\"result\":null,\"id\":1}");
        assertThat(client.liveInFlightSize()).isZero();

        client.subscribeKline(SOL, "1s", k -> {});
        awaitTrue(() -> ws.sent.size() == 2);
        ws.receive("{\"error\":{\"code\":2,\"msg\":\"Invalid request\"},\"id\":2}");
        assertThat(client.liveInFlightSize()).isZero();
    }

    @Test
    void aNewConnectionForgetsTheRequestsOfThePreviousSocket() throws Exception {
        FakeWs ws = new FakeWs();
        BinanceStreamClient client = client(ws, Duration.ZERO);
        client.subscribeKline(BTC, "1s", k -> {});
        client.connect();
        client.subscribeKline(ETH, "1s", k -> {});
        awaitTrue(() -> ws.sent.size() == 1);
        assertThat(client.liveInFlightSize()).isEqualTo(1);

        client.connect();

        assertThat(client.liveInFlightSize()).isZero();
        // and the registry, now carrying both streams, is what the new URL is built from
        assertThat(ws.url).isEqualTo("wss://stream.binance.com:9443/stream?streams=btcusdt@kline_1s/ethusdt@kline_1s");
    }

    @Test
    void aStreamQueuedWhileAFrameIsBeingSentGoesOutInTheNextFrame() throws Exception {
        FakeWs ws = new FakeWs();
        BinanceStreamClient client = client(ws, Duration.ZERO);
        client.subscribeKline(BTC, "1s", k -> {});
        client.connect();

        ws.gate = new java.util.concurrent.CountDownLatch(1);
        client.subscribeKline(ETH, "1s", k -> {});
        awaitTrue(() -> ws.sendsStarted == 1);          // the first frame is held inside send()
        client.subscribeKline(SOL, "1s", k -> {});      // registered while it is held
        ws.gate.countDown();

        awaitTrue(() -> ws.sent.size() == 2);
        assertThat(MAPPER.readTree(ws.sent.get(0)).get("params")).extracting(JsonNode::asText).containsExactly("ethusdt@kline_1s");
        assertThat(MAPPER.readTree(ws.sent.get(1)).get("params")).extracting(JsonNode::asText).containsExactly("solusdt@kline_1s");
    }

    @Test
    void controlRepliesAndOtherMessagesWithoutAStreamDoNotBreakTheHandler() throws Exception {
        FakeWs ws = new FakeWs();
        BinanceStreamClient client = client(ws, Duration.ZERO);
        client.subscribeKline(BTC, "1s", k -> {});
        client.connect();

        ws.receive("{\"result\":null,\"id\":99}");
        ws.receive("{\"error\":{\"code\":3,\"msg\":\"x\"}}");
        ws.receive("not json");
        ws.receive("{}");

        // still alive: a new stream is requested normally
        client.subscribeKline(ETH, "1s", k -> {});
        awaitTrue(() -> ws.sent.size() == 1);
    }

    // ------------------------------------------------------------------ futures

    @Test
    void theFuturesClientUsesTheSameLivePath() throws Exception {
        FakeWs ws = new FakeWs();
        BinanceStreamClient client = new BinanceStreamClient(config(ws), true, Duration.ZERO, Duration.ZERO);
        client.subscribeFundingRate(new Instrument("BTC", "USDT", "USDT"), f -> {});
        client.connect();

        client.subscribeFundingRate(ETH_PERP, f -> {});

        assertThat(ws.url).isEqualTo("wss://fstream.binance.com/market/stream?streams=btcusdt@markPrice@1s");
        awaitTrue(() -> ws.sent.size() == 1);
        assertThat(MAPPER.readTree(ws.sent.get(0)).get("params")).extracting(JsonNode::asText)
                .containsExactly("ethusdt@markPrice@1s");
    }

    @Test
    void aFrameCarriesAModestNumberOfStreams() {
        assertThat(BinanceStreamClient.LIVE_CHUNK).isBetween(1, 100);
    }

    @Test
    void theFrameIsExactlyTheDocumentedShape() throws Exception {
        assertThat(BinanceStreamClient.liveFrame(List.of("a@kline_1s", "b@ticker"), 7))
                .isEqualTo("{\"method\":\"SUBSCRIBE\",\"params\":[\"a@kline_1s\",\"b@ticker\"],\"id\":7}");
    }

    // ------------------------------------------------------------------ helpers

    private static BinanceStreamClient client(FakeWs ws, Duration coalesce) {
        return new BinanceStreamClient(config(ws), false, coalesce, Duration.ZERO);
    }

    private static StreamClientConfig config(FakeWs ws) {
        return StreamClientConfig.withDefaults(() -> ws);
    }

    private static void awaitTrue(BooleanSupplier condition) {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
        throw new AssertionError("condition not met within 5 s");
    }

    /** Long enough for a wrongly sent frame to show up, short enough not to slow the suite. */
    private static void pause() {
        try {
            Thread.sleep(250);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @SuppressWarnings("unchecked")
    private static List<String> pendingStreams(BinanceStreamClient client) {
        try {
            Field f = BinanceStreamClient.class.getDeclaredField("pendingStreams");
            f.setAccessible(true);
            return (List<String>) f.get(client);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    /** A websocket that records what is sent and lets the test open, close and answer it. */
    private static final class FakeWs implements WebSocketClient {
        volatile boolean open;
        volatile String url;
        volatile boolean failNextSend;
        volatile int failedSends;
        volatile Runnable duringConnect = () -> {};
        volatile java.util.concurrent.CountDownLatch gate;
        volatile int sendsStarted;
        final List<String> sent = new CopyOnWriteArrayList<>();
        final List<Long> sentAtNanos = new CopyOnWriteArrayList<>();
        private volatile Consumer<String> messageHandler = m -> {};

        @Override public void connect(String url) {
            this.url = url;
            duringConnect.run();
            open = true;
        }
        @Override public void send(String message) {
            sendsStarted++;
            java.util.concurrent.CountDownLatch g = gate;
            if (g != null) {
                try {
                    g.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (failNextSend) {
                failNextSend = false;
                failedSends++;
                throw new IllegalStateException("Send pending");
            }
            sent.add(message);
            sentAtNanos.add(System.nanoTime());
        }
        @Override public void onMessage(Consumer<String> handler) { this.messageHandler = handler; }
        @Override public void onClose(BiConsumer<Integer, String> handler) { /* unused */ }
        @Override public void onError(Consumer<Throwable> handler) { /* unused */ }
        @Override public boolean isOpen() { return open; }
        @Override public void close() { open = false; }

        void receive(String message) { messageHandler.accept(message); }
    }
}
