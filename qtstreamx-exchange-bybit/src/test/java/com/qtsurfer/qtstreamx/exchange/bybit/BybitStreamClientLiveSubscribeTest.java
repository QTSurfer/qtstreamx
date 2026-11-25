package com.qtsurfer.qtstreamx.exchange.bybit;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.qtsurfer.qtstreamx.core.client.StreamClientConfig;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.ws.WebSocketClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

/**
 * A topic registered after the socket is open must be requested with a live {@code subscribe} op;
 * before that it goes out with the initial subscribe as it always did. The frames are coalesced,
 * bounded in size, never repeated, never interleaved with the initial frames, and a request that
 * cannot be sent is dropped without harming the client.
 */
class BybitStreamClientLiveSubscribeTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Instrument BTC = new Instrument("BTC", "USDT", null);
    private static final Instrument ETH = new Instrument("ETH", "USDT", null);
    private static final Instrument SOL = new Instrument("SOL", "USDT", null);
    private static final Instrument ETH_PERP = new Instrument("ETH", "USDT", "USDT");

    // ------------------------------------------------------------------ connected link

    @Test
    void initialTopicsGoOutInChunksOfTenAndNothingElse() throws Exception {
        FakeWs ws = new FakeWs();
        BybitStreamClient client = client(ws, Duration.ZERO);
        for (int i = 0; i < 25; i++) {
            client.subscribeTicker(new Instrument("I" + i, "USDT", null), t -> {});
        }
        client.connect();

        pause();
        assertThat(ws.sent).hasSize(3);
        assertThat(sizes(ws.sent)).containsExactly(10, 10, 5);
        assertThat(ws.sent.get(0)).startsWith("{\"op\":\"subscribe\",\"args\":[");
    }

    @Test
    void topicRegisteredOnAnOpenSocketIsRequestedWithOneLiveFrame() throws Exception {
        FakeWs ws = new FakeWs();
        BybitStreamClient client = client(ws, Duration.ZERO);
        client.subscribeKline(BTC, "1", k -> {});
        client.connect();
        assertThat(ws.sent).hasSize(1);

        client.subscribeKline(ETH, "1", k -> {});

        awaitTrue(() -> ws.sent.size() == 2);
        JsonNode frame = MAPPER.readTree(ws.sent.get(1));
        assertThat(frame.get("op").asText()).isEqualTo("subscribe");
        assertThat(frame.get("req_id").asText()).isEqualTo("live-1");
        assertThat(frame.get("args")).extracting(JsonNode::asText).containsExactly("kline.1.ETHUSDT");
        // the registry keeps it too, so the next connection's initial subscribe carries it
        assertThat(client.pendingTopics()).containsExactly("kline.1.BTCUSDT", "kline.1.ETHUSDT");
    }

    @Test
    void aBurstOfRegistrationsIsCoalescedIntoOneFrame() throws Exception {
        FakeWs ws = new FakeWs();
        BybitStreamClient client = client(ws, Duration.ofMillis(300));
        client.subscribeKline(BTC, "1", k -> {});
        client.connect();

        client.subscribeKline(ETH, "1", k -> {});
        client.subscribeTicker(ETH, t -> {});
        client.subscribeKline(SOL, "1", k -> {});
        client.subscribeTicker(SOL, t -> {});

        awaitTrue(() -> ws.sent.size() >= 2);
        pause();
        assertThat(ws.sent).hasSize(2);
        assertThat(MAPPER.readTree(ws.sent.get(1)).get("args")).extracting(JsonNode::asText)
                .containsExactly("kline.1.ETHUSDT", "tickers.ETHUSDT", "kline.1.SOLUSDT", "tickers.SOLUSDT");
    }

    @Test
    void aLargeBatchIsSplitIntoBoundedFramesInOrderWithIncreasingIds() throws Exception {
        FakeWs ws = new FakeWs();
        BybitStreamClient client = client(ws, Duration.ofMillis(500));
        client.subscribeKline(BTC, "1", k -> {});
        client.connect();

        int n = BybitStreamClient.LIVE_CHUNK * 2 + 5;
        for (int i = 0; i < n; i++) {
            client.subscribeKline(new Instrument("T" + i, "USDT", null), "1", k -> {});
        }

        awaitTrue(() -> ws.sent.size() == 4);          // the initial frame and three live ones
        List<Integer> sizes = new ArrayList<>();
        List<String> all = new ArrayList<>();
        List<String> ids = new ArrayList<>();
        for (String raw : ws.sent.subList(1, 4)) {
            JsonNode f = MAPPER.readTree(raw);
            sizes.add(f.get("args").size());
            f.get("args").forEach(p -> all.add(p.asText()));
            ids.add(f.get("req_id").asText());
        }
        assertThat(sizes).containsExactly(BybitStreamClient.LIVE_CHUNK, BybitStreamClient.LIVE_CHUNK, 5);
        assertThat(all).hasSize(n).doesNotHaveDuplicates();
        assertThat(all.get(0)).isEqualTo("kline.1.T0USDT");
        assertThat(all.get(n - 1)).isEqualTo("kline.1.T" + (n - 1) + "USDT");
        assertThat(ids).containsExactly("live-1", "live-2", "live-3");
    }

    @Test
    void framesAreSpacedByTheConfiguredGap() throws Exception {
        FakeWs ws = new FakeWs();
        Duration gap = Duration.ofMillis(120);
        BybitStreamClient client = new BybitStreamClient(
                config(ws), BybitStreamClient.Category.SPOT, null, Duration.ofMillis(300), gap);
        client.subscribeKline(BTC, "1", k -> {});
        client.connect();
        for (int i = 0; i < BybitStreamClient.LIVE_CHUNK + 1; i++) {
            client.subscribeKline(new Instrument("G" + i, "USDT", null), "1", k -> {});
        }

        awaitTrue(() -> ws.sent.size() == 3);
        assertThat(ws.sentAtNanos.get(2) - ws.sentAtNanos.get(1)).isGreaterThanOrEqualTo(gap.toNanos() - 5_000_000L);
    }

    // ------------------------------------------------------------------ idempotence and ordering

    @Test
    void aTopicAlreadySentWithTheInitialSubscribeIsNeverRequestedAgain() throws Exception {
        FakeWs ws = new FakeWs();
        BybitStreamClient client = client(ws, Duration.ZERO);
        client.subscribeKline(BTC, "1", k -> {});
        client.connect();

        client.subscribeKline(BTC, "1", k -> {});
        pause();

        assertThat(ws.sent).hasSize(1);
    }

    @Test
    void aTopicRequestedLiveIsNotRequestedTwice() throws Exception {
        FakeWs ws = new FakeWs();
        BybitStreamClient client = client(ws, Duration.ZERO);
        client.subscribeKline(BTC, "1", k -> {});
        client.connect();

        client.subscribeKline(ETH, "1", k -> {});
        awaitTrue(() -> ws.sent.size() == 2);
        client.subscribeKline(ETH, "1", k -> {});
        pause();

        assertThat(ws.sent).hasSize(2);
    }

    @Test
    void tickerAndFundingRateOfOnePerpShareOneLiveTopic() throws Exception {
        FakeWs ws = new FakeWs();
        BybitStreamClient client = new BybitStreamClient(
                config(ws), BybitStreamClient.Category.LINEAR, () -> "{\"result\":{\"list\":[]}}",
                Duration.ofMillis(100), Duration.ZERO);
        client.subscribeTicker(new Instrument("BTC", "USDT", "USDT"), t -> {});
        client.connect();

        client.subscribeTicker(ETH_PERP, t -> {});
        client.subscribeFundingRate(ETH_PERP, f -> {});

        awaitTrue(() -> ws.sent.size() == 2);
        pause();
        assertThat(ws.sent).hasSize(2);
        assertThat(MAPPER.readTree(ws.sent.get(1)).get("args")).extracting(JsonNode::asText)
                .containsExactly("tickers.ETHUSDT");
        client.close();
    }

    @Test
    void beforeTheSocketIsOpenNothingIsSent() {
        FakeWs ws = new FakeWs();
        BybitStreamClient client = client(ws, Duration.ZERO);
        client.subscribeKline(BTC, "1", k -> {});
        client.subscribeKline(ETH, "1", k -> {});
        pause();

        assertThat(ws.sent).isEmpty();
        assertThat(client.pendingTopics()).containsExactly("kline.1.BTCUSDT", "kline.1.ETHUSDT");
    }

    @Test
    void aTopicRegisteredWhileTheInitialFramesAreGoingOutWaitsForThemAndThenIsRequested() throws Exception {
        FakeWs ws = new FakeWs();
        BybitStreamClient client = client(ws, Duration.ZERO);
        client.subscribeKline(BTC, "1", k -> {});
        // the socket is open and the initial frame is being sent (and held for a moment) when someone
        // registers a topic: a live frame sent now would overtake the initial one
        ws.duringFirstSend = () -> client.subscribeKline(ETH, "1", k -> {});
        ws.holdFirstSendMillis = 400;

        client.connect();

        awaitTrue(() -> ws.sent.size() == 2);
        assertThat(MAPPER.readTree(ws.sent.get(0)).has("req_id")).isFalse();      // the initial frame first
        assertThat(MAPPER.readTree(ws.sent.get(0)).get("args")).extracting(JsonNode::asText)
                .containsExactly("kline.1.BTCUSDT");
        assertThat(MAPPER.readTree(ws.sent.get(1)).get("args")).extracting(JsonNode::asText)
                .containsExactly("kline.1.ETHUSDT");
    }

    @Test
    void aTopicRegisteredWhileTheHandshakeRunsIsRequestedOnceTheInitialFramesAreOut() throws Exception {
        FakeWs ws = new FakeWs();
        BybitStreamClient client = client(ws, Duration.ZERO);
        client.subscribeKline(BTC, "1", k -> {});
        ws.duringConnect = () -> client.subscribeKline(ETH, "1", k -> {});

        client.connect();

        awaitTrue(() -> ws.sent.size() == 2);
        assertThat(MAPPER.readTree(ws.sent.get(0)).get("args")).extracting(JsonNode::asText)
                .containsExactly("kline.1.BTCUSDT");
        assertThat(MAPPER.readTree(ws.sent.get(1)).get("args")).extracting(JsonNode::asText)
                .containsExactly("kline.1.ETHUSDT");
    }

    // ------------------------------------------------------------------ failures

    @Test
    void aLinkThatDropsBetweenTheQueueAndTheSendSendsNothingAndKeepsTheTopic() throws Exception {
        FakeWs ws = new FakeWs();
        BybitStreamClient client = client(ws, Duration.ofMillis(300));
        client.subscribeKline(BTC, "1", k -> {});
        client.connect();

        client.subscribeKline(ETH, "1", k -> {});
        ws.open = false;                      // the socket closes while the request waits to be sent
        Thread.sleep(700);

        assertThat(ws.sent).hasSize(1);       // only the initial frame
        assertThat(client.pendingTopics()).containsExactly("kline.1.BTCUSDT", "kline.1.ETHUSDT");

        // registering a topic the registry already holds sends nothing, even on a reopened socket
        ws.open = true;
        client.subscribeKline(ETH, "1", k -> {});
        pause();
        assertThat(ws.sent).hasSize(1);
    }

    @Test
    void aSendThatFailsIsContainedAndTheClientKeepsWorking() throws Exception {
        FakeWs ws = new FakeWs();
        BybitStreamClient client = client(ws, Duration.ZERO);
        client.subscribeKline(BTC, "1", k -> {});
        client.connect();

        ws.failNextSend = true;
        client.subscribeKline(ETH, "1", k -> {});
        awaitTrue(() -> ws.failedSends == 1);
        pause();
        assertThat(ws.sent).hasSize(1);

        client.subscribeKline(SOL, "1", k -> {});
        awaitTrue(() -> ws.sent.size() == 2);
        assertThat(MAPPER.readTree(ws.sent.get(1)).get("args")).extracting(JsonNode::asText)
                .containsExactly("kline.1.SOLUSDT");
    }

    @Test
    void anAnsweredRequestIsNoLongerTrackedWhetherAcceptedOrRefused() throws Exception {
        FakeWs ws = new FakeWs();
        BybitStreamClient client = client(ws, Duration.ZERO);
        client.subscribeKline(BTC, "1", k -> {});
        client.connect();
        client.subscribeKline(ETH, "1", k -> {});
        awaitTrue(() -> ws.sent.size() == 2);
        assertThat(client.liveInFlightSize()).isEqualTo(1);

        ws.receive("{\"success\":true,\"ret_msg\":\"\",\"conn_id\":\"c\",\"req_id\":\"live-1\",\"op\":\"subscribe\"}");
        assertThat(client.liveInFlightSize()).isZero();

        client.subscribeKline(SOL, "1", k -> {});
        awaitTrue(() -> ws.sent.size() == 3);
        ws.receive("{\"success\":false,\"ret_msg\":\"error:handler not found\",\"conn_id\":\"c\",\"req_id\":\"live-2\",\"op\":\"subscribe\"}");
        assertThat(client.liveInFlightSize()).isZero();
    }

    @Test
    void aRefusedLiveFrameIsResentWithoutTheTopicBybitNamed() throws Exception {
        FakeWs ws = new FakeWs();
        BybitStreamClient client = client(ws, Duration.ofMillis(300));
        client.subscribeKline(BTC, "1", k -> {});
        client.connect();
        client.subscribeKline(ETH, "1", k -> {});
        client.subscribeKline(SOL, "1", k -> {});
        client.subscribeKline(new Instrument("XRP", "USDT", null), "1", k -> {});
        awaitTrue(() -> ws.sent.size() == 2);
        assertThat(MAPPER.readTree(ws.sent.get(1)).get("args")).hasSize(3);

        ws.receive("{\"success\":false,\"ret_msg\":\"error:handler not found,topic:kline.1.SOLUSDT\","
                + "\"conn_id\":\"c\",\"req_id\":\"live-1\",\"op\":\"subscribe\"}");

        awaitTrue(() -> ws.sent.size() == 3);
        JsonNode resent = MAPPER.readTree(ws.sent.get(2));
        assertThat(resent.has("req_id")).as("re-sent as a plain frame: its answer is handled like an initial one").isFalse();
        assertThat(resent.get("args")).extracting(JsonNode::asText).containsExactly("kline.1.ETHUSDT", "kline.1.XRPUSDT");
        assertThat(client.rejectedTopics()).containsExactly("kline.1.SOLUSDT");
        assertThat(client.liveInFlightSize()).isZero();
    }

    @Test
    void aUsdcPerpetualAddedLiveIsRequestedByItsListedSymbol() throws Exception {
        BybitInstrumentsCache.parse(
                "{\"result\":{\"nextPageCursor\":\"\",\"list\":[{\"symbol\":\"LIVEXPERP\",\"contractType\":\"LinearPerpetual\","
                        + "\"status\":\"Trading\",\"baseCoin\":\"LIVEX\",\"quoteCoin\":\"USDC\",\"settleCoin\":\"USDC\"}]}}",
                BybitInstrumentsCache.Category.LINEAR);
        FakeWs ws = new FakeWs();
        BybitStreamClient client = new BybitStreamClient(
                config(ws), BybitStreamClient.Category.LINEAR, null, Duration.ZERO, Duration.ZERO);
        client.subscribeTicker(ETH_PERP, t -> {});
        client.connect();

        client.subscribeTicker(new Instrument("LIVEX", "USDC", "USDC"), t -> {});

        awaitTrue(() -> ws.sent.size() == 2);
        assertThat(MAPPER.readTree(ws.sent.get(1)).get("args")).extracting(JsonNode::asText)
                .containsExactly("tickers.LIVEXPERP");
    }

    @Test
    void repliesWithoutALiveRequestIdAreLeftAloneAndDoNotBreakTheHandler() throws Exception {
        FakeWs ws = new FakeWs();
        BybitStreamClient client = client(ws, Duration.ZERO);
        client.subscribeKline(BTC, "1", k -> {});
        client.connect();

        ws.receive("{\"success\":true,\"ret_msg\":\"subscribe\",\"conn_id\":\"c\",\"op\":\"subscribe\"}");   // the initial subscribe's ack
        ws.receive("{\"success\":false,\"ret_msg\":\"x\",\"op\":\"subscribe\"}");
        ws.receive("{\"op\":\"pong\",\"success\":true}");
        ws.receive("{\"req_id\":\"other-1\",\"success\":false}");
        ws.receive("not json");

        client.subscribeKline(ETH, "1", k -> {});
        awaitTrue(() -> ws.sent.size() == 2);
        assertThat(client.liveInFlightSize()).isEqualTo(1);
    }

    @Test
    void aNewConnectionForgetsTheRequestsOfThePreviousSocket() throws Exception {
        FakeWs ws = new FakeWs();
        BybitStreamClient client = client(ws, Duration.ZERO);
        client.subscribeKline(BTC, "1", k -> {});
        client.connect();
        client.subscribeKline(ETH, "1", k -> {});
        awaitTrue(() -> ws.sent.size() == 2);
        assertThat(client.liveInFlightSize()).isEqualTo(1);

        client.connect();

        assertThat(client.liveInFlightSize()).isZero();
        // the registry, now carrying both topics, is what the new initial subscribe is built from
        assertThat(MAPPER.readTree(ws.sent.get(2)).get("args")).extracting(JsonNode::asText)
                .containsExactly("kline.1.BTCUSDT", "kline.1.ETHUSDT");
    }

    @Test
    void aTopicQueuedWhileAFrameIsBeingSentGoesOutInTheNextFrame() throws Exception {
        FakeWs ws = new FakeWs();
        BybitStreamClient client = client(ws, Duration.ZERO);
        client.subscribeKline(BTC, "1", k -> {});
        client.connect();

        ws.gate = new CountDownLatch(1);
        client.subscribeKline(ETH, "1", k -> {});
        awaitTrue(() -> ws.sendsStarted == 2);          // the live frame is held inside send()
        client.subscribeKline(SOL, "1", k -> {});       // registered while it is held
        ws.gate.countDown();

        awaitTrue(() -> ws.sent.size() == 3);
        assertThat(MAPPER.readTree(ws.sent.get(1)).get("args")).extracting(JsonNode::asText).containsExactly("kline.1.ETHUSDT");
        assertThat(MAPPER.readTree(ws.sent.get(2)).get("args")).extracting(JsonNode::asText).containsExactly("kline.1.SOLUSDT");
    }

    @Test
    void theFrameIsExactlyTheDocumentedShape() {
        assertThat(BybitStreamClient.liveFrame(List.of("kline.1.A", "tickers.B"), "live-7"))
                .isEqualTo("{\"req_id\":\"live-7\",\"op\":\"subscribe\",\"args\":[\"kline.1.A\",\"tickers.B\"]}");
        assertThat(BybitStreamClient.LIVE_CHUNK).isBetween(1, 10);
    }

    // ------------------------------------------------------------------ helpers

    private static BybitStreamClient client(FakeWs ws, Duration coalesce) {
        return new BybitStreamClient(config(ws), BybitStreamClient.Category.SPOT, null, coalesce, Duration.ZERO);
    }

    private static StreamClientConfig config(FakeWs ws) {
        return StreamClientConfig.withDefaults(() -> ws);
    }

    private static List<Integer> sizes(List<String> frames) throws Exception {
        List<Integer> out = new ArrayList<>();
        for (String f : frames) {
            out.add(MAPPER.readTree(f).get("args").size());
        }
        return out;
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

    /** A websocket that records what is sent and lets the test open, close and answer it. */
    private static final class FakeWs implements WebSocketClient {
        volatile boolean open;
        volatile boolean failNextSend;
        volatile int failedSends;
        volatile int sendsStarted;
        volatile Runnable duringConnect = () -> {};
        volatile Runnable duringFirstSend;
        volatile long holdFirstSendMillis;
        volatile CountDownLatch gate;
        final List<String> sent = new CopyOnWriteArrayList<>();
        final List<Long> sentAtNanos = new CopyOnWriteArrayList<>();
        private volatile Consumer<String> messageHandler = m -> {};

        @Override public void connect(String url) {
            duringConnect.run();
            open = true;
        }
        @Override public void send(String message) {
            if (message.contains("\"op\":\"ping\"")) {
                return;                                      // the keepalive is not under test
            }
            int n = ++sendsStarted;
            Runnable hook = duringFirstSend;
            if (n == 1 && hook != null) {
                hook.run();
            }
            if (n == 1 && holdFirstSendMillis > 0) {
                try {
                    Thread.sleep(holdFirstSendMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            CountDownLatch g = gate;
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
