package com.qtsurfer.qtstreamx.exchange.bybit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.qtsurfer.qtstreamx.core.client.StreamClientConfig;
import com.qtsurfer.qtstreamx.core.model.FundingRate;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.model.Kline;
import com.qtsurfer.qtstreamx.core.model.Ticker;
import com.qtsurfer.qtstreamx.core.ws.WebSocketClient;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

/**
 * Wiring tests for {@link BybitStreamClient}. Uses a fake {@link WebSocketClient} so we can
 * inject Bybit v5 JSON frames verbatim and assert the right handler fires.
 */
class BybitStreamClientTest {

    private static final Instrument BTC_USDT_SPOT = new Instrument("BTC", "USDT", null);
    private static final Instrument BTC_USDT_PERP = new Instrument("BTC", "USDT", "USDT");

    @Test
    void subscribeTickerQueuesTopicAndSendsSubscribeOnConnect() throws Exception {
        FakeWs ws = new FakeWs();
        BybitStreamClient client = BybitStreamClient.spot(fakeConfig(ws));
        client.subscribeTicker(BTC_USDT_SPOT, t -> {});

        assertThat(client.pendingTopics()).containsExactly("tickers.BTCUSDT");
        client.connect();
        assertThat(ws.connectedUrl).isEqualTo("wss://stream.bybit.com/v5/public/spot");
        assertThat(ws.sentFrames).containsExactly("{\"op\":\"subscribe\",\"args\":[\"tickers.BTCUSDT\"]}");
    }

    @Test
    void subscribeKlineEncodesIntervalInTopic() {
        FakeWs ws = new FakeWs();
        BybitStreamClient client = BybitStreamClient.spot(fakeConfig(ws));
        client.subscribeKline(BTC_USDT_SPOT, "1", k -> {});

        assertThat(client.pendingTopics()).containsExactly("kline.1.BTCUSDT");
    }

    @Test
    void subscribeFundingRateOnSpotIsRejected() {
        BybitStreamClient client = BybitStreamClient.spot(fakeConfig(new FakeWs()));
        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> client.subscribeFundingRate(BTC_USDT_PERP, f -> {}));
    }

    @Test
    void subscribeTickerAndFundingRateShareOneTopicOnLinear() throws Exception {
        FakeWs ws = new FakeWs();
        BybitStreamClient client = BybitStreamClient.linear(fakeConfig(ws));
        client.subscribeTicker(BTC_USDT_PERP, t -> {});
        client.subscribeFundingRate(BTC_USDT_PERP, f -> {});

        assertThat(client.pendingTopics()).containsExactly("tickers.BTCUSDT");
        client.connect();
        assertThat(ws.sentFrames).hasSize(1); // single subscribe frame
    }

    @Test
    void linearTickerMessageDispatchesToBothTickerAndFundingHandlers() throws Exception {
        FakeWs ws = new FakeWs();
        BybitStreamClient client = BybitStreamClient.linear(fakeConfig(ws));
        AtomicReference<Ticker> tickerRef = new AtomicReference<>();
        AtomicReference<FundingRate> frRef = new AtomicReference<>();
        client.subscribeTicker(BTC_USDT_PERP, tickerRef::set);
        client.subscribeFundingRate(BTC_USDT_PERP, frRef::set);
        client.connect();

        // Feed the canned linear ticker payload — both subscribers must see their projection.
        ws.deliver(loadFixture("bybit-ticker-linear.json"));

        assertThat(tickerRef.get()).isNotNull();
        assertThat(tickerRef.get().last()).isEqualByComparingTo("17216.00");
        assertThat(frRef.get()).isNotNull();
        assertThat(frRef.get().rate()).isEqualByComparingTo("-0.000212");
    }

    @Test
    void fundingRateEmittedOnDeltasUsingCachedSnapshotValues() throws Exception {
        FakeWs ws = new FakeWs();
        BybitStreamClient client = BybitStreamClient.linear(fakeConfig(ws));
        java.util.List<FundingRate> captured = new java.util.ArrayList<>();
        client.subscribeFundingRate(BTC_USDT_PERP, captured::add);
        client.connect();

        // Snapshot with fundingRate + nextFundingTime.
        ws.deliver(loadFixture("bybit-ticker-linear.json"));
        int afterSnapshot = captured.size();

        // Delta that OMITS fundingRate/nextFundingTime — typical Bybit v5 behaviour.
        ws.deliver(
                "{\"topic\":\"tickers.BTCUSDT\",\"ts\":123,\"type\":\"delta\","
                        + "\"data\":{\"symbol\":\"BTCUSDT\",\"lastPrice\":\"17300\",\"markPrice\":\"17299.5\"}}");

        assertThat(captured).hasSizeGreaterThan(afterSnapshot);
        FundingRate latest = captured.get(captured.size() - 1);
        // Cached from the snapshot.
        assertThat(latest.rate()).isEqualByComparingTo("-0.000212");
        // markPrice is the one from the delta — refreshed each tick.
        assertThat(latest.markPrice()).isEqualByComparingTo("17299.5");
    }

    @Test
    void fundingRateNotEmittedBeforeFirstSnapshot() throws Exception {
        FakeWs ws = new FakeWs();
        BybitStreamClient client = BybitStreamClient.linear(fakeConfig(ws));
        java.util.List<FundingRate> captured = new java.util.ArrayList<>();
        client.subscribeFundingRate(BTC_USDT_PERP, captured::add);
        client.connect();

        // First ticker without fundingRate → cache empty → no emission.
        ws.deliver(
                "{\"topic\":\"tickers.BTCUSDT\",\"ts\":1,\"type\":\"delta\","
                        + "\"data\":{\"symbol\":\"BTCUSDT\",\"lastPrice\":\"17300\"}}");
        assertThat(captured).isEmpty();
    }

    @Test
    void klineMessageDispatchesToKlineHandler() throws Exception {
        FakeWs ws = new FakeWs();
        BybitStreamClient client = BybitStreamClient.spot(fakeConfig(ws));
        AtomicReference<Kline> klineRef = new AtomicReference<>();
        client.subscribeKline(BTC_USDT_SPOT, "1", klineRef::set);
        client.connect();

        ws.deliver(loadFixture("bybit-kline.json"));

        assertThat(klineRef.get()).isNotNull();
        assertThat(klineRef.get().close()).isEqualByComparingTo("16677");
        assertThat(klineRef.get().closed()).isFalse();
    }

    @Test
    void messageOnUnsubscribedTopicIsIgnored() throws Exception {
        FakeWs ws = new FakeWs();
        BybitStreamClient client = BybitStreamClient.spot(fakeConfig(ws));
        AtomicReference<Ticker> tickerRef = new AtomicReference<>();
        client.subscribeTicker(BTC_USDT_SPOT, tickerRef::set);
        client.connect();

        // A ticker for an instrument we never subscribed to — must not fire the BTC handler.
        ws.deliver(
                "{\"topic\":\"tickers.ETHUSDT\",\"ts\":1,\"type\":\"snapshot\",\"data\":{\"symbol\":\"ETHUSDT\",\"lastPrice\":\"1000\"}}");

        assertThat(tickerRef.get()).isNull();
    }

    @Test
    void topicsChunkedTenAtATimeInSubscribeFrames() throws Exception {
        FakeWs ws = new FakeWs();
        BybitStreamClient client = BybitStreamClient.spot(fakeConfig(ws));
        for (int i = 0; i < 25; i++) {
            client.subscribeTicker(
                    new Instrument("T" + i, "USDT", null), t -> {});
        }
        client.connect();

        // 25 topics → 3 frames (10 + 10 + 5).
        assertThat(ws.sentFrames).hasSize(3);
        assertThat(ws.sentFrames.get(0)).contains("T0USDT").contains("T9USDT");
        assertThat(ws.sentFrames.get(2)).contains("T20USDT").contains("T24USDT");
    }

    /* ------------------------------------------------------------- */

    private static StreamClientConfig fakeConfig(WebSocketClient ws) {
        return StreamClientConfig.withDefaults(() -> ws);
    }

    private static String loadFixture(String name) throws Exception {
        try (InputStream in = BybitStreamClientTest.class.getResourceAsStream("/" + name)) {
            Objects.requireNonNull(in, name + " missing from classpath");
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    /** Canned WS client: records sent frames + lets tests inject inbound messages. */
    private static final class FakeWs implements WebSocketClient {
        Consumer<String> onMessage = s -> {};
        String connectedUrl;
        final List<String> sentFrames = new ArrayList<>();

        @Override public void connect(String url) { this.connectedUrl = url; }
        @Override public void send(String message) { sentFrames.add(message); }
        @Override public void onMessage(Consumer<String> handler) { this.onMessage = handler; }
        @Override public void onClose(BiConsumer<Integer, String> handler) {}
        @Override public void onError(Consumer<Throwable> handler) {}
        @Override public boolean isOpen() { return connectedUrl != null; }
        @Override public void close() {}

        /** Inject a raw JSON frame as if the server had pushed it. */
        void deliver(String message) { onMessage.accept(message); }
    }
}
