package com.qtsurfer.qtstreamx.exchange.okx;

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
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

class OkxStreamClientTest {

    private static final Instrument BTC_USDT_SPOT = new Instrument("BTC", "USDT", null);
    private static final Instrument BTC_USDT_SWAP = new Instrument("BTC", "USDT", "USDT");

    @Test
    void subscribeTickerRoutesToPublicBucket() throws Exception {
        FakeWs ws = new FakeWs();
        OkxStreamClient client = new OkxStreamClient(fakeConfig(ws));
        client.subscribeTicker(BTC_USDT_SPOT, t -> {});

        assertThat(client.pendingPublicArgs())
                .containsExactly(new OkxStreamClient.SubscribeArg("tickers", "BTC-USDT"));
        assertThat(client.pendingBusinessArgs()).isEmpty();
        client.connect();
        assertThat(ws.connectedUrl).isEqualTo(OkxStreamClient.PUBLIC_WS);
    }

    @Test
    void subscribeKlineRoutesToBusinessBucket() throws Exception {
        FakeWsFactory fact = new FakeWsFactory();
        OkxStreamClient client = new OkxStreamClient(StreamClientConfig.withDefaults(fact));
        client.subscribeKline(BTC_USDT_SPOT, "1m", k -> {});

        assertThat(client.pendingBusinessArgs())
                .containsExactly(new OkxStreamClient.SubscribeArg("candle1m", "BTC-USDT"));
        assertThat(client.pendingPublicArgs()).isEmpty();
        client.connect();
        assertThat(fact.instances).hasSize(1);
        assertThat(fact.instances.get(0).connectedUrl).isEqualTo(OkxStreamClient.BUSINESS_WS);
    }

    @Test
    void subscribeFundingRateRejectsSpot() {
        OkxStreamClient client = new OkxStreamClient(fakeConfig(new FakeWs()));
        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> client.subscribeFundingRate(BTC_USDT_SPOT, f -> {}));
    }

    @Test
    void subscribeFundingRateRoutesToPublicWithSwapSuffix() {
        OkxStreamClient client = new OkxStreamClient(fakeConfig(new FakeWs()));
        client.subscribeFundingRate(BTC_USDT_SWAP, f -> {});

        assertThat(client.pendingPublicArgs())
                .containsExactly(new OkxStreamClient.SubscribeArg("funding-rate", "BTC-USDT-SWAP"));
        assertThat(client.pendingBusinessArgs()).isEmpty();
    }

    @Test
    void tickerAndKlineOpenTwoWsConnections() throws Exception {
        FakeWsFactory fact = new FakeWsFactory();
        OkxStreamClient client = new OkxStreamClient(StreamClientConfig.withDefaults(fact));
        client.subscribeTicker(BTC_USDT_SPOT, t -> {});
        client.subscribeKline(BTC_USDT_SPOT, "1m", k -> {});
        client.connect();

        assertThat(fact.instances).hasSize(2);
        // Order: public first, then business.
        assertThat(fact.instances.get(0).connectedUrl).isEqualTo(OkxStreamClient.PUBLIC_WS);
        assertThat(fact.instances.get(1).connectedUrl).isEqualTo(OkxStreamClient.BUSINESS_WS);
    }

    @Test
    void tickerMessageDispatchesToHandler() throws Exception {
        FakeWs ws = new FakeWs();
        OkxStreamClient client = new OkxStreamClient(fakeConfig(ws));
        AtomicReference<Ticker> ref = new AtomicReference<>();
        client.subscribeTicker(BTC_USDT_SPOT, ref::set);
        client.connect();

        ws.deliver(loadFixture("okx-ticker-spot.json"));

        assertThat(ref.get()).isNotNull();
        assertThat(ref.get().last()).isEqualByComparingTo("9999.99");
    }

    @Test
    void fundingRateMessageDispatchesToHandler() throws Exception {
        FakeWs ws = new FakeWs();
        OkxStreamClient client = new OkxStreamClient(fakeConfig(ws));
        AtomicReference<FundingRate> ref = new AtomicReference<>();
        client.subscribeFundingRate(BTC_USDT_SWAP, ref::set);
        client.connect();

        ws.deliver(loadFixture("okx-funding-rate.json"));

        assertThat(ref.get()).isNotNull();
        assertThat(ref.get().rate()).isEqualByComparingTo("0.0012");
    }

    @Test
    void klineMessageDispatchesToHandler() throws Exception {
        FakeWs ws = new FakeWs();
        OkxStreamClient client = new OkxStreamClient(fakeConfig(ws));
        AtomicReference<Kline> ref = new AtomicReference<>();
        client.subscribeKline(BTC_USDT_SPOT, "1s", ref::set);
        client.connect();

        ws.deliver(loadFixture("okx-kline.json"));

        assertThat(ref.get()).isNotNull();
        assertThat(ref.get().close()).isEqualByComparingTo("10000.25");
    }

    @Test
    void argsChunkedAt50PerSubscribeFrame() throws Exception {
        FakeWs ws = new FakeWs();
        OkxStreamClient client = new OkxStreamClient(fakeConfig(ws));
        for (int i = 0; i < 120; i++) {
            client.subscribeTicker(new Instrument("T" + i, "USDT", null), t -> {});
        }
        client.connect();

        // 120 args → 3 frames (50 + 50 + 20).
        assertThat(ws.sentFrames).hasSize(3);
    }

    @Test
    void unsubscribedChannelMessagesAreIgnored() throws Exception {
        FakeWs ws = new FakeWs();
        OkxStreamClient client = new OkxStreamClient(fakeConfig(ws));
        AtomicReference<Ticker> ref = new AtomicReference<>();
        client.subscribeTicker(BTC_USDT_SPOT, ref::set);
        client.connect();

        ws.deliver(
                "{\"arg\":{\"channel\":\"tickers\",\"instId\":\"ETH-USDT\"},\"data\":[{\"instId\":\"ETH-USDT\",\"last\":\"1\",\"ts\":\"1\"}]}");

        assertThat(ref.get()).isNull();
    }

    /* ------------------------------------------------------------- */

    private static StreamClientConfig fakeConfig(WebSocketClient ws) {
        return StreamClientConfig.withDefaults(() -> ws);
    }

    private static String loadFixture(String name) throws Exception {
        try (InputStream in = OkxStreamClientTest.class.getResourceAsStream("/" + name)) {
            Objects.requireNonNull(in, name + " missing from classpath");
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static final class FakeWsFactory implements Supplier<WebSocketClient> {
        final List<FakeWs> instances = new ArrayList<>();

        @Override
        public FakeWs get() {
            FakeWs ws = new FakeWs();
            instances.add(ws);
            return ws;
        }
    }

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

        void deliver(String message) { onMessage.accept(message); }
    }
}
