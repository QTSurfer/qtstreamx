package com.qtsurfer.qtstreamx.exchange.htx;

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

class HtxSpotStreamClientTest {

    private static final Instrument BTC_USDT_SPOT = new Instrument("BTC", "USDT", null);
    private static final Instrument BTC_USDT_PERP = new Instrument("BTC", "USDT", "USDT");

    @Test
    void tickerSubscribeBuildsDetailTopic() throws Exception {
        FakeWs ws = new FakeWs();
        HtxSpotStreamClient client = new HtxSpotStreamClient(StreamClientConfig.withDefaults(() -> ws));
        client.subscribeTicker(BTC_USDT_SPOT, t -> {});
        client.connect();

        assertThat(client.pendingSubs()).containsExactly("market.btcusdt.detail");
        assertThat(ws.sentFrames).hasSize(1);
        assertThat(ws.sentFrames.get(0)).contains("\"sub\":\"market.btcusdt.detail\"");
    }

    @Test
    void klineSubscribeBuildsKlineTopic() throws Exception {
        FakeWs ws = new FakeWs();
        HtxSpotStreamClient client = new HtxSpotStreamClient(StreamClientConfig.withDefaults(() -> ws));
        client.subscribeKline(BTC_USDT_SPOT, "1min", k -> {});
        client.connect();

        assertThat(client.pendingSubs()).containsExactly("market.btcusdt.kline.1min");
        assertThat(ws.sentFrames.get(0)).contains("market.btcusdt.kline.1min");
    }

    @Test
    void spotRejectsFundingRate() {
        HtxSpotStreamClient client =
                new HtxSpotStreamClient(StreamClientConfig.withDefaults(FakeWs::new));
        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> client.subscribeFundingRate(BTC_USDT_PERP, f -> {}));
    }

    @Test
    void connectWithoutSubsFails() {
        HtxSpotStreamClient client =
                new HtxSpotStreamClient(StreamClientConfig.withDefaults(FakeWs::new));
        assertThatExceptionOfType(IllegalStateException.class).isThrownBy(client::connect);
    }

    @Test
    void tickerMessageDispatches() throws Exception {
        FakeWs ws = new FakeWs();
        HtxSpotStreamClient client = new HtxSpotStreamClient(StreamClientConfig.withDefaults(() -> ws));
        AtomicReference<Ticker> ref = new AtomicReference<>();
        client.subscribeTicker(BTC_USDT_SPOT, ref::set);
        client.connect();

        ws.deliver(loadFixture("htx-ticker-spot.json"));
        assertThat(ref.get()).isNotNull();
        assertThat(ref.get().last()).isEqualByComparingTo("65010.5");
    }

    @Test
    void klineMessageDispatches() throws Exception {
        FakeWs ws = new FakeWs();
        HtxSpotStreamClient client = new HtxSpotStreamClient(StreamClientConfig.withDefaults(() -> ws));
        AtomicReference<Kline> ref = new AtomicReference<>();
        client.subscribeKline(BTC_USDT_SPOT, "1min", ref::set);
        client.connect();

        ws.deliver(loadFixture("htx-kline.json"));
        assertThat(ref.get()).isNotNull();
        assertThat(ref.get().close()).isEqualByComparingTo("65040.0");
    }

    @Test
    void pingIsSwallowed() throws Exception {
        FakeWs ws = new FakeWs();
        HtxSpotStreamClient client = new HtxSpotStreamClient(StreamClientConfig.withDefaults(() -> ws));
        AtomicReference<Ticker> ref = new AtomicReference<>();
        client.subscribeTicker(BTC_USDT_SPOT, ref::set);
        client.connect();

        ws.deliver("{\"ping\":1713715200000}");
        assertThat(ref.get()).isNull();
    }

    @Test
    void unknownChannelIgnored() throws Exception {
        FakeWs ws = new FakeWs();
        HtxSpotStreamClient client = new HtxSpotStreamClient(StreamClientConfig.withDefaults(() -> ws));
        AtomicReference<Ticker> ref = new AtomicReference<>();
        client.subscribeTicker(BTC_USDT_SPOT, ref::set);
        client.connect();

        ws.deliver("{\"ch\":\"market.ethusdt.detail\",\"ts\":1,\"tick\":{\"close\":1}}");
        assertThat(ref.get()).isNull();
    }

    @Test
    void onDisconnectFiredOnce() throws Exception {
        FakeWs ws = new FakeWs();
        HtxSpotStreamClient client = new HtxSpotStreamClient(StreamClientConfig.withDefaults(() -> ws));
        client.subscribeTicker(BTC_USDT_SPOT, t -> {});
        int[] fired = {0};
        client.onDisconnect(() -> fired[0]++);
        client.connect();

        ws.onClose.accept(1000, "test");
        ws.onError.accept(new RuntimeException("boom"));
        assertThat(fired[0]).isEqualTo(1);
    }

    private static String loadFixture(String name) throws Exception {
        try (InputStream in = HtxSpotStreamClientTest.class.getResourceAsStream("/" + name)) {
            Objects.requireNonNull(in, name);
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static final class FakeWs implements WebSocketClient {
        Consumer<String> onMessage = s -> {};
        BiConsumer<Integer, String> onClose = (c, r) -> {};
        Consumer<Throwable> onError = t -> {};
        String connectedUrl;
        final List<String> sentFrames = new ArrayList<>();

        @Override public void connect(String url) { this.connectedUrl = url; }
        @Override public void send(String message) { sentFrames.add(message); }
        @Override public void onMessage(Consumer<String> handler) { this.onMessage = handler; }
        @Override public void onClose(BiConsumer<Integer, String> handler) { this.onClose = handler; }
        @Override public void onError(Consumer<Throwable> handler) { this.onError = handler; }
        @Override public boolean isOpen() { return connectedUrl != null; }
        @Override public void close() {}

        void deliver(String message) { onMessage.accept(message); }
    }
}
