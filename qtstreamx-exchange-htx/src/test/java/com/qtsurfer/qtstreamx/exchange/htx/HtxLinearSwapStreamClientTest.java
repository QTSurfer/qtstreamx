package com.qtsurfer.qtstreamx.exchange.htx;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.qtsurfer.qtstreamx.core.client.StreamClientConfig;
import com.qtsurfer.qtstreamx.core.model.FundingRate;
import com.qtsurfer.qtstreamx.core.model.Instrument;
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

class HtxLinearSwapStreamClientTest {

    private static final Instrument BTC_USDT_SPOT = new Instrument("BTC", "USDT", null);
    private static final Instrument BTC_USDT_PERP = new Instrument("BTC", "USDT", "USDT");

    @Test
    void tickerTopicRoutesToMarketWs() throws Exception {
        FakeWsFactory fact = new FakeWsFactory();
        HtxLinearSwapStreamClient client =
                new HtxLinearSwapStreamClient(StreamClientConfig.withDefaults(fact));
        client.subscribeTicker(BTC_USDT_PERP, t -> {});
        client.connect();

        assertThat(client.pendingMarketSubs()).containsExactly("market.BTC-USDT.detail");
        assertThat(client.pendingNotifySubs()).isEmpty();
        // Only market WS was created
        assertThat(fact.instances).hasSize(1);
        assertThat(fact.instances.get(0).connectedUrl).isEqualTo(HtxLinearSwapStreamClient.MARKET_URL);
    }

    @Test
    void fundingTopicRoutesToNotifyWs() throws Exception {
        FakeWsFactory fact = new FakeWsFactory();
        HtxLinearSwapStreamClient client =
                new HtxLinearSwapStreamClient(StreamClientConfig.withDefaults(fact));
        client.subscribeFundingRate(BTC_USDT_PERP, f -> {});
        client.connect();

        assertThat(client.pendingNotifySubs()).containsExactly("public.BTC-USDT.funding_rate");
        assertThat(client.pendingMarketSubs()).isEmpty();
        assertThat(fact.instances).hasSize(1);
        assertThat(fact.instances.get(0).connectedUrl).isEqualTo(HtxLinearSwapStreamClient.NOTIFY_URL);
        // Notify uses "op":"sub" frame, not "sub":"..."
        assertThat(fact.instances.get(0).sentFrames.get(0)).contains("\"op\":\"sub\"");
    }

    @Test
    void bothTickerAndFundingOpensTwoWs() throws Exception {
        FakeWsFactory fact = new FakeWsFactory();
        HtxLinearSwapStreamClient client =
                new HtxLinearSwapStreamClient(StreamClientConfig.withDefaults(fact));
        client.subscribeTicker(BTC_USDT_PERP, t -> {});
        client.subscribeFundingRate(BTC_USDT_PERP, f -> {});
        client.connect();

        assertThat(fact.instances).hasSize(2);
    }

    @Test
    void fundingRejectsSpotInstrument() {
        HtxLinearSwapStreamClient client =
                new HtxLinearSwapStreamClient(StreamClientConfig.withDefaults(FakeWs::new));
        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> client.subscribeFundingRate(BTC_USDT_SPOT, f -> {}));
    }

    @Test
    void tickerMessageDispatches() throws Exception {
        FakeWsFactory fact = new FakeWsFactory();
        HtxLinearSwapStreamClient client =
                new HtxLinearSwapStreamClient(StreamClientConfig.withDefaults(fact));
        AtomicReference<Ticker> ref = new AtomicReference<>();
        client.subscribeTicker(BTC_USDT_PERP, ref::set);
        client.connect();

        fact.instances.get(0).deliver(loadFixture("htx-ticker-linear.json"));
        assertThat(ref.get()).isNotNull();
        assertThat(ref.get().last()).isEqualByComparingTo("65020.5");
        assertThat(ref.get().volume()).isEqualByComparingTo("4321.0");
    }

    @Test
    void fundingRateMessageDispatches() throws Exception {
        FakeWsFactory fact = new FakeWsFactory();
        HtxLinearSwapStreamClient client =
                new HtxLinearSwapStreamClient(StreamClientConfig.withDefaults(fact));
        AtomicReference<FundingRate> ref = new AtomicReference<>();
        client.subscribeFundingRate(BTC_USDT_PERP, ref::set);
        client.connect();

        fact.instances.get(0).deliver(loadFixture("htx-funding-rate.json"));
        assertThat(ref.get()).isNotNull();
        assertThat(ref.get().rate()).isEqualByComparingTo("0.00009876");
        assertThat(ref.get().intervalHours()).isEqualTo(8);
    }

    @Test
    void notifyPingSwallowed() throws Exception {
        FakeWsFactory fact = new FakeWsFactory();
        HtxLinearSwapStreamClient client =
                new HtxLinearSwapStreamClient(StreamClientConfig.withDefaults(fact));
        AtomicReference<FundingRate> ref = new AtomicReference<>();
        client.subscribeFundingRate(BTC_USDT_PERP, ref::set);
        client.connect();

        fact.instances.get(0).deliver("{\"op\":\"ping\",\"ts\":1713715200000}");
        assertThat(ref.get()).isNull();
    }

    @Test
    void connectWithoutSubsFails() {
        HtxLinearSwapStreamClient client =
                new HtxLinearSwapStreamClient(StreamClientConfig.withDefaults(FakeWs::new));
        assertThatExceptionOfType(IllegalStateException.class).isThrownBy(client::connect);
    }

    private static String loadFixture(String name) throws Exception {
        try (InputStream in = HtxLinearSwapStreamClientTest.class.getResourceAsStream("/" + name)) {
            Objects.requireNonNull(in, name);
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static final class FakeWsFactory implements java.util.function.Supplier<WebSocketClient> {
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
