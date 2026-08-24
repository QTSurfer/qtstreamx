package com.qtsurfer.qtstreamx.exchange.bitget;

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

class BitgetStreamClientTest {

    private static final Instrument BTC_USDT_SPOT = new Instrument("BTC", "USDT", null);
    private static final Instrument BTC_USDT_PERP = new Instrument("BTC", "USDT", "USDT");

    @Test
    void spotSubscribeUsesSpotInstType() throws Exception {
        FakeWs ws = new FakeWs();
        BitgetStreamClient client = BitgetStreamClient.spot(fakeConfig(ws));
        client.subscribeTicker(BTC_USDT_SPOT, t -> {});
        client.connect();

        assertThat(client.pendingArgs())
                .containsExactly(new BitgetStreamClient.SubscribeArg("SPOT", "ticker", "BTCUSDT"));
        assertThat(ws.sentFrames).hasSize(1);
        assertThat(ws.sentFrames.get(0)).contains("\"instType\":\"SPOT\"");
    }

    @Test
    void futuresTickerPlusFundingShareOneSubscribe() throws Exception {
        FakeWs ws = new FakeWs();
        BitgetStreamClient client = BitgetStreamClient.usdtFutures(fakeConfig(ws));
        client.subscribeTicker(BTC_USDT_PERP, t -> {});
        client.subscribeFundingRate(BTC_USDT_PERP, f -> {});
        client.connect();

        assertThat(client.pendingArgs())
                .containsExactly(
                        new BitgetStreamClient.SubscribeArg(
                                "USDT-FUTURES", "ticker", "BTCUSDT"));
    }

    @Test
    void spotRejectsFunding() {
        BitgetStreamClient client = BitgetStreamClient.spot(fakeConfig(new FakeWs()));
        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> client.subscribeFundingRate(BTC_USDT_PERP, f -> {}));
    }

    @Test
    void futuresTickerMessageDispatchesToBothHandlers() throws Exception {
        FakeWs ws = new FakeWs();
        BitgetStreamClient client = BitgetStreamClient.usdtFutures(fakeConfig(ws));
        AtomicReference<Ticker> tRef = new AtomicReference<>();
        AtomicReference<FundingRate> fRef = new AtomicReference<>();
        client.subscribeTicker(BTC_USDT_PERP, tRef::set);
        client.subscribeFundingRate(BTC_USDT_PERP, fRef::set);
        client.connect();

        ws.deliver(loadFixture("bitget-ticker-futures.json"));
        assertThat(tRef.get()).isNotNull();
        assertThat(fRef.get()).isNotNull();
        assertThat(fRef.get().rate()).isEqualByComparingTo("0.00009876");
    }

    @Test
    void klineMessageDispatches() throws Exception {
        FakeWs ws = new FakeWs();
        BitgetStreamClient client = BitgetStreamClient.spot(fakeConfig(ws));
        AtomicReference<com.qtsurfer.qtstreamx.core.model.Kline> ref = new AtomicReference<>();
        client.subscribeKline(BTC_USDT_SPOT, "1m", ref::set);
        client.connect();

        ws.deliver(loadFixture("bitget-candle.json"));
        assertThat(ref.get()).isNotNull();
        assertThat(ref.get().close()).isEqualByComparingTo("65040.0");
    }

    @Test
    void argsChunkedAt30PerFrame() throws Exception {
        FakeWs ws = new FakeWs();
        BitgetStreamClient client = BitgetStreamClient.spot(fakeConfig(ws));
        for (int i = 0; i < 75; i++) {
            client.subscribeTicker(new Instrument("T" + i, "USDT", null), t -> {});
        }
        client.connect();
        // 75 → 3 frames (30 + 30 + 15)
        assertThat(ws.sentFrames).hasSize(3);
    }

    private static StreamClientConfig fakeConfig(WebSocketClient ws) {
        return StreamClientConfig.withDefaults(() -> ws);
    }

    private static String loadFixture(String name) throws Exception {
        try (InputStream in =
                BitgetStreamClientTest.class.getResourceAsStream("/" + name)) {
            Objects.requireNonNull(in, name);
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
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
