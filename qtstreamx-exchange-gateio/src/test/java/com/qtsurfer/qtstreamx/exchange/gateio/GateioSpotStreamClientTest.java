package com.qtsurfer.qtstreamx.exchange.gateio;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.qtsurfer.qtstreamx.core.client.StreamClientConfig;
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

class GateioSpotStreamClientTest {

    private static final Instrument BTC_USDT = new Instrument("BTC", "USDT", null);

    @Test
    void subscribeTickerQueuesSymbolAndSendsOneFrame() throws Exception {
        FakeWs ws = new FakeWs();
        GateioSpotStreamClient client = new GateioSpotStreamClient(fakeConfig(ws));
        client.subscribeTicker(BTC_USDT, t -> {});
        client.subscribeTicker(new Instrument("ETH", "USDT", null), t -> {});
        client.connect();

        assertThat(ws.connectedUrl).isEqualTo("wss://api.gateio.ws/ws/v4/");
        assertThat(ws.sentFrames).hasSize(1);
        assertThat(ws.sentFrames.get(0))
                .contains("\"channel\":\"spot.tickers\"")
                .contains("\"BTC_USDT\"")
                .contains("\"ETH_USDT\"");
    }

    @Test
    void subscribeFundingRateRejected() {
        GateioSpotStreamClient client = new GateioSpotStreamClient(fakeConfig(new FakeWs()));
        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> client.subscribeFundingRate(BTC_USDT, f -> {}));
    }

    @Test
    void tickerMessageRoutedByCurrencyPair() throws Exception {
        FakeWs ws = new FakeWs();
        GateioSpotStreamClient client = new GateioSpotStreamClient(fakeConfig(ws));
        AtomicReference<Ticker> ref = new AtomicReference<>();
        client.subscribeTicker(BTC_USDT, ref::set);
        client.connect();
        ws.deliver(loadFixture("gateio-ticker-spot.json"));

        assertThat(ref.get()).isNotNull();
        assertThat(ref.get().last()).isEqualByComparingTo("65010.5");
    }

    @Test
    void klineSubscribeSendsOneFramePerPair() throws Exception {
        FakeWs ws = new FakeWs();
        GateioSpotStreamClient client = new GateioSpotStreamClient(fakeConfig(ws));
        client.subscribeKline(BTC_USDT, "1m", k -> {});
        client.subscribeKline(new Instrument("ETH", "USDT", null), "1m", k -> {});
        client.subscribeKline(new Instrument("SOL", "USDT", null), "1m", k -> {});
        client.connect();

        // No tickers → 3 candle frames, one per pair (payload shape [interval, pair]).
        assertThat(ws.sentFrames).hasSize(3);
        for (String frame : ws.sentFrames) {
            assertThat(frame).startsWith("{").endsWith("}").contains("spot.candlesticks");
        }
    }

    @Test
    void klineMessageRoutedByNField() throws Exception {
        FakeWs ws = new FakeWs();
        GateioSpotStreamClient client = new GateioSpotStreamClient(fakeConfig(ws));
        AtomicReference<Kline> ref = new AtomicReference<>();
        client.subscribeKline(BTC_USDT, "1m", ref::set);
        client.connect();
        ws.deliver(loadFixture("gateio-candlestick-spot.json"));

        assertThat(ref.get()).isNotNull();
        assertThat(ref.get().close()).isEqualByComparingTo("65040.0");
    }

    @Test
    void subscribeAckFramesIgnored() throws Exception {
        FakeWs ws = new FakeWs();
        GateioSpotStreamClient client = new GateioSpotStreamClient(fakeConfig(ws));
        AtomicReference<Ticker> ref = new AtomicReference<>();
        client.subscribeTicker(BTC_USDT, ref::set);
        client.connect();

        ws.deliver("{\"channel\":\"spot.tickers\",\"event\":\"subscribe\",\"result\":{\"status\":\"success\"}}");
        assertThat(ref.get()).isNull();
    }

    private static StreamClientConfig fakeConfig(WebSocketClient ws) {
        return StreamClientConfig.withDefaults(() -> ws);
    }

    private static String loadFixture(String name) throws Exception {
        try (InputStream in =
                GateioSpotStreamClientTest.class.getResourceAsStream("/" + name)) {
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
