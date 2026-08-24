package com.qtsurfer.qtstreamx.exchange.gateio;

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

class GateioFuturesStreamClientTest {

    private static final Instrument BTC_USDT_PERP = new Instrument("BTC", "USDT", "USDT");

    @Test
    void subscribeTickerUsesFuturesUrlAndContract() throws Exception {
        FakeWs ws = new FakeWs();
        GateioFuturesStreamClient client = new GateioFuturesStreamClient(fakeConfig(ws));
        client.subscribeTicker(BTC_USDT_PERP, t -> {});
        client.connect();
        assertThat(ws.connectedUrl).isEqualTo("wss://fx-ws.gateio.ws/v4/ws/usdt");
        assertThat(ws.sentFrames).hasSize(1);
        assertThat(ws.sentFrames.get(0)).contains("\"BTC_USDT\"").contains("futures.tickers");
    }

    @Test
    void klineIsUnsupportedOnFutures() {
        GateioFuturesStreamClient client = new GateioFuturesStreamClient(fakeConfig(new FakeWs()));
        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> client.subscribeKline(BTC_USDT_PERP, "1m", k -> {}));
    }

    @Test
    void fundingRejectsSpot() {
        GateioFuturesStreamClient client = new GateioFuturesStreamClient(fakeConfig(new FakeWs()));
        Instrument spot = new Instrument("BTC", "USDT", null);
        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> client.subscribeFundingRate(spot, f -> {}));
    }

    @Test
    void tickerMessageDispatchesToBothHandlers() throws Exception {
        FakeWs ws = new FakeWs();
        GateioFuturesStreamClient client = new GateioFuturesStreamClient(fakeConfig(ws));
        AtomicReference<Ticker> tRef = new AtomicReference<>();
        AtomicReference<FundingRate> fRef = new AtomicReference<>();
        client.subscribeTicker(BTC_USDT_PERP, tRef::set);
        client.subscribeFundingRate(BTC_USDT_PERP, fRef::set);
        client.connect();
        ws.deliver(loadFixture("gateio-ticker-futures.json"));

        assertThat(tRef.get()).isNotNull();
        assertThat(tRef.get().last()).isEqualByComparingTo("65010.5");
        assertThat(fRef.get()).isNotNull();
        assertThat(fRef.get().rate()).isEqualByComparingTo("0.00009876");
    }

    private static StreamClientConfig fakeConfig(WebSocketClient ws) {
        return StreamClientConfig.withDefaults(() -> ws);
    }

    private static String loadFixture(String name) throws Exception {
        try (InputStream in =
                GateioFuturesStreamClientTest.class.getResourceAsStream("/" + name)) {
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
