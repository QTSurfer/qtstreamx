package com.qtsurfer.qtstreamx.exchange.kraken;

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

class KrakenFuturesStreamClientTest {

    private static final Instrument BTC_USD_PERP = new Instrument("BTC", "USD", "USD");

    @Test
    void subscribeTickerUsesPfProductIdWithXbtRewrite() throws Exception {
        FakeWs ws = new FakeWs();
        KrakenFuturesStreamClient client = new KrakenFuturesStreamClient(fakeConfig(ws));
        client.subscribeTicker(BTC_USD_PERP, t -> {});
        client.connect();

        assertThat(ws.connectedUrl).isEqualTo("wss://futures.kraken.com/ws/v1");
        assertThat(client.queuedProductIds()).containsExactly("PF_XBTUSD");
        assertThat(ws.sentFrames).hasSize(1);
        assertThat(ws.sentFrames.get(0))
                .isEqualTo(
                        "{\"event\":\"subscribe\",\"feed\":\"ticker\",\"product_ids\":[\"PF_XBTUSD\"]}");
    }

    @Test
    void subscribeKlineIsUnsupportedOnFutures() {
        KrakenFuturesStreamClient client = new KrakenFuturesStreamClient(fakeConfig(new FakeWs()));
        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> client.subscribeKline(BTC_USD_PERP, "1", k -> {}));
    }

    @Test
    void subscribeFundingRateRequiresDerivative() {
        KrakenFuturesStreamClient client = new KrakenFuturesStreamClient(fakeConfig(new FakeWs()));
        Instrument spot = new Instrument("BTC", "USD", null);
        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> client.subscribeFundingRate(spot, f -> {}));
    }

    @Test
    void tickerMessageDispatchesToBothHandlers() throws Exception {
        FakeWs ws = new FakeWs();
        KrakenFuturesStreamClient client = new KrakenFuturesStreamClient(fakeConfig(ws));
        AtomicReference<Ticker> tRef = new AtomicReference<>();
        AtomicReference<FundingRate> fRef = new AtomicReference<>();
        client.subscribeTicker(BTC_USD_PERP, tRef::set);
        client.subscribeFundingRate(BTC_USD_PERP, fRef::set);
        client.connect();

        ws.deliver(loadFixture("kraken-ticker-futures.json"));

        assertThat(tRef.get()).isNotNull();
        assertThat(tRef.get().last()).isEqualByComparingTo("65000.5");
        assertThat(fRef.get()).isNotNull();
        assertThat(fRef.get().intervalHours()).isEqualTo(1);
        assertThat(fRef.get().rate()).isEqualByComparingTo("0.00001235");
    }

    @Test
    void unknownFeedIsIgnored() throws Exception {
        FakeWs ws = new FakeWs();
        KrakenFuturesStreamClient client = new KrakenFuturesStreamClient(fakeConfig(ws));
        AtomicReference<Ticker> tRef = new AtomicReference<>();
        client.subscribeTicker(BTC_USD_PERP, tRef::set);
        client.connect();

        ws.deliver("{\"feed\":\"heartbeat\",\"time\":1}");
        ws.deliver("{\"feed\":\"ticker\",\"product_id\":\"PF_ETHUSD\",\"last\":100}");

        assertThat(tRef.get()).isNull();
    }

    /* ------------------------------------------------------------- */

    private static StreamClientConfig fakeConfig(WebSocketClient ws) {
        return StreamClientConfig.withDefaults(() -> ws);
    }

    private static String loadFixture(String name) throws Exception {
        try (InputStream in =
                KrakenFuturesStreamClientTest.class.getResourceAsStream("/" + name)) {
            Objects.requireNonNull(in, name + " missing from classpath");
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
