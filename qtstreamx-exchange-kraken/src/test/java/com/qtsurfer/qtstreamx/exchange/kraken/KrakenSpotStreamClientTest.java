package com.qtsurfer.qtstreamx.exchange.kraken;

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

class KrakenSpotStreamClientTest {

    private static final Instrument BTC_USD = new Instrument("BTC", "USD", null);
    private static final Instrument ETH_USD = new Instrument("ETH", "USD", null);

    @Test
    void subscribeTickerBatchesSymbolsIntoOneFrame() throws Exception {
        FakeWs ws = new FakeWs();
        KrakenSpotStreamClient client = new KrakenSpotStreamClient(fakeConfig(ws));
        client.subscribeTicker(BTC_USD, t -> {});
        client.subscribeTicker(ETH_USD, t -> {});
        client.connect();

        assertThat(ws.connectedUrl).isEqualTo("wss://ws.kraken.com/v2");
        assertThat(ws.sentFrames).hasSize(1);
        assertThat(ws.sentFrames.get(0))
                .isEqualTo(
                        "{\"method\":\"subscribe\",\"params\":{\"channel\":\"ticker\",\"symbol\":[\"BTC/USD\",\"ETH/USD\"]}}");
    }

    @Test
    void subscribeKlineSendsIntervalAlongsideSymbols() throws Exception {
        FakeWs ws = new FakeWs();
        KrakenSpotStreamClient client = new KrakenSpotStreamClient(fakeConfig(ws));
        client.subscribeKline(BTC_USD, "1m", k -> {});
        client.connect();

        assertThat(ws.sentFrames).hasSize(1);
        assertThat(ws.sentFrames.get(0))
                .isEqualTo(
                        "{\"method\":\"subscribe\",\"params\":{\"channel\":\"ohlc\",\"symbol\":[\"BTC/USD\"],\"interval\":1}}");
    }

    @Test
    void subscribeFundingRateIsRejectedOnSpot() {
        KrakenSpotStreamClient client = new KrakenSpotStreamClient(fakeConfig(new FakeWs()));
        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> client.subscribeFundingRate(BTC_USD, f -> {}));
    }

    @Test
    void tickerMessageDispatchesByInstrument() throws Exception {
        FakeWs ws = new FakeWs();
        KrakenSpotStreamClient client = new KrakenSpotStreamClient(fakeConfig(ws));
        AtomicReference<Ticker> ref = new AtomicReference<>();
        client.subscribeTicker(BTC_USD, ref::set);
        client.connect();

        ws.deliver(loadFixture("kraken-ticker-spot.json"));
        assertThat(ref.get()).isNotNull();
        assertThat(ref.get().last()).isEqualByComparingTo("65000.5");
    }

    @Test
    void ohlcMessageDispatchesWhenIntervalMatches() throws Exception {
        FakeWs ws = new FakeWs();
        KrakenSpotStreamClient client = new KrakenSpotStreamClient(fakeConfig(ws));
        AtomicReference<Kline> ref = new AtomicReference<>();
        client.subscribeKline(BTC_USD, "1", ref::set);
        client.connect();

        ws.deliver(loadFixture("kraken-ohlc-spot.json"));
        assertThat(ref.get()).isNotNull();
        assertThat(ref.get().close()).isEqualByComparingTo("65010.25");
    }

    @Test
    void messageWithUnknownChannelIsIgnored() throws Exception {
        FakeWs ws = new FakeWs();
        KrakenSpotStreamClient client = new KrakenSpotStreamClient(fakeConfig(ws));
        AtomicReference<Ticker> ref = new AtomicReference<>();
        client.subscribeTicker(BTC_USD, ref::set);
        client.connect();

        ws.deliver(
                "{\"channel\":\"trade\",\"data\":[{\"symbol\":\"BTC/USD\"}]}");
        assertThat(ref.get()).isNull();
    }

    /* ------------------------------------------------------------- */

    private static StreamClientConfig fakeConfig(WebSocketClient ws) {
        return StreamClientConfig.withDefaults(() -> ws);
    }

    private static String loadFixture(String name) throws Exception {
        try (InputStream in =
                KrakenSpotStreamClientTest.class.getResourceAsStream("/" + name)) {
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
