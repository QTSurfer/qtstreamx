package com.qtsurfer.qtstreamx.exchange.binance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.qtsurfer.qtstreamx.core.client.StreamClientConfig;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.ws.WebSocketClient;
import java.lang.reflect.Field;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link BinanceStreamClient} registers the correct combined-stream names for
 * each subscription type. {@code subscribeTicker} must land on the 24hr {@code @ticker} stream
 * (the wire-level contract our downstream consumers rely on); {@code @bookTicker} is exposed
 * separately as {@link BinanceStreamClient#subscribeBookTicker}.
 */
class BinanceStreamClientTest {

    private static final Instrument BTC_USDT_SPOT = new Instrument("BTC", "USDT", null);
    private static final Instrument BTC_USDT_PERP = new Instrument("BTC", "USDT", "USDT");

    @Test
    void subscribeTickerRegistersTicker24hStream() {
        BinanceStreamClient client = BinanceStreamClient.spot(fakeConfig());
        client.subscribeTicker(BTC_USDT_SPOT, t -> {});

        assertThat(pendingStreams(client)).containsExactly("btcusdt@ticker");
    }

    @Test
    void subscribeBookTickerRegistersBookTickerStream() {
        BinanceStreamClient client = BinanceStreamClient.spot(fakeConfig());
        client.subscribeBookTicker(BTC_USDT_SPOT, t -> {});

        assertThat(pendingStreams(client)).containsExactly("btcusdt@bookTicker");
    }

    @Test
    void subscribeTickerAndBookTickerCoexist() {
        BinanceStreamClient client = BinanceStreamClient.spot(fakeConfig());
        client.subscribeTicker(BTC_USDT_SPOT, t -> {});
        client.subscribeBookTicker(BTC_USDT_SPOT, t -> {});

        assertThat(pendingStreams(client))
                .containsExactly("btcusdt@ticker", "btcusdt@bookTicker");
    }

    @Test
    void subscribeKlineRegistersKlineStream() {
        BinanceStreamClient client = BinanceStreamClient.spot(fakeConfig());
        client.subscribeKline(BTC_USDT_SPOT, "1m", k -> {});

        assertThat(pendingStreams(client)).containsExactly("btcusdt@kline_1m");
    }

    @Test
    void subscribeFundingRateRegistersMarkPriceStream() {
        BinanceStreamClient client = BinanceStreamClient.futures(fakeConfig());
        client.subscribeFundingRate(BTC_USDT_PERP, f -> {});

        assertThat(pendingStreams(client)).containsExactly("btcusdt@markPrice@1s");
    }

    @Test
    void subscribeFundingRateOnSpotIsRejected() {
        BinanceStreamClient client = BinanceStreamClient.spot(fakeConfig());
        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> client.subscribeFundingRate(BTC_USDT_PERP, f -> {}));
    }

    /* ------------------------------------------------------------- */

    private static StreamClientConfig fakeConfig() {
        return StreamClientConfig.withDefaults(NoopWebSocketClient::new);
    }

    @SuppressWarnings("unchecked")
    private static java.util.List<String> pendingStreams(BinanceStreamClient client) {
        try {
            Field f = BinanceStreamClient.class.getDeclaredField("pendingStreams");
            f.setAccessible(true);
            return (java.util.List<String>) f.get(client);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    /** Minimal WS client — never connects; satisfies {@link StreamClientConfig} for unit tests. */
    private static final class NoopWebSocketClient implements WebSocketClient {
        @Override public void connect(String url) { /* no-op */ }
        @Override public void send(String message) { /* no-op */ }
        @Override public void onMessage(Consumer<String> handler) { /* no-op */ }
        @Override public void onClose(BiConsumer<Integer, String> handler) { /* no-op */ }
        @Override public void onError(Consumer<Throwable> handler) { /* no-op */ }
        @Override public boolean isOpen() { return false; }
        @Override public void close() { /* no-op */ }
    }
}
