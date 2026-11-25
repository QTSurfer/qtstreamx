package com.qtsurfer.qtstreamx.exchange.bybit;

import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.core.client.StreamClientConfig;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.ws.WebSocketClient;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

/**
 * The two halves of the 2026-11-24 fix. Bybit names its USDC-settled linear perpetuals
 * {@code <BASE>PERP}, so the base+quote topic {@code tickers.BTCUSDC} does not exist; and Bybit
 * rejects the WHOLE subscribe frame that carries an unknown topic, so the nine other topics of that
 * frame went silent with it. Topics now use the listed symbol, and a rejected frame is re-sent
 * without the topic Bybit named.
 */
class BybitListedSymbolsAndRejectedFramesTest {

    private static final String LINEAR_PAGE =
            "{\"retCode\":0,\"result\":{\"category\":\"linear\",\"nextPageCursor\":\"\",\"list\":["
                    // the dated future comes FIRST: it must not give its symbol to the perpetual
                    + "{\"symbol\":\"ETHUSDT-04DEC26\",\"contractType\":\"LinearFutures\",\"status\":\"Trading\","
                    + "\"baseCoin\":\"ETH\",\"quoteCoin\":\"USDT\",\"settleCoin\":\"USDT\"},"
                    + "{\"symbol\":\"ETHUSDT\",\"contractType\":\"LinearPerpetual\",\"status\":\"Trading\","
                    + "\"baseCoin\":\"ETH\",\"quoteCoin\":\"USDT\",\"settleCoin\":\"USDT\",\"fundingInterval\":480},"
                    + "{\"symbol\":\"SOLPERP\",\"contractType\":\"LinearPerpetual\",\"status\":\"Trading\","
                    + "\"baseCoin\":\"SOL\",\"quoteCoin\":\"USDC\",\"settleCoin\":\"USDC\",\"fundingInterval\":480},"
                    + "{\"symbol\":\"ETHUSDT-25DEC26\",\"contractType\":\"LinearFutures\",\"status\":\"Trading\","
                    + "\"baseCoin\":\"ETH\",\"quoteCoin\":\"USDT\",\"settleCoin\":\"USDT\"}"
                    + "]}}";

    private static final Instrument SOL_USDC_PERP = new Instrument("SOL", "USDC", "USDC");
    private static final Instrument ETH_USDT_PERP = new Instrument("ETH", "USDT", "USDT");

    @Test
    void usdcPerpetualUsesItsListedSymbolAndAFutureNeverRenamesThePerpetual() {
        BybitInstrumentsCache.ParsedList parsed =
                BybitInstrumentsCache.parse(LINEAR_PAGE, BybitInstrumentsCache.Category.LINEAR);

        assertThat(parsed.instruments()).contains(SOL_USDC_PERP, ETH_USDT_PERP);
        assertThat(BybitSymbols.toStream(SOL_USDC_PERP)).isEqualTo("SOLPERP");
        assertThat(BybitSymbols.toStream(ETH_USDT_PERP)).isEqualTo("ETHUSDT");
    }

    @Test
    void linearClientSubscribesTheListedTopicForAUsdcPerpetual() {
        BybitInstrumentsCache.parse(LINEAR_PAGE, BybitInstrumentsCache.Category.LINEAR);
        BybitStreamClient client = BybitStreamClient.linear(config(new FakeWs()));

        client.subscribeTicker(SOL_USDC_PERP, t -> {});

        assertThat(client.pendingTopics()).containsExactly("tickers.SOLPERP");
    }

    @Test
    void spotPairWithTheSameCoinsKeepsBasePlusQuote() {
        BybitInstrumentsCache.parse(LINEAR_PAGE, BybitInstrumentsCache.Category.LINEAR);

        // spot BTC/USDC has no settle coin: a different instrument from the USDC perpetual
        assertThat(BybitSymbols.toStream(new Instrument("SOL", "USDC", null))).isEqualTo("SOLUSDC");
    }

    @Test
    void rejectedFrameIsResentWithoutTheNamedTopicAndOtherFramesAreLeftAlone() throws Exception {
        FakeWs ws = new FakeWs();
        BybitStreamClient client = connectedLinear(ws, 12); // frames: T0..T9, T10..T11
        assertThat(ws.sent).hasSize(2);

        ws.deliver(rejection("tickers.T3USDT"));

        assertThat(ws.sent).hasSize(3);
        String resent = ws.sent.get(2);
        assertThat(resent).doesNotContain("T3USDT");
        for (int i = 0; i < 10; i++) {
            if (i != 3) assertThat(resent).contains("\"tickers.T" + i + "USDT\"");
        }
        assertThat(resent).doesNotContain("T10USDT").doesNotContain("T11USDT");
        assertThat(client.rejectedTopics()).containsExactly("tickers.T3USDT");
    }

    @Test
    void aSecondRejectionOfTheResentFrameDropsThatTopicToo() throws Exception {
        FakeWs ws = new FakeWs();
        connectedLinear(ws, 10);

        ws.deliver(rejection("tickers.T3USDT"));
        ws.deliver(rejection("tickers.T7USDT"));

        assertThat(ws.sent).hasSize(3);
        assertThat(ws.sent.get(2)).doesNotContain("T3USDT").doesNotContain("T7USDT").contains("T0USDT").contains("T9USDT");
    }

    @Test
    void aFrameLeftEmptyIsNotResent() throws Exception {
        FakeWs ws = new FakeWs();
        connectedLinear(ws, 1);

        ws.deliver(rejection("tickers.T0USDT"));

        assertThat(ws.sent).hasSize(1);
    }

    @Test
    void aRejectionThatNamesNoTopicOrASuccessfulAckSendsNothing() throws Exception {
        FakeWs ws = new FakeWs();
        BybitStreamClient client = connectedLinear(ws, 10);

        ws.deliver("{\"success\":false,\"ret_msg\":\"error:too many requests\",\"conn_id\":\"c\",\"op\":\"subscribe\"}");
        ws.deliver("{\"success\":true,\"ret_msg\":\"\",\"conn_id\":\"c\",\"op\":\"subscribe\"}");
        ws.deliver("{\"success\":true,\"ret_msg\":\"pong\",\"conn_id\":\"c\",\"op\":\"ping\"}");

        assertThat(ws.sent).hasSize(1);
        assertThat(client.rejectedTopics()).isEmpty();
    }

    @Test
    void aRejectionOfATopicNotSentOnThisSocketChangesNothing() throws Exception {
        FakeWs ws = new FakeWs();
        connectedLinear(ws, 10);

        ws.deliver(rejection("tickers.NOTSENTUSDT"));

        assertThat(ws.sent).hasSize(1);
    }

    /* ------------------------------------------------------------- */

    private static BybitStreamClient connectedLinear(FakeWs ws, int topics) throws Exception {
        BybitStreamClient client = BybitStreamClient.linear(config(ws));
        for (int i = 0; i < topics; i++) {
            client.subscribeTicker(new Instrument("T" + i, "USDT", "USDT"), t -> {});
        }
        client.connect();
        return client;
    }

    private static String rejection(String topic) {
        return "{\"success\":false,\"ret_msg\":\"error:handler not found,topic:" + topic
                + "\",\"conn_id\":\"c\",\"op\":\"subscribe\"}";
    }

    private static StreamClientConfig config(WebSocketClient ws) {
        return StreamClientConfig.withDefaults(() -> ws);
    }

    /** Records sent frames and lets a test push inbound frames. */
    private static final class FakeWs implements WebSocketClient {
        Consumer<String> onMessage = s -> {};
        String url;
        final List<String> sent = new ArrayList<>();

        @Override public void connect(String url) { this.url = url; }
        @Override public void send(String message) { sent.add(message); }
        @Override public void onMessage(Consumer<String> handler) { this.onMessage = handler; }
        @Override public void onClose(BiConsumer<Integer, String> handler) {}
        @Override public void onError(Consumer<Throwable> handler) {}
        @Override public boolean isOpen() { return url != null; }
        @Override public void close() {}

        void deliver(String message) { onMessage.accept(message); }
    }
}
