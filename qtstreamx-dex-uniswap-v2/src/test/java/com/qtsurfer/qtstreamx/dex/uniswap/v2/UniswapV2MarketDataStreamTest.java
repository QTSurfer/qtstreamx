package com.qtsurfer.qtstreamx.dex.uniswap.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.qtsurfer.qtstreamx.core.client.MarketTradeAcknowledgement;
import com.qtsurfer.qtstreamx.core.client.MarketTradeBatch;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.model.MarketTrade;
import com.qtsurfer.qtstreamx.core.model.TradeSide;
import com.qtsurfer.qtstreamx.dex.core.EvmToken;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLog;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogAcknowledgement;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogBatch;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogBatchHandler;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogFilter;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogStream;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogStreamId;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class UniswapV2MarketDataStreamTest {

    private static final String PAIR = "0x00000000000000000000000000000000000000ab";
    private static final String SWAP_TOPIC =
            "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822";

    @Test
    void emitsToken1BaseSaleFromCanonicalSwap() throws Exception {
        RecordingLogStream source = new RecordingLogStream();
        UniswapV2Pair pair = pair("WETH", "USDC");
        List<MarketTrade> trades = new ArrayList<>();

        try (UniswapV2MarketDataStream stream =
                new UniswapV2MarketDataStream(source, List.of(pair))) {
            stream.start(trades::add);
            source.emit(swapLog(
                    BigInteger.ZERO,
                    new BigInteger("1000000000000000000"),
                    new BigInteger("2000000000"),
                    BigInteger.ZERO));
        }

        assertThat(source.filter.addresses()).containsExactly(PAIR);
        assertThat(source.filter.eventTopics()).containsExactly(SWAP_TOPIC);
        assertThat(trades).singleElement().satisfies(trade -> {
            assertThat(trade.market().venue()).isEqualTo("uniswap-v2");
            assertThat(trade.market().nativeId()).isEqualTo(PAIR);
            assertThat(trade.market().instrument()).isEqualTo(new Instrument("WETH", "USDC"));
            assertThat(trade.eventId()).isEqualTo("eip155:1:0xblock:0xtx:7");
            assertThat(trade.price()).isEqualByComparingTo("2000");
            assertThat(trade.baseAmount()).isEqualByComparingTo("1");
            assertThat(trade.quoteAmount()).isEqualByComparingTo("2000");
            assertThat(trade.side()).isEqualTo(TradeSide.SELL);
            assertThat(trade.timestamp()).isEqualTo(1_700_000_000_123_456L);
        });
    }

    @Test
    void emitsOppositeDirectionAsToken1BaseBuy() throws Exception {
        RecordingLogStream source = new RecordingLogStream();
        UniswapV2Pair pair = pair("WETH", "USDC");
        List<MarketTrade> trades = new ArrayList<>();

        try (UniswapV2MarketDataStream stream =
                new UniswapV2MarketDataStream(source, List.of(pair))) {
            stream.start(trades::add);
            source.emit(swapLog(
                    new BigInteger("2000000000"),
                    BigInteger.ZERO,
                    BigInteger.ZERO,
                    new BigInteger("1000000000000000000")));
        }

        assertThat(trades).singleElement().satisfies(trade -> {
            assertThat(trade.price()).isEqualByComparingTo("2000");
            assertThat(trade.baseAmount()).isEqualByComparingTo("1");
            assertThat(trade.quoteAmount()).isEqualByComparingTo("2000");
            assertThat(trade.side()).isEqualTo(TradeSide.BUY);
        });
    }

    @Test
    void orientsPriceAndSideWhenToken0IsLogicalBase() throws Exception {
        RecordingLogStream source = new RecordingLogStream();
        UniswapV2Pair pair = pair("USDC", "WETH");
        List<MarketTrade> trades = new ArrayList<>();

        try (UniswapV2MarketDataStream stream =
                new UniswapV2MarketDataStream(source, List.of(pair))) {
            stream.start(trades::add);
            source.emit(swapLog(
                    new BigInteger("2000000000"),
                    BigInteger.ZERO,
                    BigInteger.ZERO,
                    new BigInteger("1000000000000000000")));
        }

        assertThat(trades).singleElement().satisfies(trade -> {
            assertThat(trade.price()).isEqualByComparingTo("0.0005");
            assertThat(trade.baseAmount()).isEqualByComparingTo("2000");
            assertThat(trade.quoteAmount()).isEqualByComparingTo("1");
            assertThat(trade.side()).isEqualTo(TradeSide.SELL);
        });
    }

    @Test
    void isolatesAmbiguousSwapAndContinuesWithValidEvents() throws Exception {
        RecordingLogStream source = new RecordingLogStream();
        UniswapV2Pair pair = pair("WETH", "USDC");
        List<MarketTrade> trades = new ArrayList<>();
        List<Throwable> errors = new ArrayList<>();

        try (UniswapV2MarketDataStream stream =
                new UniswapV2MarketDataStream(source, List.of(pair))) {
            stream.onError(errors::add);
            stream.start(trades::add);
            source.emit(swapLog(
                    BigInteger.ONE,
                    BigInteger.ONE,
                    BigInteger.ONE,
                    BigInteger.ZERO));
            source.emit(swapLog(
                    new BigInteger("2000000000"),
                    BigInteger.ZERO,
                    BigInteger.ZERO,
                    new BigInteger("1000000000000000000")));
        }

        assertThat(errors).singleElement().satisfies(error ->
                assertThat(error).hasMessageContaining("exactly one input"));
        assertThat(trades).hasSize(1);
    }

    @Test
    void keepsSameInstrumentPairsDistinctByNativeIdentity() throws Exception {
        String secondPairAddress = "0x00000000000000000000000000000000000000ac";
        RecordingLogStream source = new RecordingLogStream();
        UniswapV2Pair firstPair = pair(PAIR, "WETH", "USDC");
        UniswapV2Pair secondPair = pair(secondPairAddress, "WETH", "USDC");
        List<MarketTrade> trades = new ArrayList<>();

        try (UniswapV2MarketDataStream stream =
                new UniswapV2MarketDataStream(source, List.of(firstPair, secondPair))) {
            stream.start(trades::add);
            source.emit(swapLog(
                    PAIR,
                    new BigInteger("2000000000"),
                    BigInteger.ZERO,
                    BigInteger.ZERO,
                    new BigInteger("1000000000000000000")));
            source.emit(swapLog(
                    secondPairAddress,
                    new BigInteger("2100000000"),
                    BigInteger.ZERO,
                    BigInteger.ZERO,
                    new BigInteger("1000000000000000000")));
        }

        assertThat(trades).extracting(trade -> trade.market().nativeId())
                .containsExactly(PAIR, secondPairAddress);
        assertThat(trades).extracting(trade -> trade.market().instrument())
                .containsOnly(new Instrument("WETH", "USDC"));
    }

    @Test
    void acknowledgesRawCursorOnlyAfterNormalizedTradeBatchIsAccepted() throws Exception {
        RecordingLogStream source = new RecordingLogStream();
        List<MarketTradeBatch> batches = new ArrayList<>();
        UniswapV2MarketDataStream stream =
                new UniswapV2MarketDataStream(source, List.of(pair("WETH", "USDC")));
        stream.startRecoverable(batch -> {
            batches.add(batch);
            return MarketTradeAcknowledgement.ACKNOWLEDGED;
        });

        EvmLogAcknowledgement acknowledgement = source.emitBatch(swapLog(
                BigInteger.ZERO,
                new BigInteger("1000000000000000000"),
                new BigInteger("2000000000"),
                BigInteger.ZERO));

        assertThat(acknowledgement).isEqualTo(EvmLogAcknowledgement.ACKNOWLEDGED);
        assertThat(batches).singleElement().satisfies(batch ->
                assertThat(batch.trades()).singleElement().satisfies(trade -> {
                    assertThat(trade.eventId()).isEqualTo("eip155:1:0xblock:0xtx:7");
                    assertThat(trade.market().nativeId()).isEqualTo(PAIR);
                }));
    }

    @Test
    void rejectsRawCursorWhenNormalizedBatchIsRejected() throws Exception {
        RecordingLogStream source = new RecordingLogStream();
        UniswapV2MarketDataStream stream =
                new UniswapV2MarketDataStream(source, List.of(pair("WETH", "USDC")));
        stream.startRecoverable(ignored -> MarketTradeAcknowledgement.REJECTED);

        EvmLogAcknowledgement acknowledgement = source.emitBatch(swapLog(
                BigInteger.ZERO,
                new BigInteger("1000000000000000000"),
                new BigInteger("2000000000"),
                BigInteger.ZERO));

        assertThat(acknowledgement).isEqualTo(EvmLogAcknowledgement.REJECTED);
    }

    @Test
    void failsClosedOnMalformedRecoverableBatch() throws Exception {
        RecordingLogStream source = new RecordingLogStream();
        List<Throwable> errors = new ArrayList<>();
        List<MarketTradeBatch> batches = new ArrayList<>();
        UniswapV2MarketDataStream stream =
                new UniswapV2MarketDataStream(source, List.of(pair("WETH", "USDC")));
        stream.onError(errors::add);
        stream.startRecoverable(batch -> {
            batches.add(batch);
            return MarketTradeAcknowledgement.ACKNOWLEDGED;
        });

        EvmLog malformed = swapLog(
                BigInteger.ONE, BigInteger.ONE, BigInteger.ONE, BigInteger.ZERO);

        assertThatThrownBy(() -> source.emitBatch(malformed))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one input");
        assertThat(errors).hasSize(1);
        assertThat(batches).isEmpty();
    }

    private static UniswapV2Pair pair(String base, String quote) {
        return pair(PAIR, base, quote);
    }

    private static UniswapV2Pair pair(String address, String base, String quote) {
        return new UniswapV2Pair(
                "eip155:1",
                address,
                new EvmToken("USDC", "0x0000000000000000000000000000000000000001", 6),
                new EvmToken("WETH", "0x0000000000000000000000000000000000000002", 18),
                new Instrument(base, quote));
    }

    private static EvmLog swapLog(
            BigInteger amount0In,
            BigInteger amount1In,
            BigInteger amount0Out,
            BigInteger amount1Out) {
        return swapLog(PAIR, amount0In, amount1In, amount0Out, amount1Out);
    }

    private static EvmLog swapLog(
            String pairAddress,
            BigInteger amount0In,
            BigInteger amount1In,
            BigInteger amount0Out,
            BigInteger amount1Out) {
        String data = "0x"
                + word(amount0In)
                + word(amount1In)
                + word(amount0Out)
                + word(amount1Out);
        return new EvmLog(
                "eip155:1",
                pairAddress,
                List.of(SWAP_TOPIC, "0xsender", "0xto"),
                data,
                100,
                "0xblock",
                "0xtx",
                3,
                7,
                1_700_000_000_123_456L);
    }

    private static String word(BigInteger value) {
        return "%064x".formatted(value);
    }

    private static final class RecordingLogStream implements EvmLogStream {
        private EvmLogFilter filter;
        private Consumer<EvmLog> handler;
        private EvmLogBatchHandler batchHandler;

        @Override
        public void start(EvmLogFilter filter, Consumer<EvmLog> handler) {
            this.filter = filter;
            this.handler = handler;
        }

        @Override
        public void startRecoverable(EvmLogFilter filter, EvmLogBatchHandler handler) {
            this.filter = filter;
            batchHandler = handler;
        }

        @Override
        public void onError(Consumer<Throwable> handler) {}

        @Override
        public boolean isConnected() {
            return true;
        }

        @Override
        public void close() {}

        private void emit(EvmLog log) {
            handler.accept(log);
        }

        private EvmLogAcknowledgement emitBatch(EvmLog log) throws Exception {
            return batchHandler.handle(new EvmLogBatch(
                    new EvmLogStreamId("eip155:1", "uniswap-v2-usdc-weth"),
                    log.blockNumber(),
                    log.blockNumber(),
                    log.blockHash(),
                    List.of(log)));
        }
    }
}
