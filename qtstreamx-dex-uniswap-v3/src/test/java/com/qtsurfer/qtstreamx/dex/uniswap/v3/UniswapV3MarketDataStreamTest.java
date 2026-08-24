package com.qtsurfer.qtstreamx.dex.uniswap.v3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.qtsurfer.qtstreamx.core.client.MarketTradeAcknowledgement;
import com.qtsurfer.qtstreamx.core.client.MarketTradeBatch;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.model.MarketTrade;
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

class UniswapV3MarketDataStreamTest {

    @Test
    void isolatesMalformedPoolEventAndContinues() throws Exception {
        RecordingLogStream source = new RecordingLogStream();
        UniswapV3Pool pool = new UniswapV3Pool(
                "eip155:1", "0x00000000000000000000000000000000000000ab",
                new EvmToken("BASE", "0x0000000000000000000000000000000000000001", 6),
                new EvmToken("QUOTE", "0x0000000000000000000000000000000000000002", 18),
                new Instrument("BASE", "QUOTE"), 3_000);
        List<MarketTrade> trades = new ArrayList<>();
        List<Throwable> errors = new ArrayList<>();

        try (UniswapV3MarketDataStream stream = new UniswapV3MarketDataStream(source, List.of(pool))) {
            stream.onError(errors::add);
            stream.start(trades::add);
            source.emit(new EvmLog(
                    "eip155:1", pool.address(),
                    List.of(UniswapV3SwapDecoder.SWAP_TOPIC, "0xsender", "0xrecipient"),
                    "0x1234", 100, "0xblock", "0xbad", 0, 6, 1L));
            source.emit(UniswapV3SwapDecoderTest.swapLog(
                    BigInteger.valueOf(1_000_000),
                    BigInteger.valueOf(-1_000_000),
                    BigInteger.ONE.shiftLeft(96)));
        }

        assertThat(errors).hasSize(1);
        assertThat(trades).hasSize(1);
        assertThat(source.filter.addresses()).containsExactly(pool.address());
        assertThat(source.filter.eventTopics()).containsExactly(UniswapV3SwapDecoder.SWAP_TOPIC);
    }

    @Test
    void propagatesDownstreamConsumerFailures() throws Exception {
        RecordingLogStream source = new RecordingLogStream();
        UniswapV3Pool pool = new UniswapV3Pool(
                "eip155:1", "0x00000000000000000000000000000000000000ab",
                new EvmToken("BASE", "0x0000000000000000000000000000000000000001", 6),
                new EvmToken("QUOTE", "0x0000000000000000000000000000000000000002", 18),
                new Instrument("BASE", "QUOTE"), 3_000);

        try (UniswapV3MarketDataStream stream = new UniswapV3MarketDataStream(source, List.of(pool))) {
            stream.start(ignored -> {
                throw new IllegalStateException("downstream unavailable");
            });

            assertThatThrownBy(() -> source.emit(UniswapV3SwapDecoderTest.swapLog(
                            BigInteger.valueOf(1_000_000),
                            BigInteger.valueOf(-1_000_000),
                            BigInteger.ONE.shiftLeft(96))))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("downstream unavailable");
        }
    }

    @Test
    void acknowledgesRawCursorOnlyAfterNormalizedTradeBatchIsAccepted() throws Exception {
        RecordingLogStream source = new RecordingLogStream();
        UniswapV3Pool pool = new UniswapV3Pool(
                "eip155:1", "0x00000000000000000000000000000000000000ab",
                new EvmToken("BASE", "0x0000000000000000000000000000000000000001", 6),
                new EvmToken("QUOTE", "0x0000000000000000000000000000000000000002", 18),
                new Instrument("BASE", "QUOTE"), 3_000);
        List<MarketTradeBatch> batches = new ArrayList<>();
        UniswapV3MarketDataStream stream = new UniswapV3MarketDataStream(source, List.of(pool));
        stream.startRecoverable(batch -> {
            batches.add(batch);
            return MarketTradeAcknowledgement.ACKNOWLEDGED;
        });

        EvmLogAcknowledgement acknowledgement = source.emitBatch(UniswapV3SwapDecoderTest.swapLog(
                BigInteger.valueOf(1_000_000),
                BigInteger.valueOf(-1_000_000),
                BigInteger.ONE.shiftLeft(96)));

        assertThat(acknowledgement).isEqualTo(EvmLogAcknowledgement.ACKNOWLEDGED);
        assertThat(batches).singleElement().satisfies(batch ->
                assertThat(batch.trades()).singleElement().satisfies(trade -> {
                    assertThat(trade.eventId()).isEqualTo("eip155:1:0xblock:0xtx:7");
                    assertThat(trade.market().nativeId()).isEqualTo(pool.address());
                }));
    }

    @Test
    void rejectsRawCursorWhenNormalizedBatchIsRejected() throws Exception {
        RecordingLogStream source = new RecordingLogStream();
        UniswapV3Pool pool = pool();
        UniswapV3MarketDataStream stream = new UniswapV3MarketDataStream(source, List.of(pool));
        stream.startRecoverable(ignored -> MarketTradeAcknowledgement.REJECTED);

        EvmLogAcknowledgement acknowledgement = source.emitBatch(UniswapV3SwapDecoderTest.swapLog(
                BigInteger.valueOf(1_000_000),
                BigInteger.valueOf(-1_000_000),
                BigInteger.ONE.shiftLeft(96)));

        assertThat(acknowledgement).isEqualTo(EvmLogAcknowledgement.REJECTED);
    }

    @Test
    void failsClosedOnMalformedRecoverableBatch() throws Exception {
        RecordingLogStream source = new RecordingLogStream();
        UniswapV3Pool pool = pool();
        List<Throwable> errors = new ArrayList<>();
        List<MarketTradeBatch> batches = new ArrayList<>();
        UniswapV3MarketDataStream stream = new UniswapV3MarketDataStream(source, List.of(pool));
        stream.onError(errors::add);
        stream.startRecoverable(batch -> {
            batches.add(batch);
            return MarketTradeAcknowledgement.ACKNOWLEDGED;
        });
        EvmLog malformed = new EvmLog(
                "eip155:1",
                pool.address(),
                List.of(UniswapV3SwapDecoder.SWAP_TOPIC, "0xsender", "0xrecipient"),
                "0x1234",
                100,
                "0xblock",
                "0xbad",
                0,
                6,
                1L);

        assertThatThrownBy(() -> source.emitBatch(malformed))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(errors).hasSize(1);
        assertThat(batches).isEmpty();
    }

    private static UniswapV3Pool pool() {
        return new UniswapV3Pool(
                "eip155:1", "0x00000000000000000000000000000000000000ab",
                new EvmToken("BASE", "0x0000000000000000000000000000000000000001", 6),
                new EvmToken("QUOTE", "0x0000000000000000000000000000000000000002", 18),
                new Instrument("BASE", "QUOTE"), 3_000);
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

        void emit(EvmLog log) {
            handler.accept(log);
        }

        EvmLogAcknowledgement emitBatch(EvmLog log) throws Exception {
            return batchHandler.handle(new EvmLogBatch(
                    new EvmLogStreamId("eip155:1", "uniswap-v3-base-quote"),
                    log.blockNumber(),
                    log.blockNumber(),
                    log.blockHash(),
                    List.of(log)));
        }
    }
}
