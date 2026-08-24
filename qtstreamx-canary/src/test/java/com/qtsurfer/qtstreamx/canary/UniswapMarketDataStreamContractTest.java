package com.qtsurfer.qtstreamx.canary;

import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.core.client.MarketTradeStream;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.model.MarketTrade;
import com.qtsurfer.qtstreamx.dex.core.EvmToken;
import com.qtsurfer.qtstreamx.dex.uniswap.v2.UniswapV2MarketDataStream;
import com.qtsurfer.qtstreamx.dex.uniswap.v2.UniswapV2Pair;
import com.qtsurfer.qtstreamx.dex.uniswap.v3.UniswapV3MarketDataStream;
import com.qtsurfer.qtstreamx.dex.uniswap.v3.UniswapV3Pool;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLog;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogFilter;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class UniswapMarketDataStreamContractTest {

    @Test
    void startsBothVersionsThroughOneMarketTradeLifecycle() throws Exception {
        RecordingLogStream v2Source = new RecordingLogStream();
        RecordingLogStream v3Source = new RecordingLogStream();
        MarketTradeStream v2 = new UniswapV2MarketDataStream(v2Source, List.of(v2Pair()));
        MarketTradeStream v3 = new UniswapV3MarketDataStream(v3Source, List.of(v3Pool()));

        try (v2; v3) {
            for (MarketTradeStream stream : List.of(v2, v3)) {
                stream.onError(ignored -> {});
                stream.start(ignored -> {});
                assertThat(stream.isConnected()).isTrue();
            }
        }

        assertThat(v2Source.filter.addresses()).hasSize(1);
        assertThat(v3Source.filter.addresses()).hasSize(1);
        assertThat(v2Source.closed).isTrue();
        assertThat(v3Source.closed).isTrue();
    }

    @Test
    void keepsSameInstrumentVersionsDistinctAndIsolatesMalformedEvents() throws Exception {
        RecordingLogStream v2Source = new RecordingLogStream();
        RecordingLogStream v3Source = new RecordingLogStream();
        MarketTradeStream v2 = new UniswapV2MarketDataStream(v2Source, List.of(v2Pair()));
        MarketTradeStream v3 = new UniswapV3MarketDataStream(v3Source, List.of(v3Pool()));
        List<MarketTrade> trades = new ArrayList<>();
        List<Throwable> v2Errors = new ArrayList<>();
        List<Throwable> v3Errors = new ArrayList<>();

        try (v2; v3) {
            v2.onError(v2Errors::add);
            v3.onError(v3Errors::add);
            v2.start(trades::add);
            v3.start(trades::add);
            v2Source.emit(v2Swap(BigInteger.ONE, BigInteger.ONE, BigInteger.ONE, BigInteger.ZERO));
            v2Source.emit(v2Swap(
                    new BigInteger("1000000000000000000"),
                    BigInteger.ZERO,
                    BigInteger.ZERO,
                    new BigInteger("2000000000")));
            v3Source.emit(v3Swap());
        }

        assertThat(v2Errors).hasSize(1);
        assertThat(v3Errors).isEmpty();
        assertThat(trades).extracting(trade -> trade.market().venue())
                .containsExactly("uniswap-v2", "uniswap-v3");
        assertThat(trades).extracting(trade -> trade.market().instrument())
                .containsOnly(new Instrument("BASE", "QUOTE"));
        assertThat(trades).extracting(trade -> trade.market().nativeId())
                .containsExactly(
                        "0x00000000000000000000000000000000000000a2",
                        "0x00000000000000000000000000000000000000a3");
    }

    private static EvmLog v2Swap(
            BigInteger amount0In,
            BigInteger amount1In,
            BigInteger amount0Out,
            BigInteger amount1Out) {
        return new EvmLog(
                "eip155:1",
                "0x00000000000000000000000000000000000000a2",
                List.of(
                        "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822",
                        "0xsender",
                        "0xto"),
                "0x" + word(amount0In) + word(amount1In) + word(amount0Out) + word(amount1Out),
                100,
                "0xblock-v2",
                "0xtx-v2",
                0,
                1,
                120_000_001L);
    }

    private static EvmLog v3Swap() {
        return new EvmLog(
                "eip155:1",
                "0x00000000000000000000000000000000000000a3",
                List.of(
                        "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
                        "0xsender",
                        "0xrecipient"),
                "0x"
                        + signedWord(BigInteger.ONE.negate().multiply(BigInteger.TEN.pow(18)))
                        + signedWord(BigInteger.valueOf(2_000_000_000L))
                        + word(BigInteger.ONE.shiftLeft(96))
                        + word(BigInteger.ONE)
                        + signedWord(BigInteger.ZERO),
                101,
                "0xblock-v3",
                "0xtx-v3",
                0,
                2,
                120_000_002L);
    }

    private static String signedWord(BigInteger value) {
        BigInteger encoded = value.signum() < 0
                ? value.add(BigInteger.ONE.shiftLeft(256))
                : value;
        return word(encoded);
    }

    private static String word(BigInteger value) {
        return "%064x".formatted(value);
    }

    private static UniswapV2Pair v2Pair() {
        return new UniswapV2Pair(
                "eip155:1",
                "0x00000000000000000000000000000000000000a2",
                new EvmToken(
                        "BASE", "0x0000000000000000000000000000000000000001", 18),
                new EvmToken(
                        "QUOTE", "0x0000000000000000000000000000000000000002", 6),
                new Instrument("BASE", "QUOTE"));
    }

    private static UniswapV3Pool v3Pool() {
        return new UniswapV3Pool(
                "eip155:1",
                "0x00000000000000000000000000000000000000a3",
                new EvmToken(
                        "BASE", "0x0000000000000000000000000000000000000001", 18),
                new EvmToken(
                        "QUOTE", "0x0000000000000000000000000000000000000002", 6),
                new Instrument("BASE", "QUOTE"),
                3_000);
    }

    private static final class RecordingLogStream implements EvmLogStream {
        private EvmLogFilter filter;
        private Consumer<EvmLog> handler;
        private boolean closed;

        @Override
        public void start(EvmLogFilter filter, Consumer<EvmLog> handler) {
            this.filter = filter;
            this.handler = handler;
        }

        @Override
        public void onError(Consumer<Throwable> handler) {}

        @Override
        public boolean isConnected() {
            return true;
        }

        @Override
        public void close() {
            closed = true;
        }

        private void emit(EvmLog log) {
            handler.accept(log);
        }
    }
}
