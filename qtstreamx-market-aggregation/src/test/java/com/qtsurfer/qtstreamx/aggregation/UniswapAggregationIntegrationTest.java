package com.qtsurfer.qtstreamx.aggregation;

import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.model.MarketKline;
import com.qtsurfer.qtstreamx.core.model.MarketTicker;
import com.qtsurfer.qtstreamx.dex.core.EvmToken;
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

class UniswapAggregationIntegrationTest {

    private static final String SWAP_TOPIC =
            "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";
    private static final String POOL_ADDRESS =
            "0x00000000000000000000000000000000000000ab";

    @Test
    void aggregatesTradeProducedByUniswapAdapter() throws Exception {
        RecordingLogStream source = new RecordingLogStream();
        UniswapV3Pool pool = new UniswapV3Pool(
                "eip155:1",
                POOL_ADDRESS,
                new EvmToken(
                        "BASE", "0x0000000000000000000000000000000000000001", 6),
                new EvmToken(
                        "QUOTE", "0x0000000000000000000000000000000000000002", 18),
                new Instrument("BASE", "QUOTE"),
                3_000);
        List<MarketTicker> tickers = new ArrayList<>();
        List<MarketKline> klines = new ArrayList<>();
        MarketDataAggregator aggregator = new MarketDataAggregator(
                CandleInterval.ONE_MINUTE, tickers::add, klines::add);

        try (UniswapV3MarketDataStream stream =
                new UniswapV3MarketDataStream(source, List.of(pool))) {
            stream.start(aggregator::accept);
            source.emit(swapLog());
        }

        assertThat(tickers).singleElement().satisfies(ticker -> {
            assertThat(ticker.market().nativeId()).isEqualTo(POOL_ADDRESS);
            assertThat(ticker.ticker().last()).isEqualByComparingTo("0.000000000001");
        });
        assertThat(klines).singleElement().satisfies(kline -> {
            assertThat(kline.kline().volume()).isEqualByComparingTo("1");
            assertThat(kline.kline().quoteVolume()).isEqualByComparingTo("0.000000000001");
            assertThat(kline.kline().numberOfTrades()).isEqualTo(1);
        });
    }

    private static EvmLog swapLog() {
        String data = "0x"
                + signedWord(BigInteger.valueOf(1_000_000))
                + signedWord(BigInteger.valueOf(-1_000_000))
                + unsignedWord(BigInteger.ONE.shiftLeft(96))
                + unsignedWord(BigInteger.ONE)
                + signedWord(BigInteger.ZERO);
        return new EvmLog(
                "eip155:1",
                POOL_ADDRESS,
                List.of(SWAP_TOPIC, "0xsender", "0xrecipient"),
                data,
                100,
                "0xblock",
                "0xtx",
                0,
                7,
                120_000_001L);
    }

    private static String signedWord(BigInteger value) {
        BigInteger encoded = value.signum() < 0
                ? value.add(BigInteger.ONE.shiftLeft(256))
                : value;
        return unsignedWord(encoded);
    }

    private static String unsignedWord(BigInteger value) {
        return "%064x".formatted(value);
    }

    private static final class RecordingLogStream implements EvmLogStream {
        private Consumer<EvmLog> handler;

        @Override
        public void start(EvmLogFilter filter, Consumer<EvmLog> handler) {
            this.handler = handler;
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
    }
}
