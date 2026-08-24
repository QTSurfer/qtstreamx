package com.qtsurfer.qtstreamx.dex.uniswap.v2.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.dex.core.EvmToken;
import com.qtsurfer.qtstreamx.dex.uniswap.v2.UniswapV2MarketDataStream;
import com.qtsurfer.qtstreamx.dex.uniswap.v2.UniswapV2Pair;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLog;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogFilter;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogStream;
import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class PublicUniswapV2StreamTest {

    @Test
    void constructsAdapterUsingOnlyPublicTypes() throws Exception {
        UniswapV2Pair pair = new UniswapV2Pair(
                "eip155:1",
                "0x00000000000000000000000000000000000000ab",
                new EvmToken("BASE", "0x0000000000000000000000000000000000000001", 18),
                new EvmToken("QUOTE", "0x0000000000000000000000000000000000000002", 6),
                new Instrument("BASE", "QUOTE"));
        StubLogStream source = new StubLogStream();

        try (UniswapV2MarketDataStream stream =
                new UniswapV2MarketDataStream(source, List.of(pair))) {
            stream.start(ignored -> {});
            assertThat(stream.isConnected()).isTrue();
        }

        assertThat(source.filter.addresses()).containsExactly(pair.address());
    }

    private static final class StubLogStream implements EvmLogStream {
        private EvmLogFilter filter;

        @Override
        public void start(EvmLogFilter filter, Consumer<EvmLog> handler) {
            this.filter = filter;
        }

        @Override
        public void onError(Consumer<Throwable> handler) {}

        @Override
        public boolean isConnected() {
            return true;
        }

        @Override
        public void close() {}
    }
}
