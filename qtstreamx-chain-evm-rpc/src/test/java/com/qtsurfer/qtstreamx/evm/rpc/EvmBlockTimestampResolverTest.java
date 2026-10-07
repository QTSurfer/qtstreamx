package com.qtsurfer.qtstreamx.evm.rpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

class EvmBlockTimestampResolverTest {

    private static final long GENESIS_SECOND = 1_600_000_000L;
    private static final long BLOCK_INTERVAL_SECONDS = 12L;

    @Test
    void findsTheFirstBlockAtOrAfterAnInstantWellWithinTheChain() {
        FakeEvmRpcReader reader = new FakeEvmRpcReader(10_000);
        EvmBlockTimestampResolver resolver = new EvmBlockTimestampResolver(reader);

        long target = GENESIS_SECOND + 1_000L * BLOCK_INTERVAL_SECONDS;
        assertThat(resolver.firstBlockAtOrAfter(target)).isEqualTo(1_000L);
    }

    @Test
    void aTargetBetweenTwoBlocksResolvesToTheLaterOne() {
        FakeEvmRpcReader reader = new FakeEvmRpcReader(10_000);
        EvmBlockTimestampResolver resolver = new EvmBlockTimestampResolver(reader);

        long betweenBlock999And1000 = GENESIS_SECOND + 999L * BLOCK_INTERVAL_SECONDS + 1;
        assertThat(resolver.firstBlockAtOrAfter(betweenBlock999And1000)).isEqualTo(1_000L);
    }

    @Test
    void exactBlockTimestampResolvesToThatBlock() {
        FakeEvmRpcReader reader = new FakeEvmRpcReader(10_000);
        EvmBlockTimestampResolver resolver = new EvmBlockTimestampResolver(reader);

        long exact = GENESIS_SECOND + 1_000L * BLOCK_INTERVAL_SECONDS;
        assertThat(resolver.firstBlockAtOrAfter(exact)).isEqualTo(1_000L);
    }

    @Test
    void aTargetBeforeGenesisResolvesToBlockZero() {
        FakeEvmRpcReader reader = new FakeEvmRpcReader(10_000);
        EvmBlockTimestampResolver resolver = new EvmBlockTimestampResolver(reader);

        assertThat(resolver.firstBlockAtOrAfter(GENESIS_SECOND - 1_000)).isEqualTo(0L);
    }

    @Test
    void aTargetAfterTheLatestBlockResolvesToTheLatestBlock() {
        FakeEvmRpcReader reader = new FakeEvmRpcReader(10_000);
        EvmBlockTimestampResolver resolver = new EvmBlockTimestampResolver(reader);

        long farInTheFuture = GENESIS_SECOND + 1_000_000L * BLOCK_INTERVAL_SECONDS;
        assertThat(resolver.firstBlockAtOrAfter(farInTheFuture)).isEqualTo(10_000L);
    }

    @Test
    void rejectsANegativeInput() {
        FakeEvmRpcReader reader = new FakeEvmRpcReader(10_000);
        EvmBlockTimestampResolver resolver = new EvmBlockTimestampResolver(reader);

        assertThatThrownBy(() -> resolver.firstBlockAtOrAfter(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** Only {@code latestBlockNumber} and {@code block} are exercised by this resolver. */
    private static final class FakeEvmRpcReader implements EvmRpcReader {
        private final long latestBlockNumber;

        FakeEvmRpcReader(long latestBlockNumber) {
            this.latestBlockNumber = latestBlockNumber;
        }

        @Override
        public long latestBlockNumber() {
            return latestBlockNumber;
        }

        @Override
        public EvmBlock block(long blockNumber) {
            long second = GENESIS_SECOND + blockNumber * BLOCK_INTERVAL_SECONDS;
            return new EvmBlock(blockNumber, "0xblock" + blockNumber, second * 1_000_000L);
        }

        @Override
        public List<EvmRpcLog> logs(EvmLogFilter filter, long fromBlock, long toBlock) {
            throw new UnsupportedOperationException("not used by this test");
        }

        @Override
        public byte[] call(String contractAddress, byte[] data, EvmBlockTag blockTag) {
            throw new UnsupportedOperationException("not used by this test");
        }
    }
}
