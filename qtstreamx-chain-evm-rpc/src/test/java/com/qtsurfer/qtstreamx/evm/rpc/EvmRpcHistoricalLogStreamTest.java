package com.qtsurfer.qtstreamx.evm.rpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class EvmRpcHistoricalLogStreamTest {

    private static final EvmLogFilter FILTER = new EvmLogFilter(Set.of("0xpool"), Set.of("0xswap"));

    @Test
    void replaysConfirmedLogsInChainOrderThenCompletes() {
        FakeEvmRpcReader reader = new FakeEvmRpcReader(200);
        reader.block(100, "0xblock100");
        reader.block(101, "0xblock101");
        reader.logAt(101, "0xtx101", 0, 4);
        reader.logAt(100, "0xtx100b", 2, 8);
        reader.logAt(100, "0xtx100a", 1, 9);
        List<EvmLog> emitted = new ArrayList<>();
        EvmRpcHistoricalLogStream stream =
                new EvmRpcHistoricalLogStream(reader, "eip155:1", 100, 101, 2);

        stream.start(FILTER, emitted::add);

        assertThat(emitted)
                .extracting(EvmLog::transactionHash)
                .containsExactly("0xtx100a", "0xtx100b", "0xtx101");
        assertThat(stream.status()).isEqualTo(EvmLogStreamStatus.COMPLETED);
        assertThat(stream.isConnected()).isFalse();
    }

    @Test
    void rejectsRangeThatExtendsPastTheConfirmedSafeHead() {
        FakeEvmRpcReader reader = new FakeEvmRpcReader(101);
        reader.block(100, "0xblock100");
        EvmRpcHistoricalLogStream stream =
                new EvmRpcHistoricalLogStream(reader, "eip155:1", 90, 100, 2);

        assertThatThrownBy(() -> stream.start(FILTER, ignored -> {}))
                .isInstanceOf(EvmHistoricalRangeNotFinalizedException.class)
                .hasMessageContaining("99");
        assertThat(stream.status()).isEqualTo(EvmLogStreamStatus.FAILED);
    }

    @Test
    void dropsLogsFromANonCanonicalBlockHash() {
        FakeEvmRpcReader reader = new FakeEvmRpcReader(200);
        reader.block(100, "0xcanonical");
        reader.logAt(100, "0xstale-fork", "0xother-hash", 0, 0);
        reader.logAt(100, "0xkept", "0xcanonical", 1, 0);
        List<EvmLog> emitted = new ArrayList<>();
        EvmRpcHistoricalLogStream stream =
                new EvmRpcHistoricalLogStream(reader, "eip155:1", 100, 100, 2);

        stream.start(FILTER, emitted::add);

        assertThat(emitted).extracting(EvmLog::transactionHash).containsExactly("0xkept");
    }

    @Test
    void replayingTheSameRangeTwiceProducesIdenticalOutput() {
        List<EvmLog> firstRun = new ArrayList<>();
        List<EvmLog> secondRun = new ArrayList<>();
        for (List<EvmLog> destination : List.of(firstRun, secondRun)) {
            FakeEvmRpcReader reader = new FakeEvmRpcReader(200);
            reader.block(100, "0xblock100");
            reader.block(101, "0xblock101");
            reader.logAt(101, "0xtx101", 0, 4);
            reader.logAt(100, "0xtx100b", 2, 8);
            reader.logAt(100, "0xtx100a", 1, 9);
            new EvmRpcHistoricalLogStream(reader, "eip155:1", 100, 101, 2)
                    .start(FILTER, destination::add);
        }

        assertThat(firstRun).isEqualTo(secondRun);
    }

    @Test
    void rejectsStartingTwice() {
        FakeEvmRpcReader reader = new FakeEvmRpcReader(200);
        reader.block(100, "0xblock100");
        EvmRpcHistoricalLogStream stream =
                new EvmRpcHistoricalLogStream(reader, "eip155:1", 100, 100, 2);
        stream.start(FILTER, ignored -> {});

        assertThatThrownBy(() -> stream.start(FILTER, ignored -> {}))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void rejectsAReversedBlockRange() {
        FakeEvmRpcReader reader = new FakeEvmRpcReader(200);

        assertThatThrownBy(() -> new EvmRpcHistoricalLogStream(reader, "eip155:1", 101, 100, 2))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static final class FakeEvmRpcReader implements EvmRpcReader {
        private final long latestBlockNumber;
        private final Map<Long, EvmBlock> blocks = new HashMap<>();
        private final List<EvmRpcLog> logs = new ArrayList<>();

        FakeEvmRpcReader(long latestBlockNumber) {
            this.latestBlockNumber = latestBlockNumber;
        }

        void block(long number, String hash) {
            blocks.put(number, new EvmBlock(number, hash, number * 12_000_000L));
        }

        void logAt(long blockNumber, String transactionHash, int transactionIndex, int logIndex) {
            logAt(blockNumber, transactionHash, "0xblock" + blockNumber, transactionIndex, logIndex);
        }

        void logAt(
                long blockNumber,
                String transactionHash,
                String blockHash,
                int transactionIndex,
                int logIndex) {
            logs.add(new EvmRpcLog(
                    "0xpool",
                    List.of("0xswap"),
                    "0xdata",
                    blockNumber,
                    blockHash,
                    transactionHash,
                    transactionIndex,
                    logIndex,
                    false));
        }

        @Override
        public long latestBlockNumber() {
            return latestBlockNumber;
        }

        @Override
        public EvmBlock block(long blockNumber) {
            EvmBlock block = blocks.get(blockNumber);
            if (block == null) {
                throw new IllegalStateException("no fake block registered for " + blockNumber);
            }
            return block;
        }

        @Override
        public List<EvmRpcLog> logs(EvmLogFilter filter, long fromBlock, long toBlock) {
            return logs.stream()
                    .filter(log -> log.blockNumber() >= fromBlock && log.blockNumber() <= toBlock)
                    .sorted(java.util.Comparator.comparingLong(EvmRpcLog::blockNumber)
                            .thenComparingInt(EvmRpcLog::transactionIndex)
                            .thenComparingInt(EvmRpcLog::logIndex))
                    .toList();
        }

        @Override
        public byte[] call(String contractAddress, byte[] data, EvmBlockTag blockTag) {
            throw new UnsupportedOperationException();
        }
    }
}
