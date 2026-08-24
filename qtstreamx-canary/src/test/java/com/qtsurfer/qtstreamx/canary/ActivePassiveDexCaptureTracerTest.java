package com.qtsurfer.qtstreamx.canary;

import com.qtsurfer.qtstreamx.evm.rpc.ActivePassiveEvmLogStream;
import com.qtsurfer.qtstreamx.evm.rpc.EvmProviderBundle;

import com.qtsurfer.qtstreamx.evm.rpc.FileEvmLogCheckpointStore;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.qtsurfer.qtstreamx.aggregation.CandleInterval;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.dex.core.EvmToken;
import com.qtsurfer.qtstreamx.dex.uniswap.v2.UniswapV2MarketDataStream;
import com.qtsurfer.qtstreamx.dex.uniswap.v2.UniswapV2Pair;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLog;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogAcknowledgement;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogBatch;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogBatchHandler;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogCheckpoint;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogCheckpointStore;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogFilter;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogStream;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogStreamId;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogStreamStatus;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRecoveryPolicy;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRecoveryTransitionException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ActivePassiveDexCaptureTracerTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final EvmLogStreamId STREAM_ID =
            new EvmLogStreamId("eip155:1", "uniswap-v2-tracer");
    private static final String PAIR = "0x00000000000000000000000000000000000000ab";
    private static final String SWAP_TOPIC =
            "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822";

    @TempDir
    Path directory;

    @Test
    void resumesOnPassiveWithSameCheckpointAndExactTradeIds() throws Exception {
        Path checkpoints = directory.resolve("checkpoints");
        Path artifacts = directory.resolve("artifacts");
        FileEvmLogCheckpointStore store = new FileEvmLogCheckpointStore(checkpoints);
        List<DurableFixtureStream> created = new ArrayList<>();
        ActivePassiveEvmLogStream supervisor = new ActivePassiveEvmLogStream(
                List.of(
                        bundle("active"),
                        bundle("passive")),
                STREAM_ID,
                store,
                2,
                100,
                (bundle, checkpointStore, policy) -> {
                    long block = bundle.upstreamId().equals("active") ? 100 : 101;
                    DurableFixtureStream stream = new DurableFixtureStream(
                            checkpointStore, policy, swapLog(block));
                    created.add(stream);
                    return stream;
                });
        UniswapV2Pair pair = new UniswapV2Pair(
                "eip155:1",
                PAIR,
                new EvmToken("USDC", "0x0000000000000000000000000000000000000001", 6),
                new EvmToken("WETH", "0x0000000000000000000000000000000000000002", 18),
                new Instrument("WETH", "USDC"));

        try (DexCaptureSession session = new DexCaptureSession(
                new UniswapV2MarketDataStream(supervisor, List.of(pair)),
                CandleInterval.ONE_MINUTE,
                artifacts)) {
            session.startRecoverable();
            created.getFirst().failUpstream();
        }

        assertThat(readEventIds(artifacts.resolve("trades.ndjson")))
                .containsExactly(
                        "eip155:1:0xblock100:0xtx100:1",
                        "eip155:1:0xblock101:0xtx101:1");
        assertThat(store.load(STREAM_ID))
                .contains(new EvmLogCheckpoint(STREAM_ID, 101, "0xblock101"));
        assertThat(created).hasSize(2);
        assertThat(created.get(1).restoredThrough).isEqualTo(100);
        assertThat(supervisor.metrics().switches()).isEqualTo(1);
    }

    private static EvmProviderBundle bundle(String upstreamId) {
        return new EvmProviderBundle(
                upstreamId,
                "https://rpc.invalid/secret-" + upstreamId,
                "wss://rpc.invalid/secret-" + upstreamId,
                ActivePassiveEvmLogStreamTest.liveCapabilities(upstreamId));
    }

    private static List<String> readEventIds(Path path) throws Exception {
        try (var lines = Files.lines(path)) {
            return lines.map(line -> {
                try {
                    JsonNode trade = MAPPER.readTree(line);
                    return trade.path("eventId").asText();
                } catch (Exception exception) {
                    throw new IllegalStateException(exception);
                }
            }).toList();
        }
    }

    private static EvmLog swapLog(long block) {
        String data = "0x"
                + word(new BigInteger("2000000000"))
                + word(BigInteger.ZERO)
                + word(BigInteger.ZERO)
                + word(new BigInteger("1000000000000000000"));
        return new EvmLog(
                "eip155:1",
                PAIR,
                List.of(SWAP_TOPIC, "0xsender", "0xto"),
                data,
                block,
                "0xblock" + block,
                "0xtx" + block,
                0,
                1,
                block * 1_000_000L);
    }

    private static String word(BigInteger value) {
        return "%064x".formatted(value);
    }

    private static final class DurableFixtureStream implements EvmLogStream {
        private final EvmLogCheckpointStore store;
        private final EvmRecoveryPolicy policy;
        private final EvmLog log;
        private Consumer<Throwable> errorHandler = ignored -> {};
        private EvmLogStreamStatus status = EvmLogStreamStatus.IDLE;
        private long restoredThrough = -1;

        private DurableFixtureStream(
                EvmLogCheckpointStore store,
                EvmRecoveryPolicy policy,
                EvmLog log) {
            this.store = store;
            this.policy = policy;
            this.log = log;
        }

        @Override
        public void start(EvmLogFilter filter, Consumer<EvmLog> handler) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void startRecoverable(EvmLogFilter filter, EvmLogBatchHandler handler) throws Exception {
            restoredThrough = store.load(policy.streamId())
                    .map(EvmLogCheckpoint::blockNumber)
                    .orElse(-1L);
            EvmLogAcknowledgement acknowledgement = handler.handle(new EvmLogBatch(
                    policy.streamId(),
                    log.blockNumber(),
                    log.blockNumber(),
                    log.blockHash(),
                    List.of(log)));
            if (acknowledgement == EvmLogAcknowledgement.ACKNOWLEDGED) {
                store.save(new EvmLogCheckpoint(
                        policy.streamId(), log.blockNumber(), log.blockHash()));
            }
            status = EvmLogStreamStatus.LIVE;
        }

        @Override
        public void onError(Consumer<Throwable> handler) {
            errorHandler = handler;
        }

        @Override
        public boolean isConnected() {
            return status == EvmLogStreamStatus.LIVE;
        }

        @Override
        public EvmLogStreamStatus status() {
            return status;
        }

        @Override
        public void close() {
            status = EvmLogStreamStatus.CLOSED;
        }

        private void failUpstream() {
            status = EvmLogStreamStatus.FAILED;
            errorHandler.accept(new EvmRecoveryTransitionException(
                    EvmRecoveryTransitionException.Stage.UPSTREAM_RECOVERY,
                    policy.streamId(),
                    policy.upstreamId()));
        }
    }
}
