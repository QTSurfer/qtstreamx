package com.qtsurfer.qtstreamx.evm.rpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.qtsurfer.qtstreamx.core.ws.WebSocketClient;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class EvmRpcDurableLogStreamTest {

    private static final EvmLogStreamId STREAM_ID = new EvmLogStreamId("eip155:1", "eth-usdc-weth-v3");
    private static final EvmLogFilter FILTER = new EvmLogFilter(Set.of("0xpool"), Set.of("0xswap"));

    @Test
    void resumesWithOverlapOnAnotherProviderWithoutMissingOrDuplicatingLogs() throws Exception {
        InMemoryCheckpointStore checkpoints = new InMemoryCheckpointStore();
        List<EvmLog> delivered = new ArrayList<>();
        RecoveryHttpClient firstProvider = new RecoveryHttpClient(
                103,
                blocks(100, 102),
                List.of(log(100), log(101)));

        try (EvmRpcLogStream stream = durableStream(firstProvider, checkpoints, "provider-a", 2, 10)) {
            stream.startRecoverable(FILTER, batch -> {
                delivered.addAll(batch.logs());
                return EvmLogAcknowledgement.ACKNOWLEDGED;
            });
        }

        assertThat(checkpoints.load(STREAM_ID)).contains(new EvmLogCheckpoint(STREAM_ID, 101, "0xblock101"));

        RecoveryHttpClient replacementProvider = new RecoveryHttpClient(
                104,
                blocks(100, 102),
                List.of(log(100), log(101), log(102)));
        List<EvmLogBatch> replacementBatches = new ArrayList<>();

        try (EvmRpcLogStream stream = durableStream(
                replacementProvider, checkpoints, "provider-b", 2, 10)) {
            stream.startRecoverable(FILTER, batch -> {
                replacementBatches.add(batch);
                delivered.addAll(batch.logs());
                return EvmLogAcknowledgement.ACKNOWLEDGED;
            });

            assertThat(stream.status()).isEqualTo(EvmLogStreamStatus.LIVE);
        }

        assertThat(firstProvider.requestedRanges).containsExactly("100-101");
        assertThat(replacementProvider.requestedRanges).containsExactly("100-102");
        assertThat(replacementBatches).singleElement().satisfies(batch -> {
            assertThat(batch.streamId()).isEqualTo(STREAM_ID);
            assertThat(batch.fromBlock()).isEqualTo(102);
            assertThat(batch.toBlock()).isEqualTo(102);
            assertThat(batch.logs()).extracting(EvmLog::transactionHash).containsExactly("0xtx102");
        });
        assertThat(delivered)
                .extracting(EvmLog::transactionHash)
                .containsExactly("0xtx100", "0xtx101", "0xtx102");
        assertThat(checkpoints.load(STREAM_ID)).contains(new EvmLogCheckpoint(STREAM_ID, 102, "0xblock102"));
    }

    @Test
    void doesNotAdvanceOrConnectWhenDownstreamRejectsBatch() throws Exception {
        InMemoryCheckpointStore checkpoints = new InMemoryCheckpointStore();
        RecoveryHttpClient http = new RecoveryHttpClient(102, blocks(100, 100), List.of(log(100)));
        RecordingWebSocket webSocket = new RecordingWebSocket();
        List<Throwable> errors = new ArrayList<>();
        EvmRpcLogStream stream = durableStream(http, checkpoints, "provider-a", 0, 10, webSocket);
        stream.onError(errors::add);

        assertThatThrownBy(() -> stream.startRecoverable(
                        FILTER,
                        ignored -> EvmLogAcknowledgement.REJECTED))
                .isInstanceOf(EvmRecoveryTransitionException.class)
                .satisfies(error -> assertThat(((EvmRecoveryTransitionException) error).stage())
                        .isEqualTo(EvmRecoveryTransitionException.Stage.BATCH_DELIVERY));

        assertThat(checkpoints.load(STREAM_ID)).isEmpty();
        assertThat(webSocket.connectCount).isZero();
        assertThat(stream.status()).isEqualTo(EvmLogStreamStatus.FAILED);
        assertThat(errors).singleElement().isInstanceOf(EvmRecoveryTransitionException.class);
        stream.close();
    }

    @Test
    void advancesAcrossConfirmedBlocksWithoutMatchingLogs() throws Exception {
        InMemoryCheckpointStore checkpoints = new InMemoryCheckpointStore();
        RecoveryHttpClient http = new RecoveryHttpClient(102, blocks(100, 100), List.of());
        List<EvmLogBatch> batches = new ArrayList<>();

        try (EvmRpcLogStream stream = durableStream(http, checkpoints, "provider-a", 0, 10)) {
            stream.startRecoverable(FILTER, batch -> {
                batches.add(batch);
                return EvmLogAcknowledgement.ACKNOWLEDGED;
            });
        }

        assertThat(batches).singleElement().satisfies(batch -> {
            assertThat(batch.fromBlock()).isEqualTo(100);
            assertThat(batch.toBlock()).isEqualTo(100);
            assertThat(batch.logs()).isEmpty();
        });
        assertThat(checkpoints.load(STREAM_ID)).contains(new EvmLogCheckpoint(STREAM_ID, 100, "0xblock100"));
    }

    @Test
    void entersGapExhaustedWithoutSkippingOrConnecting() throws Exception {
        InMemoryCheckpointStore checkpoints = new InMemoryCheckpointStore();
        checkpoints.save(new EvmLogCheckpoint(STREAM_ID, 100, "0xblock100"));
        RecoveryHttpClient http = new RecoveryHttpClient(110, blocks(100, 108), List.of());
        RecordingWebSocket webSocket = new RecordingWebSocket();
        EvmRpcLogStream stream = durableStream(http, checkpoints, "provider-a", 2, 5, webSocket);

        assertThatThrownBy(() -> stream.startRecoverable(
                        FILTER,
                        ignored -> EvmLogAcknowledgement.ACKNOWLEDGED))
                .isInstanceOf(EvmRecoveryGapException.class)
                .hasMessageContaining("eth-usdc-weth-v3", "eip155:1", "provider-a")
                .hasMessageNotContaining("rpc.invalid");

        assertThat(stream.status()).isEqualTo(EvmLogStreamStatus.GAP_EXHAUSTED);
        assertThat(stream.recoveryMetrics().gaps()).isEqualTo(1);
        assertThat(stream.recoveryMetrics().terminalFailures()).isEqualTo(1);
        assertThat(http.requestedRanges).isEmpty();
        assertThat(webSocket.connectCount).isZero();
        assertThat(checkpoints.load(STREAM_ID)).contains(new EvmLogCheckpoint(STREAM_ID, 100, "0xblock100"));
        stream.close();
    }

    @Test
    void rejectsDivergentCheckpointWithoutExposingHashesOrEndpoint() throws Exception {
        InMemoryCheckpointStore checkpoints = new InMemoryCheckpointStore();
        checkpoints.save(new EvmLogCheckpoint(STREAM_ID, 100, "0xorphan"));
        RecoveryHttpClient http = new RecoveryHttpClient(102, blocks(100, 100), List.of());
        RecordingWebSocket webSocket = new RecordingWebSocket();
        EvmRpcLogStream stream = durableStream(http, checkpoints, "provider-b", 2, 10, webSocket);

        assertThatThrownBy(() -> stream.startRecoverable(
                        FILTER,
                        ignored -> EvmLogAcknowledgement.ACKNOWLEDGED))
                .isInstanceOf(EvmCheckpointMismatchException.class)
                .hasMessageContaining("eth-usdc-weth-v3", "eip155:1", "provider-b")
                .hasMessageNotContaining("0xorphan")
                .hasMessageNotContaining("0xblock100")
                .hasMessageNotContaining("rpc.invalid");

        assertThat(stream.status()).isEqualTo(EvmLogStreamStatus.FAILED);
        assertThat(http.requestedRanges).isEmpty();
        assertThat(webSocket.connectCount).isZero();
        stream.close();
    }

    @Test
    void rejectsAHeadBehindTheValidatedCheckpointWithoutConnecting() throws Exception {
        InMemoryCheckpointStore checkpoints = new InMemoryCheckpointStore();
        checkpoints.save(new EvmLogCheckpoint(STREAM_ID, 101, "0xblock101"));
        RecoveryHttpClient staleProvider = new RecoveryHttpClient(100, blocks(100, 101), List.of());
        RecordingWebSocket webSocket = new RecordingWebSocket();
        EvmRpcLogStream stream = durableStream(
                staleProvider, checkpoints, "stale-provider", 2, 10, webSocket);

        assertThatThrownBy(() -> stream.startRecoverable(
                        FILTER,
                        ignored -> EvmLogAcknowledgement.ACKNOWLEDGED))
                .isInstanceOf(EvmStaleHeadException.class)
                .hasMessageContaining("stale-provider", "101")
                .hasMessageNotContaining("rpc.invalid");

        assertThat(stream.status()).isEqualTo(EvmLogStreamStatus.FAILED);
        assertThat(staleProvider.requestedRanges).isEmpty();
        assertThat(webSocket.connectCount).isZero();
        assertThat(checkpoints.load(STREAM_ID)).contains(new EvmLogCheckpoint(STREAM_ID, 101, "0xblock101"));
        stream.close();
    }

    @Test
    void failsWhenOverlapLogsDisagreeWithCanonicalBlocks() throws Exception {
        InMemoryCheckpointStore checkpoints = new InMemoryCheckpointStore();
        checkpoints.save(new EvmLogCheckpoint(STREAM_ID, 101, "0xblock101"));
        EvmRpcLog divergentOverlap = new EvmRpcLog(
                "0xpool",
                List.of("0xswap"),
                "0xdata",
                100,
                "0xorphan100",
                "0xtx100",
                0,
                1,
                false);
        RecoveryHttpClient http = new RecoveryHttpClient(
                103,
                blocks(100, 101),
                List.of(divergentOverlap));
        RecordingWebSocket webSocket = new RecordingWebSocket();
        EvmRpcLogStream stream = durableStream(http, checkpoints, "provider-b", 2, 10, webSocket);

        assertThatThrownBy(() -> stream.startRecoverable(
                        FILTER,
                        ignored -> EvmLogAcknowledgement.ACKNOWLEDGED))
                .isInstanceOf(EvmRecoveryTransitionException.class)
                .satisfies(error -> assertThat(((EvmRecoveryTransitionException) error).stage())
                        .isEqualTo(EvmRecoveryTransitionException.Stage.OVERLAP_VALIDATION))
                .hasMessageNotContaining("0xorphan100");

        assertThat(stream.status()).isEqualTo(EvmLogStreamStatus.FAILED);
        assertThat(webSocket.connectCount).isZero();
        assertThat(checkpoints.load(STREAM_ID)).contains(new EvmLogCheckpoint(STREAM_ID, 101, "0xblock101"));
        stream.close();
    }

    @Test
    void keepsCursorUnchangedWhenCheckpointSaveFails() throws Exception {
        InMemoryCheckpointStore checkpoints = new InMemoryCheckpointStore();
        checkpoints.failSaves = true;
        RecoveryHttpClient http = new RecoveryHttpClient(102, blocks(100, 100), List.of(log(100)));
        RecordingWebSocket webSocket = new RecordingWebSocket();
        EvmRpcLogStream stream = durableStream(http, checkpoints, "provider-a", 0, 10, webSocket);

        assertThatThrownBy(() -> stream.startRecoverable(
                        FILTER,
                        ignored -> EvmLogAcknowledgement.ACKNOWLEDGED))
                .isInstanceOf(EvmRecoveryTransitionException.class)
                .satisfies(error -> assertThat(((EvmRecoveryTransitionException) error).stage())
                        .isEqualTo(EvmRecoveryTransitionException.Stage.CHECKPOINT_SAVE))
                .hasMessageNotContaining("store-secret");

        assertThat(checkpoints.load(STREAM_ID)).isEmpty();
        assertThat(webSocket.connectCount).isZero();
        assertThat(stream.status()).isEqualTo(EvmLogStreamStatus.FAILED);
        stream.close();
    }

    @Test
    void treatsRejectedReconnectBatchAsTerminalInsteadOfRetrying() throws Exception {
        InMemoryCheckpointStore checkpoints = new InMemoryCheckpointStore();
        RecoveryHttpClient http = new RecoveryHttpClient(99, blocks(100, 100), List.of());
        RecordingWebSocket firstWebSocket = new RecordingWebSocket();
        RecordingWebSocket replacementWebSocket = new RecordingWebSocket();
        Deque<RecordingWebSocket> webSockets = new ArrayDeque<>(List.of(
                firstWebSocket,
                replacementWebSocket));
        EvmRpcLogStream stream = new EvmRpcLogStream(
                streamConfig(),
                webSockets::removeFirst,
                http,
                ReconnectScheduler.immediate(),
                checkpoints,
                new EvmRecoveryPolicy(STREAM_ID, "provider-a", 0, 10));

        stream.startRecoverable(FILTER, ignored -> EvmLogAcknowledgement.REJECTED);
        http.latestBlock = 102;
        http.logs = List.of(log(100));
        firstWebSocket.disconnect();

        assertThat(stream.status()).isEqualTo(EvmLogStreamStatus.FAILED);
        assertThat(checkpoints.load(STREAM_ID)).isEmpty();
        assertThat(replacementWebSocket.connectCount).isZero();
        stream.close();
    }

    @Test
    void serializesConcurrentHeadsAtOneAcknowledgementBoundary() throws Exception {
        InMemoryCheckpointStore checkpoints = new InMemoryCheckpointStore();
        RecoveryHttpClient http = new RecoveryHttpClient(99, blocks(100, 100), List.of());
        RecordingWebSocket webSocket = new RecordingWebSocket();
        EvmRpcLogStream stream = durableStream(http, checkpoints, "provider-a", 0, 10, webSocket);
        AtomicInteger deliveries = new AtomicInteger();
        CountDownLatch handlerEntered = new CountDownLatch(1);
        CountDownLatch allowAcknowledgement = new CountDownLatch(1);

        stream.startRecoverable(FILTER, ignored -> {
            deliveries.incrementAndGet();
            handlerEntered.countDown();
            allowAcknowledgement.await();
            return EvmLogAcknowledgement.ACKNOWLEDGED;
        });
        webSocket.emit(logNotification(100));

        Thread firstHead = Thread.startVirtualThread(() -> webSocket.emit(headNotification(102)));
        handlerEntered.await();
        Thread secondHead = Thread.startVirtualThread(() -> webSocket.emit(headNotification(102)));
        allowAcknowledgement.countDown();
        firstHead.join();
        secondHead.join();

        assertThat(deliveries).hasValue(1);
        assertThat(checkpoints.saveCount).isEqualTo(1);
        assertThat(checkpoints.load(STREAM_ID)).contains(new EvmLogCheckpoint(STREAM_ID, 100, "0xblock100"));
        assertThat(stream.status()).isEqualTo(EvmLogStreamStatus.LIVE);
        stream.close();
    }

    @Test
    void reportsConcurrentTerminalWebSocketFailuresOnlyOnce() throws Exception {
        InMemoryCheckpointStore checkpoints = new InMemoryCheckpointStore();
        RecoveryHttpClient http = new RecoveryHttpClient(99, blocks(100, 100), List.of());
        RecordingWebSocket webSocket = new RecordingWebSocket();
        EvmRpcLogStream stream = durableStream(http, checkpoints, "provider-a", 0, 10, webSocket);
        List<Throwable> errors = java.util.Collections.synchronizedList(new ArrayList<>());
        stream.onError(errors::add);
        stream.startRecoverable(FILTER, ignored -> EvmLogAcknowledgement.ACKNOWLEDGED);

        Thread firstFailure = Thread.startVirtualThread(() -> webSocket.emit("not-json"));
        Thread secondFailure = Thread.startVirtualThread(() -> webSocket.emit("also-not-json"));
        firstFailure.join();
        secondFailure.join();

        assertThat(errors).singleElement().isInstanceOf(EvmRecoveryTransitionException.class);
        assertThat(stream.status()).isEqualTo(EvmLogStreamStatus.FAILED);
        assertThat(webSocket.isOpen()).isFalse();
        stream.close();
    }

    private static EvmRpcLogStream durableStream(
            RecoveryHttpClient http,
            EvmLogCheckpointStore checkpoints,
            String upstreamId,
            int overlapBlocks,
            long maxReplayBlocks) {
        return durableStream(
                http,
                checkpoints,
                upstreamId,
                overlapBlocks,
                maxReplayBlocks,
                new RecordingWebSocket());
    }

    private static EvmRpcLogStream durableStream(
            RecoveryHttpClient http,
            EvmLogCheckpointStore checkpoints,
            String upstreamId,
            int overlapBlocks,
            long maxReplayBlocks,
            RecordingWebSocket webSocket) {
        return new EvmRpcLogStream(
                streamConfig(),
                () -> webSocket,
                http,
                ReconnectScheduler.immediate(),
                checkpoints,
                new EvmRecoveryPolicy(STREAM_ID, upstreamId, overlapBlocks, maxReplayBlocks));
    }

    private static EvmLogStreamConfig streamConfig() {
        return new EvmLogStreamConfig(
                "eip155:1",
                "wss://rpc.invalid/ws",
                "https://rpc.invalid/http",
                100,
                2,
                2_000,
                Duration.ofSeconds(5),
                3);
    }

    private static Map<Long, EvmBlock> blocks(long fromBlock, long toBlock) {
        Map<Long, EvmBlock> blocks = new HashMap<>();
        for (long blockNumber = fromBlock; blockNumber <= toBlock; blockNumber++) {
            blocks.put(blockNumber, new EvmBlock(
                    blockNumber,
                    "0xblock" + blockNumber,
                    1_700_000_000_000_000L + blockNumber));
        }
        return blocks;
    }

    private static EvmRpcLog log(long blockNumber) {
        return new EvmRpcLog(
                "0xpool",
                List.of("0xswap"),
                "0xdata",
                blockNumber,
                "0xblock" + blockNumber,
                "0xtx" + blockNumber,
                0,
                1,
                false);
    }

    private static String logNotification(long blockNumber) {
        return """
                {"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xlogs","result":{
                  "address":"0xpool","topics":["0xswap"],"data":"0xdata","removed":false,
                  "blockNumber":"0x%x","blockHash":"0xblock%d","transactionHash":"0xtx%d",
                  "transactionIndex":"0x0","logIndex":"0x1"}}}
                """.formatted(blockNumber, blockNumber, blockNumber);
    }

    private static String headNotification(long blockNumber) {
        return """
                {"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xheads","result":{
                  "number":"0x%x","hash":"0xblock%d"}}}
                """.formatted(blockNumber, blockNumber);
    }

    private static final class InMemoryCheckpointStore implements EvmLogCheckpointStore {
        private final Map<EvmLogStreamId, EvmLogCheckpoint> checkpoints = new HashMap<>();
        private int saveCount;
        private boolean failSaves;

        @Override
        public Optional<EvmLogCheckpoint> load(EvmLogStreamId streamId) {
            return Optional.ofNullable(checkpoints.get(streamId));
        }

        @Override
        public void save(EvmLogCheckpoint checkpoint) throws Exception {
            if (failSaves) {
                throw new java.io.IOException("store-secret");
            }
            saveCount++;
            checkpoints.put(checkpoint.streamId(), checkpoint);
        }
    }

    private static final class RecoveryHttpClient implements EvmRpcHttpClient {
        private long latestBlock;
        private final Map<Long, EvmBlock> blocks;
        private List<EvmRpcLog> logs;
        private final List<String> requestedRanges = new ArrayList<>();

        private RecoveryHttpClient(
                long latestBlock,
                Map<Long, EvmBlock> blocks,
                List<EvmRpcLog> logs) {
            this.latestBlock = latestBlock;
            this.blocks = blocks;
            this.logs = List.copyOf(logs);
        }

        @Override
        public long latestBlockNumber() {
            return latestBlock;
        }

        @Override
        public EvmBlock getBlock(long number) {
            return blocks.get(number);
        }

        @Override
        public List<EvmRpcLog> getLogs(EvmLogFilter filter, long fromBlock, long toBlock) {
            requestedRanges.add(fromBlock + "-" + toBlock);
            return logs.stream()
                    .filter(log -> log.blockNumber() >= fromBlock && log.blockNumber() <= toBlock)
                    .toList();
        }
    }

    private static final class RecordingWebSocket implements WebSocketClient {
        private int connectCount;
        private boolean open;
        private Consumer<String> messageHandler;
        private BiConsumer<Integer, String> closeHandler;

        @Override
        public void connect(String url) {
            connectCount++;
            open = true;
        }

        @Override
        public void send(String message) {}

        @Override
        public void onMessage(Consumer<String> handler) {
            messageHandler = handler;
        }

        @Override
        public void onClose(BiConsumer<Integer, String> handler) {
            closeHandler = handler;
        }

        @Override
        public void onError(Consumer<Throwable> handler) {}

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() {
            open = false;
        }

        private void emit(String message) {
            messageHandler.accept(message);
        }

        private void disconnect() {
            open = false;
            closeHandler.accept(1006, "test disconnect");
        }
    }
}
