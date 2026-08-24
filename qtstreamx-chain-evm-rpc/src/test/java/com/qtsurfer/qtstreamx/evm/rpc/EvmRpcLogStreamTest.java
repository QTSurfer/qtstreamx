package com.qtsurfer.qtstreamx.evm.rpc;

import static com.qtsurfer.qtstreamx.evm.rpc.JsonRpcTestResponses.error;
import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.core.ws.WebSocketClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class EvmRpcLogStreamTest {

    @Test
    void redactsRpcEndpointsFromConfigurationDescription() {
        EvmLogStreamConfig config = new EvmLogStreamConfig(
                "eip155:1",
                "wss://alice:secret@rpc.example/ws/key-123",
                "https://example.invalid/redacted",
                100,
                2,
                2_000,
                Duration.ofSeconds(5),
                3);

        assertThat(config.toString())
                .contains("network=eip155:1")
                .contains("webSocketUrl=<redacted>")
                .contains("httpUrl=<redacted>")
                .doesNotContain("alice", "secret", "key-123", "rpc.example");
    }

    @Test
    void subscribesWithConfiguredAddressesAndEventTopics() throws Exception {
        RecordingWebSocket webSocket = new RecordingWebSocket();
        EvmRpcHttpClient http = new StubHttpClient(Map.of(
                100L, new EvmBlock(100, "0xblock100", 1_700_000_000_000_000L)));
        EvmLogStreamConfig config = streamConfig();

        try (EvmLogStream stream = new EvmRpcLogStream(config, () -> webSocket, http)) {
            stream.start(
                    new EvmLogFilter(
                            Set.of("0xpool-b", "0xpool-a"),
                            Set.of("0xswap-b", "0xswap-a")),
                    ignored -> {});
        }

        assertThat(webSocket.sentMessages).anySatisfy(message -> assertThat(message)
                .contains("\"params\":[\"logs\"")
                .contains("\"address\":[\"0xpool-a\",\"0xpool-b\"]")
                .contains("\"topics\":[[\"0xswap-a\",\"0xswap-b\"]]"));
    }

    @Test
    void surfacesJsonRpcProtocolError() throws Exception {
        RecordingWebSocket webSocket = new RecordingWebSocket();
        EvmRpcHttpClient http = new StubHttpClient(Map.of(
                100L, new EvmBlock(100, "0xblock100", 1_700_000_000_000_000L)));
        EvmLogStreamConfig config = streamConfig();
        List<Throwable> errors = new ArrayList<>();

        try (EvmLogStream stream = new EvmRpcLogStream(config, () -> webSocket, http)) {
            stream.onError(errors::add);
            stream.start(
                    new EvmLogFilter(Set.of("0xpool"), Set.of("0xswap")),
                    ignored -> {});
            webSocket.emit(error(-32602, "invalid filter"));
        }

        assertThat(errors).singleElement().satisfies(error -> assertThat(error)
                .hasMessageContaining("-32602")
                .hasMessageNotContaining("invalid filter")
                .hasMessageNotContaining("rpc.invalid"));
    }

    @Test
    void releasesLogAfterConfiguredCanonicalDescendants() throws Exception {
        RecordingWebSocket webSocket = new RecordingWebSocket();
        EvmRpcHttpClient http = new StubHttpClient(Map.of(
                100L, new EvmBlock(100, "0xblock100", 1_700_000_000_000_000L)));
        EvmLogStreamConfig config = streamConfig();
        List<EvmLog> emitted = new ArrayList<>();

        try (EvmLogStream stream = new EvmRpcLogStream(config, () -> webSocket, http)) {
            stream.start(
                    new EvmLogFilter(Set.of("0xpool"), Set.of("0xswap")),
                    emitted::add);

            webSocket.emit(logNotification(100, "0xblock100", "0xtx", 3, 7));
            webSocket.emit(headNotification(101, "0xblock101"));
            assertThat(emitted).isEmpty();

            webSocket.emit(headNotification(102, "0xblock102"));
        }

        assertThat(emitted).containsExactly(new EvmLog(
                "eip155:1",
                "0xpool",
                List.of("0xswap"),
                "0xdata",
                100,
                "0xblock100",
                "0xtx",
                3,
                7,
                1_700_000_000_000_000L));
    }

    @Test
    void emitsConfirmedLogsInCanonicalChainOrder() throws Exception {
        RecordingWebSocket webSocket = new RecordingWebSocket();
        EvmRpcHttpClient http = new StubHttpClient(Map.of(
                100L, new EvmBlock(100, "0xblock100", 1_700_000_000_000_000L),
                101L, new EvmBlock(101, "0xblock101", 1_700_000_012_000_000L)));
        EvmLogStreamConfig config = streamConfig(1, 2_000);
        List<EvmLog> emitted = new ArrayList<>();

        try (EvmLogStream stream = new EvmRpcLogStream(config, () -> webSocket, http)) {
            stream.start(
                    new EvmLogFilter(Set.of("0xpool"), Set.of("0xswap")),
                    emitted::add);
            webSocket.emit(logNotification(101, "0xblock101", "0xtx101", 0, 4));
            webSocket.emit(logNotification(100, "0xblock100", "0xtx100b", 2, 8));
            webSocket.emit(logNotification(100, "0xblock100", "0xtx100a", 1, 9));
            webSocket.emit(headNotification(102, "0xblock102"));
        }

        assertThat(emitted)
                .extracting(EvmLog::transactionHash)
                .containsExactly("0xtx100a", "0xtx100b", "0xtx101");
    }

    @Test
    void discardsLogWhenCanonicalBlockHashChanged() throws Exception {
        RecordingWebSocket webSocket = new RecordingWebSocket();
        EvmRpcHttpClient http = new StubHttpClient(Map.of(
                100L, new EvmBlock(100, "0xreplacement", 1_700_000_000_000_000L)));
        EvmLogStreamConfig config = streamConfig();
        List<EvmLog> emitted = new ArrayList<>();

        try (EvmLogStream stream = new EvmRpcLogStream(config, () -> webSocket, http)) {
            stream.start(
                    new EvmLogFilter(Set.of("0xpool"), Set.of("0xswap")),
                    emitted::add);
            webSocket.emit(logNotification(100, "0xorphan", "0xtx", 0, 1));
            webSocket.emit(headNotification(102, "0xblock102"));
            assertThat(stream.recoveryMetrics().reorgs()).isEqualTo(1);
        }

        assertThat(emitted).isEmpty();
    }

    @Test
    void emitsRepeatedLiveLogOnlyOnce() throws Exception {
        RecordingWebSocket webSocket = new RecordingWebSocket();
        EvmRpcHttpClient http = new StubHttpClient(Map.of(
                100L, new EvmBlock(100, "0xblock100", 1_700_000_000_000_000L)));
        EvmLogStreamConfig config = streamConfig();
        List<EvmLog> emitted = new ArrayList<>();

        try (EvmLogStream stream = new EvmRpcLogStream(config, () -> webSocket, http)) {
            stream.start(
                    new EvmLogFilter(Set.of("0xpool"), Set.of("0xswap")),
                    emitted::add);

            String notification = logNotification(100, "0xblock100", "0xtx", 3, 7);
            webSocket.emit(notification);
            webSocket.emit(notification);
            webSocket.emit(headNotification(102, "0xblock102"));
            assertThat(stream.recoveryMetrics().duplicateSuppressions()).isEqualTo(1);
        }

        assertThat(emitted).hasSize(1);
    }

    @Test
    void removesOrphanedLogBeforeConfirmation() throws Exception {
        RecordingWebSocket webSocket = new RecordingWebSocket();
        EvmRpcHttpClient http = new StubHttpClient(Map.of(
                100L, new EvmBlock(100, "0xblock100", 1_700_000_000_000_000L)));
        EvmLogStreamConfig config = streamConfig();
        List<EvmLog> emitted = new ArrayList<>();

        try (EvmLogStream stream = new EvmRpcLogStream(config, () -> webSocket, http)) {
            stream.start(
                    new EvmLogFilter(Set.of("0xpool"), Set.of("0xswap")),
                    emitted::add);

            String notification = logNotification(100, "0xblock100", "0xtx", 3, 7);
            webSocket.emit(notification);
            webSocket.emit(notification.replace("\"removed\":false", "\"removed\":true"));
            webSocket.emit(headNotification(102, "0xblock102"));
            assertThat(stream.recoveryMetrics().reorgs()).isEqualTo(1);
        }

        assertThat(emitted).isEmpty();
    }

    @Test
    void catchesUpGapBeforeReplacingDisconnectedWebSocket() throws Exception {
        RecordingWebSocket firstWebSocket = new RecordingWebSocket();
        RecordingWebSocket secondWebSocket = new RecordingWebSocket();
        Deque<RecordingWebSocket> webSockets = new ArrayDeque<>(List.of(firstWebSocket, secondWebSocket));
        RecoveringHttpClient http = new RecoveringHttpClient(Map.of(
                100L, new EvmBlock(100, "0xblock100", 1_700_000_000_000_000L),
                101L, new EvmBlock(101, "0xblock101", 1_700_000_012_000_000L)));
        EvmLogStreamConfig config = streamConfig();
        List<EvmLog> emitted = new ArrayList<>();

        try (EvmLogStream stream = new EvmRpcLogStream(config, webSockets::removeFirst, http)) {
            stream.start(
                    new EvmLogFilter(Set.of("0xpool"), Set.of("0xswap")),
                    emitted::add);
            firstWebSocket.emit(logNotification(100, "0xblock100", "0xtx100", 0, 1));

            http.latestBlock = 103;
            http.logs = List.of(
                    new EvmRpcLog(
                            "0xpool", List.of("0xswap"), "0xdata", 100, "0xblock100",
                            "0xtx100", 0, 1, false),
                    new EvmRpcLog(
                            "0xpool", List.of("0xswap"), "0xdata", 101, "0xblock101",
                            "0xtx101", 0, 2, false));
            firstWebSocket.disconnect();

            assertThat(secondWebSocket.isOpen()).isTrue();
        }

        assertThat(emitted)
                .extracting(EvmLog::transactionHash)
                .containsExactly("0xtx100", "0xtx101");
        assertThat(http.requestedRanges).containsExactly("100-101");
    }

    @Test
    void bisectsCatchUpRangeRejectedByProvider() throws Exception {
        RecordingWebSocket webSocket = new RecordingWebSocket();
        RangeLimitedHttpClient http = new RangeLimitedHttpClient(105, 2);
        EvmLogStreamConfig config = streamConfig(0, 10);

        try (EvmLogStream stream = new EvmRpcLogStream(config, () -> webSocket, http)) {
            stream.start(
                    new EvmLogFilter(Set.of("0xpool"), Set.of("0xswap")),
                    ignored -> {});
            assertThat(stream.recoveryMetrics().recoveryPages()).isEqualTo(7);
            assertThat(stream.recoveryMetrics().cursorLagBlocks()).isZero();
        }

        assertThat(http.requestedRanges).containsExactly(
                "100-105", "100-102", "100-101", "102-102",
                "103-105", "103-104", "105-105");
    }

    @Test
    void retriesFailedReconnectWithBoundedBackoff() throws Exception {
        RecordingWebSocket firstWebSocket = new RecordingWebSocket();
        RecordingWebSocket replacementWebSocket = new RecordingWebSocket();
        Deque<WebSocketClient> webSockets = new ArrayDeque<>(List.of(
                firstWebSocket,
                new FailingWebSocket(),
                replacementWebSocket));
        RecordingReconnectScheduler scheduler = new RecordingReconnectScheduler();
        EvmRpcHttpClient http = new StubHttpClient(Map.of(
                100L, new EvmBlock(100, "0xblock100", 1_700_000_000_000_000L)));
        EvmLogStreamConfig config = streamConfig();
        List<Throwable> errors = new ArrayList<>();

        try (EvmLogStream stream = new EvmRpcLogStream(config, webSockets::removeFirst, http, scheduler)) {
            stream.onError(errors::add);
            stream.start(
                    new EvmLogFilter(Set.of("0xpool"), Set.of("0xswap")),
                    ignored -> {});
            firstWebSocket.disconnect();

            assertThat(replacementWebSocket.isOpen()).isTrue();
            assertThat(stream.recoveryMetrics().retries()).isEqualTo(1);
        }

        assertThat(scheduler.delays).containsExactly(Duration.ofMillis(100));
        assertThat(errors).isEmpty();
    }

    private static String logNotification(
            long blockNumber,
            String blockHash,
            String transactionHash,
            int transactionIndex,
            int logIndex) {
        return """
                {"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xlogs","result":{
                  "address":"0xpool","topics":["0xswap"],"data":"0xdata","removed":false,
                  "blockNumber":"0x%x","blockHash":"%s","transactionHash":"%s",
                  "transactionIndex":"0x%x","logIndex":"0x%x"}}}
                """.formatted(blockNumber, blockHash, transactionHash, transactionIndex, logIndex);
    }

    private static EvmLogStreamConfig streamConfig() {
        return streamConfig(2, 2_000);
    }

    private static EvmLogStreamConfig streamConfig(int confirmationDepth, int maxBlockRange) {
        return new EvmLogStreamConfig(
                "eip155:1",
                "wss://rpc.invalid/ws",
                "https://rpc.invalid/http",
                100,
                confirmationDepth,
                maxBlockRange,
                Duration.ofSeconds(5),
                3);
    }

    private static String headNotification(long blockNumber, String blockHash) {
        return """
                {"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xheads","result":{
                  "number":"0x%x","hash":"%s"}}}
                """.formatted(blockNumber, blockHash);
    }

    private record StubHttpClient(Map<Long, EvmBlock> blocks) implements EvmRpcHttpClient {
        @Override
        public long latestBlockNumber() {
            return blocks.keySet().stream().mapToLong(Long::longValue).max().orElse(0);
        }

        @Override
        public EvmBlock getBlock(long number) {
            return blocks.get(number);
        }

        @Override
        public List<EvmRpcLog> getLogs(EvmLogFilter filter, long fromBlock, long toBlock) {
            return List.of();
        }
    }

    private static final class RecoveringHttpClient implements EvmRpcHttpClient {
        private final Map<Long, EvmBlock> blocks;
        private final List<String> requestedRanges = new ArrayList<>();
        private List<EvmRpcLog> logs = List.of();
        private long latestBlock = 99;

        private RecoveringHttpClient(Map<Long, EvmBlock> blocks) {
            this.blocks = blocks;
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

    private static final class RangeLimitedHttpClient implements EvmRpcHttpClient {
        private final long latestBlock;
        private final int acceptedBlockCount;
        private final List<String> requestedRanges = new ArrayList<>();

        private RangeLimitedHttpClient(long latestBlock, int acceptedBlockCount) {
            this.latestBlock = latestBlock;
            this.acceptedBlockCount = acceptedBlockCount;
        }

        @Override
        public long latestBlockNumber() {
            return latestBlock;
        }

        @Override
        public EvmBlock getBlock(long number) {
            throw new AssertionError("No block lookup expected without logs");
        }

        @Override
        public List<EvmRpcLog> getLogs(EvmLogFilter filter, long fromBlock, long toBlock) {
            requestedRanges.add(fromBlock + "-" + toBlock);
            if (toBlock - fromBlock + 1 > acceptedBlockCount) {
                throw new EvmRpcException(-32005);
            }
            return List.of();
        }
    }

    private static final class RecordingReconnectScheduler implements ReconnectScheduler {
        private final List<Duration> delays = new ArrayList<>();

        @Override
        public void schedule(Runnable task, Duration delay) {
            delays.add(delay);
            task.run();
        }

        @Override
        public void close() {}
    }

    private static final class FailingWebSocket implements WebSocketClient {
        @Override
        public void connect(String url) throws Exception {
            throw new java.io.IOException("temporary connect failure");
        }

        @Override
        public void send(String message) {}

        @Override
        public void onMessage(Consumer<String> handler) {}

        @Override
        public void onClose(BiConsumer<Integer, String> handler) {}

        @Override
        public void onError(Consumer<Throwable> handler) {}

        @Override
        public boolean isOpen() {
            return false;
        }

        @Override
        public void close() {}
    }

    private static final class RecordingWebSocket implements WebSocketClient {
        private final List<String> sentMessages = new ArrayList<>();
        private Consumer<String> messageHandler;
        private BiConsumer<Integer, String> closeHandler;
        private boolean open;

        @Override
        public void connect(String url) {
            open = true;
        }

        @Override
        public void send(String message) {
            sentMessages.add(message);
        }

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

        void emit(String message) {
            messageHandler.accept(message);
        }

        void disconnect() {
            open = false;
            closeHandler.accept(1006, "test disconnect");
        }
    }
}
