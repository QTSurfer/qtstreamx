package com.qtsurfer.qtstreamx.evm.rpc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.qtsurfer.qtstreamx.core.ws.WebSocketClient;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** JSON-RPC implementation of a confirmation-gated EVM log stream with optional durable recovery. */
public final class EvmRpcLogStream implements EvmLogStream {
    private static final Duration INITIAL_RECONNECT_DELAY = Duration.ofMillis(100);
    private static final Duration MAX_RECONNECT_DELAY = Duration.ofSeconds(5);

    private static final Comparator<EvmRpcLog> CHAIN_ORDER = Comparator
            .comparingInt(EvmRpcLog::transactionIndex)
            .thenComparingInt(EvmRpcLog::logIndex);

    private final EvmLogStreamConfig config;
    private final Supplier<WebSocketClient> webSocketFactory;
    private final EvmRpcHttpClient httpClient;
    private final ReconnectScheduler reconnectScheduler;
    private final EvmLogCheckpointStore checkpointStore;
    private final EvmRecoveryPolicy recoveryPolicy;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final NavigableMap<Long, List<EvmRpcLog>> pending = new TreeMap<>();
    private final Set<LogKey> seenLogs = new HashSet<>();
    private final AtomicBoolean reconnecting = new AtomicBoolean();

    private WebSocketClient webSocket;
    private EvmLogFilter filter;
    private Consumer<EvmLog> logHandler;
    private EvmLogBatchHandler batchHandler;
    private Consumer<Throwable> errorHandler = ignored -> {};
    private long committedThrough;
    private long cursorLagBlocks;
    private long recoveryPages;
    private long retries;
    private long gaps;
    private long reorgs;
    private long duplicateSuppressions;
    private long terminalFailures;
    private boolean restoredCheckpoint;
    private volatile boolean closed;
    private volatile boolean terminal;
    private volatile EvmLogStreamStatus status = EvmLogStreamStatus.IDLE;

    /**
     * Creates a stream backed by the JDK HTTP client and the supplied WebSocket adapter.
     *
     * @param config runtime stream configuration
     * @param webSocketFactory creates a fresh WebSocket for initial connect and reconnect
     */
    public EvmRpcLogStream(
            EvmLogStreamConfig config,
            Supplier<WebSocketClient> webSocketFactory) {
        this(
                config,
                webSocketFactory,
                new JsonRpcHttpClient(
                        config,
                        new JdkJsonRpcHttpTransport(URI.create(config.httpUrl()))),
                new ScheduledReconnectScheduler(),
                null,
                null);
    }

    /**
     * Creates a durable stream backed by provider-neutral checkpoint persistence.
     *
     * @param config runtime stream configuration
     * @param webSocketFactory creates a fresh WebSocket for initial connect and reconnect
     * @param checkpointStore persists explicitly acknowledged canonical cursors
     * @param recoveryPolicy stable stream identity, current upstream alias, and replay bounds
     */
    public EvmRpcLogStream(
            EvmLogStreamConfig config,
            Supplier<WebSocketClient> webSocketFactory,
            EvmLogCheckpointStore checkpointStore,
            EvmRecoveryPolicy recoveryPolicy) {
        this(
                config,
                webSocketFactory,
                new JsonRpcHttpClient(
                        config,
                        new JdkJsonRpcHttpTransport(URI.create(config.httpUrl()))),
                new ScheduledReconnectScheduler(),
                Objects.requireNonNull(checkpointStore, "checkpointStore"),
                Objects.requireNonNull(recoveryPolicy, "recoveryPolicy"));
    }

    EvmRpcLogStream(
            EvmLogStreamConfig config,
            Supplier<WebSocketClient> webSocketFactory,
            EvmRpcHttpClient httpClient) {
        this(config, webSocketFactory, httpClient, ReconnectScheduler.immediate(), null, null);
    }

    EvmRpcLogStream(
            EvmLogStreamConfig config,
            Supplier<WebSocketClient> webSocketFactory,
            EvmRpcHttpClient httpClient,
            ReconnectScheduler reconnectScheduler) {
        this(config, webSocketFactory, httpClient, reconnectScheduler, null, null);
    }

    EvmRpcLogStream(
            EvmLogStreamConfig config,
            Supplier<WebSocketClient> webSocketFactory,
            EvmRpcHttpClient httpClient,
            ReconnectScheduler reconnectScheduler,
            EvmLogCheckpointStore checkpointStore,
            EvmRecoveryPolicy recoveryPolicy) {
        this.config = Objects.requireNonNull(config, "config");
        this.webSocketFactory = Objects.requireNonNull(webSocketFactory, "webSocketFactory");
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
        this.reconnectScheduler = Objects.requireNonNull(reconnectScheduler, "reconnectScheduler");
        this.checkpointStore = checkpointStore;
        this.recoveryPolicy = recoveryPolicy;
        if ((checkpointStore == null) != (recoveryPolicy == null)) {
            throw new IllegalArgumentException("checkpointStore and recoveryPolicy must be configured together");
        }
        if (recoveryPolicy != null && !config.network().equals(recoveryPolicy.streamId().network())) {
            throw new IllegalArgumentException("recovery stream network must match stream configuration");
        }
        this.committedThrough = config.startBlock() - 1;
    }

    @Override
    public void start(EvmLogFilter filter, Consumer<EvmLog> handler) throws Exception {
        requireStartable();
        if (recoveryPolicy != null) {
            throw new IllegalStateException("Use startRecoverable when durable recovery is configured");
        }
        this.filter = Objects.requireNonNull(filter, "filter");
        logHandler = Objects.requireNonNull(handler, "handler");
        status = EvmLogStreamStatus.RECOVERING;
        try {
            catchUp();
            connectWebSocket();
            status = EvmLogStreamStatus.LIVE;
        } catch (Exception exception) {
            markTerminal(exception);
            throw exception;
        }
    }

    @Override
    public void startRecoverable(EvmLogFilter filter, EvmLogBatchHandler handler) throws Exception {
        requireStartable();
        if (recoveryPolicy == null) {
            throw new IllegalStateException("Durable recovery was not configured");
        }
        this.filter = Objects.requireNonNull(filter, "filter");
        batchHandler = Objects.requireNonNull(handler, "handler");
        status = EvmLogStreamStatus.RECOVERING;
        try {
            restoreCheckpoint();
            catchUp();
            connectWebSocket();
            status = EvmLogStreamStatus.LIVE;
        } catch (Exception exception) {
            RuntimeException safeException = safeRecoveryFailure(exception);
            markTerminal(safeException);
            throw safeException;
        }
    }

    private void requireStartable() {
        if (closed || status != EvmLogStreamStatus.IDLE) {
            throw new IllegalStateException("EVM log stream can only be started once");
        }
    }

    private synchronized void restoreCheckpoint() {
        Optional<EvmLogCheckpoint> stored;
        try {
            stored = checkpointStore.load(recoveryPolicy.streamId());
        } catch (Exception exception) {
            throw transitionFailure(EvmRecoveryTransitionException.Stage.CHECKPOINT_LOAD);
        }
        if (stored.isEmpty()) {
            return;
        }
        EvmLogCheckpoint checkpoint = stored.orElseThrow();
        if (!checkpoint.streamId().equals(recoveryPolicy.streamId())) {
            throw new IllegalStateException("Checkpoint store returned a different stream identity");
        }
        EvmBlock canonicalBlock = requireBlock(checkpoint.blockNumber());
        if (!checkpoint.blockHash().equals(canonicalBlock.hash())) {
            throw new EvmCheckpointMismatchException(
                    recoveryPolicy.streamId(),
                    checkpoint.blockNumber(),
                    recoveryPolicy.upstreamId());
        }
        committedThrough = checkpoint.blockNumber();
        restoredCheckpoint = true;
    }

    private void connectWebSocket() throws Exception {
        webSocket = webSocketFactory.get();
        webSocket.onMessage(this::handleMessage);
        webSocket.onClose((code, reason) -> reconnect());
        webSocket.onError(this::handleTransportError);
        webSocket.connect(config.webSocketUrl());
        webSocket.send(logSubscriptionRequest(1, filter));
        webSocket.send(subscriptionRequest(2, "newHeads"));
    }

    @Override
    public void onError(Consumer<Throwable> handler) {
        errorHandler = Objects.requireNonNull(handler, "handler");
    }

    @Override
    public boolean isConnected() {
        return status == EvmLogStreamStatus.LIVE && webSocket != null && webSocket.isOpen();
    }

    @Override
    public EvmLogStreamStatus status() {
        return status;
    }

    @Override
    public synchronized EvmLogStreamMetrics recoveryMetrics() {
        return new EvmLogStreamMetrics(
                cursorLagBlocks,
                recoveryPages,
                retries,
                gaps,
                reorgs,
                duplicateSuppressions,
                terminalFailures);
    }

    @Override
    public synchronized void close() throws Exception {
        closed = true;
        terminal = true;
        status = EvmLogStreamStatus.CLOSED;
        if (webSocket != null) {
            webSocket.close();
        }
        reconnectScheduler.close();
    }

    private synchronized void reconnect() {
        if (closed || terminal || !reconnecting.compareAndSet(false, true)) {
            return;
        }
        status = EvmLogStreamStatus.RECOVERING;
        reconnectAttempt(0);
    }

    private void reconnectAttempt(int retries) {
        try {
            catchUp();
            connectWebSocket();
            reconnecting.set(false);
            status = EvmLogStreamStatus.LIVE;
        } catch (Exception exception) {
            if (isTerminalRecoveryFailure(exception)) {
                reconnecting.set(false);
                markTerminal(exception);
                return;
            }
            if (retries >= config.maxRetries()) {
                reconnecting.set(false);
                markTerminal(recoveryPolicy == null ? exception : safeRecoveryFailure(exception));
                return;
            }
            recordRetry();
            reconnectScheduler.schedule(
                    () -> reconnectAttempt(retries + 1),
                    reconnectDelay(retries));
        }
    }

    private void handleTransportError(Throwable error) {
        reconnect();
    }

    private static Duration reconnectDelay(int retries) {
        Duration delay = INITIAL_RECONNECT_DELAY.multipliedBy(1L << Math.min(retries, 30));
        return delay.compareTo(MAX_RECONNECT_DELAY) > 0 ? MAX_RECONNECT_DELAY : delay;
    }

    private synchronized void catchUp() throws Exception {
        long latestBlock = httpClient.latestBlockNumber();
        long safeBlock = latestBlock - config.confirmationDepth();
        if (recoveryPolicy != null && restoredCheckpoint && safeBlock < committedThrough) {
            throw new EvmStaleHeadException(
                    recoveryPolicy.streamId(),
                    safeBlock,
                    committedThrough,
                    recoveryPolicy.upstreamId());
        }
        long missingBlocks = Math.max(0, safeBlock - committedThrough);
        cursorLagBlocks = missingBlocks;
        if (recoveryPolicy != null && missingBlocks > recoveryPolicy.maxReplayBlocks()) {
            status = EvmLogStreamStatus.GAP_EXHAUSTED;
            gaps++;
            throw new EvmRecoveryGapException(
                    recoveryPolicy.streamId(),
                    missingBlocks,
                    recoveryPolicy.maxReplayBlocks(),
                    recoveryPolicy.upstreamId());
        }
        long fromBlock = recoveryPolicy == null
                ? Math.max(config.startBlock(), committedThrough + 1)
                : Math.max(
                        config.startBlock(),
                        committedThrough - recoveryPolicy.overlapBlocks() + 1);
        while (fromBlock <= safeBlock) {
            long toBlock = Math.min(safeBlock, fromBlock + config.maxBlockRange() - 1L);
            fetchRange(fromBlock, toBlock);
            fromBlock = toBlock + 1;
        }
        releaseConfirmed(latestBlock);
        cursorLagBlocks = Math.max(0, safeBlock - committedThrough);
    }

    private void fetchRange(long fromBlock, long toBlock) {
        recoveryPages++;
        try {
            httpClient.getLogs(filter, fromBlock, toBlock).forEach(this::bufferLog);
        } catch (EvmRpcException exception) {
            if (fromBlock == toBlock) {
                throw exception;
            }
            long midpoint = fromBlock + (toBlock - fromBlock) / 2;
            fetchRange(fromBlock, midpoint);
            fetchRange(midpoint + 1, toBlock);
        }
    }

    private void handleMessage(String message) {
        if (terminal) {
            return;
        }
        try {
            JsonNode root = objectMapper.readTree(message);
            if (root.hasNonNull("error")) {
                JsonNode error = root.path("error");
                throw new IllegalStateException(
                        "JSON-RPC request failed with code " + error.path("code").asInt());
            }
            JsonNode result = root.path("params").path("result");
            if (result.has("address")) {
                bufferLog(parseLog(result));
            } else if (result.has("number")) {
                releaseConfirmed(parseHexLong(result.path("number").asText()));
            }
        } catch (Exception exception) {
            if (recoveryPolicy != null) {
                markTerminal(safeRecoveryFailure(exception));
            } else {
                errorHandler.accept(exception);
            }
        }
    }

    private synchronized void bufferLog(EvmRpcLog log) {
        LogKey key = LogKey.from(log);
        if (log.removed()) {
            if (seenLogs.contains(key)) {
                reorgs++;
            }
            removePending(log.blockNumber(), key);
        } else if (seenLogs.add(key)) {
            pending.computeIfAbsent(log.blockNumber(), ignored -> new ArrayList<>()).add(log);
        } else {
            duplicateSuppressions++;
        }
    }

    private void removePending(long blockNumber, LogKey key) {
        seenLogs.remove(key);
        List<EvmRpcLog> logs = pending.get(blockNumber);
        if (logs == null) {
            return;
        }
        logs.removeIf(candidate -> LogKey.from(candidate).equals(key));
        if (logs.isEmpty()) {
            pending.remove(blockNumber);
        }
    }

    private synchronized void releaseConfirmed(long headNumber) throws Exception {
        long safeBlock = headNumber - config.confirmationDepth();
        if (recoveryPolicy != null) {
            releaseRecoverable(safeBlock);
            return;
        }
        List<Long> releasable = new ArrayList<>(pending.headMap(safeBlock, true).keySet());
        for (long blockNumber : releasable) {
            EvmBlock canonicalBlock = httpClient.getBlock(blockNumber);
            List<EvmRpcLog> logs = pending.remove(blockNumber);
            List<EvmRpcLog> canonicalLogs = logs.stream()
                    .filter(log -> log.blockHash().equals(canonicalBlock.hash()))
                    .toList();
            reorgs += logs.size() - canonicalLogs.size();
            canonicalLogs.stream()
                    .sorted(CHAIN_ORDER)
                    .map(log -> confirmed(log, canonicalBlock.timestamp()))
                    .forEach(logHandler);
        }
        committedThrough = Math.max(committedThrough, safeBlock);
    }

    private void releaseRecoverable(long safeBlock) throws Exception {
        validateAndDiscardOverlap();
        long fromBlock = committedThrough + 1;
        if (safeBlock < fromBlock) {
            return;
        }

        List<EvmLog> confirmedLogs = new ArrayList<>();
        for (long blockNumber : new ArrayList<>(pending.subMap(fromBlock, true, safeBlock, true).keySet())) {
            EvmBlock canonicalBlock = requireBlock(blockNumber);
            List<EvmRpcLog> logs = pending.get(blockNumber);
            List<EvmRpcLog> canonicalLogs = logs.stream()
                    .filter(log -> log.blockHash().equals(canonicalBlock.hash()))
                    .toList();
            reorgs += logs.size() - canonicalLogs.size();
            canonicalLogs.stream()
                    .sorted(CHAIN_ORDER)
                    .map(log -> confirmed(log, canonicalBlock.timestamp()))
                    .forEach(confirmedLogs::add);
        }

        EvmBlock cursorBlock = requireBlock(safeBlock);
        EvmLogBatch batch = new EvmLogBatch(
                recoveryPolicy.streamId(),
                fromBlock,
                safeBlock,
                cursorBlock.hash(),
                confirmedLogs);
        EvmLogAcknowledgement acknowledgement;
        try {
            acknowledgement = batchHandler.handle(batch);
        } catch (Exception exception) {
            throw transitionFailure(EvmRecoveryTransitionException.Stage.BATCH_DELIVERY);
        }
        if (acknowledgement != EvmLogAcknowledgement.ACKNOWLEDGED) {
            throw transitionFailure(EvmRecoveryTransitionException.Stage.BATCH_DELIVERY);
        }

        try {
            checkpointStore.save(new EvmLogCheckpoint(
                    recoveryPolicy.streamId(),
                    safeBlock,
                    cursorBlock.hash()));
        } catch (Exception exception) {
            throw transitionFailure(EvmRecoveryTransitionException.Stage.CHECKPOINT_SAVE);
        }
        committedThrough = safeBlock;
        discardThrough(safeBlock);
    }

    private void validateAndDiscardOverlap() {
        for (long blockNumber : new ArrayList<>(pending.headMap(committedThrough, true).keySet())) {
            EvmBlock canonicalBlock = requireBlock(blockNumber);
            boolean divergent = pending.get(blockNumber).stream()
                    .anyMatch(log -> !log.blockHash().equals(canonicalBlock.hash()));
            if (divergent) {
                reorgs++;
                throw transitionFailure(EvmRecoveryTransitionException.Stage.OVERLAP_VALIDATION);
            }
        }
        discardThrough(committedThrough);
    }

    private EvmBlock requireBlock(long blockNumber) {
        EvmBlock block = httpClient.getBlock(blockNumber);
        if (block == null) {
            throw new IllegalStateException("Canonical block " + blockNumber + " is unavailable");
        }
        return block;
    }

    private void discardThrough(long blockNumber) {
        pending.headMap(blockNumber, true).clear();
        seenLogs.removeIf(key -> key.blockNumber() <= blockNumber);
    }

    private synchronized void markTerminal(Throwable exception) {
        if (terminal) {
            return;
        }
        terminal = true;
        terminalFailures++;
        if (status != EvmLogStreamStatus.GAP_EXHAUSTED) {
            status = EvmLogStreamStatus.FAILED;
        }
        errorHandler.accept(exception);
        if (webSocket != null && webSocket.isOpen()) {
            try {
                webSocket.close();
            } catch (Exception ignored) {
                // The original endpoint-free terminal error remains authoritative.
            }
        }
    }

    private boolean isTerminalRecoveryFailure(Exception exception) {
        return exception instanceof EvmRecoveryGapException
                || exception instanceof EvmCheckpointMismatchException
                || exception instanceof EvmStaleHeadException
                || exception instanceof EvmRecoveryTransitionException;
    }

    private RuntimeException safeRecoveryFailure(Exception exception) {
        if (isTerminalRecoveryFailure(exception)) {
            return (RuntimeException) exception;
        }
        return transitionFailure(EvmRecoveryTransitionException.Stage.UPSTREAM_RECOVERY);
    }

    private EvmRecoveryTransitionException transitionFailure(
            EvmRecoveryTransitionException.Stage stage) {
        return new EvmRecoveryTransitionException(
                stage,
                recoveryPolicy.streamId(),
                recoveryPolicy.upstreamId());
    }

    private EvmRpcLog parseLog(JsonNode result) {
        List<String> topics = new ArrayList<>();
        result.path("topics").forEach(topic -> topics.add(topic.asText()));
        return new EvmRpcLog(
                result.path("address").asText(),
                topics,
                result.path("data").asText(),
                parseHexLong(result.path("blockNumber").asText()),
                result.path("blockHash").asText(),
                result.path("transactionHash").asText(),
                parseHexInt(result.path("transactionIndex").asText()),
                parseHexInt(result.path("logIndex").asText()),
                result.path("removed").asBoolean());
    }

    private EvmLog confirmed(EvmRpcLog log, long timestamp) {
        return new EvmLog(
                config.network(),
                log.address(),
                log.topics(),
                log.data(),
                log.blockNumber(),
                log.blockHash(),
                log.transactionHash(),
                log.transactionIndex(),
                log.logIndex(),
                timestamp);
    }

    private static long parseHexLong(String value) {
        return Long.parseUnsignedLong(value.substring(2), 16);
    }

    private static int parseHexInt(String value) {
        return Integer.parseUnsignedInt(value.substring(2), 16);
    }

    private synchronized void recordRetry() {
        retries++;
    }

    private String logSubscriptionRequest(long id, EvmLogFilter filter) {
        ObjectNode request = subscriptionRequestNode(id, "logs");
        ObjectNode query = request.withArray("params").addObject();
        ArrayNode addresses = query.putArray("address");
        filter.addresses().stream().sorted().forEach(addresses::add);
        ArrayNode topics = query.putArray("topics").addArray();
        filter.eventTopics().stream().sorted().forEach(topics::add);
        return request.toString();
    }

    private String subscriptionRequest(long id, String subscription) {
        return subscriptionRequestNode(id, subscription).toString();
    }

    private ObjectNode subscriptionRequestNode(long id, String subscription) {
        ObjectNode request = objectMapper.createObjectNode();
        request.put("jsonrpc", "2.0");
        request.put("id", id);
        request.put("method", "eth_subscribe");
        request.putArray("params").add(subscription);
        return request;
    }

    private record LogKey(long blockNumber, String blockHash, String transactionHash, int logIndex) {
        private static LogKey from(EvmRpcLog log) {
            return new LogKey(
                    log.blockNumber(),
                    log.blockHash(),
                    log.transactionHash(),
                    log.logIndex());
        }
    }
}
