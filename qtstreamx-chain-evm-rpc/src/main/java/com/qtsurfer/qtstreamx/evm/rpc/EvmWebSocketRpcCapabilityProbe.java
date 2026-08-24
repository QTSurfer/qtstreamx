package com.qtsurfer.qtstreamx.evm.rpc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.qtsurfer.qtstreamx.core.ws.WebSocketClient;
import java.io.IOException;
import java.net.http.HttpTimeoutException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Supplier;

/** Runs one bounded WebSocket connection to verify network, safe head, and subscriptions. */
public final class EvmWebSocketRpcCapabilityProbe {
    private static final int LOGS_INDEX = 0;
    private static final int HEADS_INDEX = 1;
    private static final int NETWORK_INDEX = 2;
    private static final int SAFE_BLOCK_INDEX = 3;
    private static final int OBSERVATION_COUNT = 4;
    private static final String SAFE_ALIAS = "[a-z][a-z0-9-]{0,62}";
    private static final String EVM_ADDRESS = "0x[0-9a-fA-F]{40}";
    private static final String EVM_TOPIC = "0x[0-9a-fA-F]{64}";
    private static final String EVM_BLOCK_HASH = "0x[0-9a-fA-F]{64}";

    private final EvmRpcWebSocketProbeConfig config;
    private final String upstreamId;
    private final Supplier<WebSocketClient> webSocketFactory;
    private final Clock clock;
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Creates a WebSocket capability probe.
     *
     * @param config network, endpoint, and response timeout
     * @param upstreamId opaque lowercase alias containing no endpoint or credential data
     * @param webSocketFactory creates the single transport used by the probe
     */
    public EvmWebSocketRpcCapabilityProbe(
            EvmRpcWebSocketProbeConfig config,
            String upstreamId,
            Supplier<WebSocketClient> webSocketFactory) {
        this(config, upstreamId, webSocketFactory, Clock.systemUTC());
    }

    EvmWebSocketRpcCapabilityProbe(
            EvmRpcWebSocketProbeConfig config,
            String upstreamId,
            Supplier<WebSocketClient> webSocketFactory,
            Clock clock) {
        this.config = Objects.requireNonNull(config, "config");
        this.upstreamId = validateUpstreamId(upstreamId);
        this.webSocketFactory = Objects.requireNonNull(webSocketFactory, "webSocketFactory");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * Probes network, safe head, and {@code logs}/{@code newHeads} acknowledgements over one
     * connection.
     *
     * @param filter known address/topic filter for the configured network
     * @param budget hard request, time, and amplification ceilings
     * @return endpoint-free WebSocket capability report
     * @throws IllegalArgumentException when the configured timeout or amplification exceeds budget
     */
    public EvmRpcCapabilityReport probe(EvmLogFilter filter, EvmRpcProbeBudget budget) {
        Objects.requireNonNull(filter, "filter");
        Objects.requireNonNull(budget, "budget");
        validateFilter(filter);
        validateBudget(budget);

        Instant startedAt = clock.instant();
        long deadlineNanos = deadlineAfter(budget.maxWallClock());
        CountDownLatch completed = new CountDownLatch(OBSERVATION_COUNT);
        AtomicReferenceArray<EvmRpcProbeObservation> observations =
                new AtomicReferenceArray<>(OBSERVATION_COUNT);
        WebSocketClient webSocket = Objects.requireNonNull(
                webSocketFactory.get(), "webSocketFactory result");
        webSocket.onMessage(message -> handleMessage(message, observations, completed, startedAt));
        webSocket.onClose((code, reason) -> completeOutstanding(
                observations,
                completed,
                EvmRpcProbeStatus.TRANSPORT_FAILURE,
                startedAt));
        webSocket.onError(error -> completeOutstanding(
                observations,
                completed,
                classifyTransport(error),
                startedAt));

        try {
            Duration connectTimeout = minimum(config.responseTimeout(), remaining(deadlineNanos));
            if (connectTimeout.isZero()) {
                completeOutstanding(
                        observations,
                        completed,
                        EvmRpcProbeStatus.TIMEOUT,
                        startedAt);
                return report(startedAt, observations);
            }
            webSocket.connect(config.webSocketUrl(), connectTimeout);
            sendWithinBudget(
                    webSocket,
                    observations,
                    completed,
                    LOGS_INDEX,
                    budget,
                    logSubscriptionRequest(filter),
                    deadlineNanos,
                    startedAt);
            sendWithinBudget(
                    webSocket,
                    observations,
                    completed,
                    HEADS_INDEX,
                    budget,
                    subscriptionRequest(2, "newHeads"),
                    deadlineNanos,
                    startedAt);
            sendWithinBudget(
                    webSocket,
                    observations,
                    completed,
                    NETWORK_INDEX,
                    budget,
                    rpcRequest(3, "eth_chainId"),
                    deadlineNanos,
                    startedAt);
            sendWithinBudget(
                    webSocket,
                    observations,
                    completed,
                    SAFE_BLOCK_INDEX,
                    budget,
                    safeBlockRequest(),
                    deadlineNanos,
                    startedAt);
            await(completed, observations, deadlineNanos, startedAt);
        } catch (Exception exception) {
            completeOutstanding(
                    observations,
                    completed,
                    classifyTransport(exception),
                    startedAt);
        } finally {
            close(webSocket);
        }

        return report(startedAt, observations);
    }

    private void sendWithinBudget(
            WebSocketClient webSocket,
            AtomicReferenceArray<EvmRpcProbeObservation> observations,
            CountDownLatch completed,
            int index,
            EvmRpcProbeBudget budget,
            String request,
            long deadlineNanos,
            Instant startedAt) {
        if (index >= budget.maxRequests() || remaining(deadlineNanos).isZero()) {
            complete(
                    observations,
                    completed,
                    index,
                    EvmRpcProbeStatus.BUDGET_EXHAUSTED,
                    OptionalInt.empty(),
                    startedAt);
            return;
        }
        webSocket.send(request);
    }

    private void handleMessage(
            String message,
            AtomicReferenceArray<EvmRpcProbeObservation> observations,
            CountDownLatch completed,
            Instant startedAt) {
        try {
            JsonNode root = objectMapper.readTree(message);
            if (!root.has("id")) {
                return;
            }
            int index = switch (root.path("id").asInt()) {
                case 1 -> LOGS_INDEX;
                case 2 -> HEADS_INDEX;
                case 3 -> NETWORK_INDEX;
                case 4 -> SAFE_BLOCK_INDEX;
                default -> -1;
            };
            if (index < 0) {
                return;
            }
            OptionalInt errorCode = root.hasNonNull("error")
                    ? OptionalInt.of(root.path("error").path("code").asInt())
                    : OptionalInt.empty();
            if (errorCode.isPresent()) {
                complete(
                        observations,
                        completed,
                        index,
                        classifyRpcError(errorCode.getAsInt()),
                        errorCode,
                        OptionalLong.empty(),
                        null,
                        startedAt);
            } else if (index == NETWORK_INDEX) {
                completeNetwork(root.path("result"), observations, completed, startedAt);
            } else if (index == SAFE_BLOCK_INDEX) {
                completeSafeBlock(root.path("result"), observations, completed, startedAt);
            } else {
                EvmRpcProbeStatus status = root.path("result").isTextual()
                                && !root.path("result").asText().isBlank()
                        ? EvmRpcProbeStatus.SUPPORTED
                        : EvmRpcProbeStatus.MALFORMED_RESPONSE;
                complete(observations, completed, index, status, errorCode, startedAt);
            }
        } catch (RuntimeException | IOException exception) {
            completeOutstanding(
                    observations,
                    completed,
                    EvmRpcProbeStatus.MALFORMED_RESPONSE,
                    startedAt);
        }
    }

    private void await(
            CountDownLatch completed,
            AtomicReferenceArray<EvmRpcProbeObservation> observations,
            long deadlineNanos,
            Instant startedAt) {
        try {
            Duration wait = minimum(config.responseTimeout(), remaining(deadlineNanos));
            if (wait.isZero()) {
                completeOutstanding(
                        observations,
                        completed,
                        EvmRpcProbeStatus.TIMEOUT,
                        startedAt);
                return;
            }
            boolean acknowledged = completed.await(
                    wait.toNanos(),
                    TimeUnit.NANOSECONDS);
            if (!acknowledged) {
                completeOutstanding(
                        observations,
                        completed,
                        EvmRpcProbeStatus.TIMEOUT,
                        startedAt);
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            completeOutstanding(
                    observations,
                    completed,
                    EvmRpcProbeStatus.TRANSPORT_FAILURE,
                    startedAt);
        }
    }

    private void completeNetwork(
            JsonNode result,
            AtomicReferenceArray<EvmRpcProbeObservation> observations,
            CountDownLatch completed,
            Instant startedAt) {
        EvmRpcProbeStatus status = EvmRpcProbeStatus.MALFORMED_RESPONSE;
        if (result.isTextual()) {
            try {
                long actualChainId = parseQuantity(result.asText());
                long expectedChainId = Long.parseLong(config.network().substring("eip155:".length()));
                status = actualChainId == expectedChainId
                        ? EvmRpcProbeStatus.SUPPORTED
                        : EvmRpcProbeStatus.WRONG_NETWORK;
            } catch (RuntimeException ignored) {
                // The endpoint-free malformed classification is the complete observation.
            }
        }
        complete(
                observations,
                completed,
                NETWORK_INDEX,
                status,
                OptionalInt.empty(),
                OptionalLong.empty(),
                null,
                startedAt);
    }

    private void completeSafeBlock(
            JsonNode result,
            AtomicReferenceArray<EvmRpcProbeObservation> observations,
            CountDownLatch completed,
            Instant startedAt) {
        EvmRpcProbeStatus status = EvmRpcProbeStatus.MALFORMED_RESPONSE;
        OptionalLong blockNumber = OptionalLong.empty();
        String blockHash = null;
        if (result.isObject()
                && result.path("number").isTextual()
                && result.path("hash").isTextual()
                && result.path("hash").asText().matches(EVM_BLOCK_HASH)) {
            try {
                blockNumber = OptionalLong.of(parseQuantity(result.path("number").asText()));
                blockHash = result.path("hash").asText();
                status = EvmRpcProbeStatus.SUPPORTED;
            } catch (RuntimeException ignored) {
                blockNumber = OptionalLong.empty();
                blockHash = null;
            }
        }
        complete(
                observations,
                completed,
                SAFE_BLOCK_INDEX,
                status,
                OptionalInt.empty(),
                blockNumber,
                blockHash,
                startedAt);
    }

    private static long parseQuantity(String value) {
        if (value == null || !value.matches("0x[0-9a-fA-F]+")) {
            throw new IllegalArgumentException("invalid hexadecimal quantity");
        }
        return Long.parseUnsignedLong(value.substring(2), 16);
    }

    private void completeOutstanding(
            AtomicReferenceArray<EvmRpcProbeObservation> observations,
            CountDownLatch completed,
            EvmRpcProbeStatus status,
            Instant startedAt) {
        for (int index = 0; index < OBSERVATION_COUNT; index++) {
            complete(
                    observations,
                    completed,
                    index,
                    status,
                    OptionalInt.empty(),
                    startedAt);
        }
    }

    private void complete(
            AtomicReferenceArray<EvmRpcProbeObservation> observations,
            CountDownLatch completed,
            int index,
            EvmRpcProbeStatus status,
            OptionalInt rpcErrorCode,
            Instant startedAt) {
        complete(
                observations,
                completed,
                index,
                status,
                rpcErrorCode,
                OptionalLong.empty(),
                null,
                startedAt);
    }

    private void complete(
            AtomicReferenceArray<EvmRpcProbeObservation> observations,
            CountDownLatch completed,
            int index,
            EvmRpcProbeStatus status,
            OptionalInt rpcErrorCode,
            OptionalLong blockNumber,
            String blockHash,
            Instant startedAt) {
        EvmRpcProbeObservation observation = new EvmRpcProbeObservation(
                EvmRpcTransport.WEBSOCKET,
                operation(index),
                purpose(index),
                status,
                blockNumber,
                OptionalLong.empty(),
                blockHash,
                OptionalInt.empty(),
                rpcErrorCode,
                clock.instant(),
                elapsed(startedAt));
        if (observations.compareAndSet(index, null, observation)) {
            completed.countDown();
        }
    }

    private Duration elapsed(Instant startedAt) {
        Duration elapsed = Duration.between(startedAt, clock.instant());
        return elapsed.isNegative() ? Duration.ZERO : elapsed;
    }

    private String logSubscriptionRequest(EvmLogFilter filter) {
        ObjectNode request = subscriptionRequestNode(1, "logs");
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

    private String rpcRequest(long id, String method) {
        ObjectNode request = objectMapper.createObjectNode();
        request.put("jsonrpc", "2.0");
        request.put("id", id);
        request.put("method", method);
        request.putArray("params");
        return request.toString();
    }

    private String safeBlockRequest() {
        ObjectNode request = objectMapper.createObjectNode();
        request.put("jsonrpc", "2.0");
        request.put("id", 4);
        request.put("method", "eth_getBlockByNumber");
        request.putArray("params").add("safe").add(false);
        return request.toString();
    }

    private ObjectNode subscriptionRequestNode(long id, String subscription) {
        ObjectNode request = objectMapper.createObjectNode();
        request.put("jsonrpc", "2.0");
        request.put("id", id);
        request.put("method", "eth_subscribe");
        request.putArray("params").add(subscription);
        return request;
    }

    private static List<EvmRpcProbeObservation> snapshot(
            AtomicReferenceArray<EvmRpcProbeObservation> observations) {
        List<EvmRpcProbeObservation> snapshot = new ArrayList<>(OBSERVATION_COUNT);
        for (int index = 0; index < OBSERVATION_COUNT; index++) {
            snapshot.add(Objects.requireNonNull(observations.get(index), "probe observation"));
        }
        return List.copyOf(snapshot);
    }

    private EvmRpcCapabilityReport report(
            Instant startedAt,
            AtomicReferenceArray<EvmRpcProbeObservation> observations) {
        return new EvmRpcCapabilityReport(
                upstreamId,
                config.network(),
                startedAt,
                clock.instant(),
                snapshot(observations));
    }

    private static long deadlineAfter(Duration duration) {
        return System.nanoTime() + duration.toNanos();
    }

    private static Duration remaining(long deadlineNanos) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        return remainingNanos <= 0 ? Duration.ZERO : Duration.ofNanos(remainingNanos);
    }

    private static Duration minimum(Duration left, Duration right) {
        return left.compareTo(right) <= 0 ? left : right;
    }

    private static EvmRpcProbeOperation operation(int index) {
        return switch (index) {
            case LOGS_INDEX -> EvmRpcProbeOperation.LOG_SUBSCRIPTION;
            case HEADS_INDEX -> EvmRpcProbeOperation.NEW_HEADS_SUBSCRIPTION;
            case NETWORK_INDEX -> EvmRpcProbeOperation.CHAIN_ID;
            case SAFE_BLOCK_INDEX -> EvmRpcProbeOperation.SAFE_BLOCK;
            default -> throw new IllegalArgumentException("unknown WebSocket probe index");
        };
    }

    private static EvmRpcProbePurpose purpose(int index) {
        return switch (index) {
            case LOGS_INDEX, HEADS_INDEX -> EvmRpcProbePurpose.LIVE_SUBSCRIPTION;
            case NETWORK_INDEX -> EvmRpcProbePurpose.NETWORK;
            case SAFE_BLOCK_INDEX -> EvmRpcProbePurpose.FINALITY;
            default -> throw new IllegalArgumentException("unknown WebSocket probe index");
        };
    }

    private static EvmRpcProbeStatus classifyRpcError(int code) {
        return code == 429
                ? EvmRpcProbeStatus.RATE_LIMITED
                : EvmRpcProbeStatus.UNSUPPORTED;
    }

    private static EvmRpcProbeStatus classifyTransport(Throwable exception) {
        return exception instanceof HttpTimeoutException
                ? EvmRpcProbeStatus.TIMEOUT
                : EvmRpcProbeStatus.TRANSPORT_FAILURE;
    }

    private void validateBudget(EvmRpcProbeBudget budget) {
        if (config.responseTimeout().compareTo(budget.maxWallClock()) > 0) {
            throw new IllegalArgumentException("response timeout exceeds the probe wall-clock budget");
        }
    }

    private static String validateUpstreamId(String upstreamId) {
        Objects.requireNonNull(upstreamId, "upstreamId");
        if (!upstreamId.matches(SAFE_ALIAS)) {
            throw new IllegalArgumentException("upstreamId must be a lowercase opaque alias");
        }
        return upstreamId;
    }

    private static void validateFilter(EvmLogFilter filter) {
        if (filter.addresses().size() != 1
                || filter.addresses().stream().noneMatch(address -> address.matches(EVM_ADDRESS))) {
            throw new IllegalArgumentException("filter must contain one 20-byte address");
        }
        if (filter.eventTopics().size() != 1
                || filter.eventTopics().stream().noneMatch(topic -> topic.matches(EVM_TOPIC))) {
            throw new IllegalArgumentException("filter must contain one 32-byte event topic");
        }
    }

    private static void close(WebSocketClient webSocket) {
        try {
            webSocket.close();
        } catch (Exception ignored) {
            // The report is already terminal and never includes provider-controlled close text.
        }
    }
}
