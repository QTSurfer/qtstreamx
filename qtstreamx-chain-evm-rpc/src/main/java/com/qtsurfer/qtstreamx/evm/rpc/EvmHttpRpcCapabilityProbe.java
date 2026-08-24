package com.qtsurfer.qtstreamx.evm.rpc;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpTimeoutException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.Supplier;

/**
 * Runs a bounded, direct HTTP capability probe against one configured EVM upstream.
 *
 * <p>Log observations issue exactly one provider request: they do not use the normal reader's
 * pagination or bisection. A successful interval can therefore be reported as proven while a
 * rejected interval remains an explicit capability outcome.
 */
public final class EvmHttpRpcCapabilityProbe {
    private static final int STARTUP_OPERATION_COUNT = 6;
    private static final int ROUTE_OPERATION_COUNT = 8;
    private static final int FULL_OPERATION_COUNT = 10;
    private static final int MAX_PROBE_RESPONSE_BYTES = 16 * 1024 * 1024;
    private static final Duration MAX_PROBE_REQUEST_TIMEOUT = Duration.ofSeconds(3);
    private static final String SAFE_ALIAS = "[a-z][a-z0-9-]{0,62}";

    private final EvmRpcReaderConfig config;
    private final String upstreamId;
    private final EvmRpcProbeHttpClient client;
    private final Clock clock;

    /**
     * Creates a probe backed by the JDK HTTP transport.
     *
     * @param config network, endpoint, timeout, and retry configuration
     * @param upstreamId opaque lowercase alias containing no endpoint or credential data
     */
    public EvmHttpRpcCapabilityProbe(EvmRpcReaderConfig config, String upstreamId) {
        this(
                config,
                upstreamId,
                new JsonRpcHttpClient(
                        new ProbeRequestConfig(effectiveRequestTimeout(config), 0),
                        new JdkJsonRpcHttpTransport(
                                endpoint(config.httpUrl()),
                                MAX_PROBE_RESPONSE_BYTES)),
                Clock.systemUTC());
    }

    EvmHttpRpcCapabilityProbe(
            EvmRpcReaderConfig config,
            String upstreamId,
            EvmRpcProbeHttpClient client,
            Clock clock) {
        this.config = Objects.requireNonNull(config, "config");
        this.upstreamId = validateUpstreamId(upstreamId);
        this.client = Objects.requireNonNull(client, "client");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    EvmHttpRpcCapabilityProbe(
            EvmRpcReaderConfig config,
            String upstreamId,
            JsonRpcHttpTransport transport,
            Clock clock) {
        this(
                config,
                upstreamId,
                new JsonRpcHttpClient(
                        new ProbeRequestConfig(effectiveRequestTimeout(config), 0),
                        transport),
                clock);
    }

    /**
     * Executes the fixed HTTP probe sequence within the declared budget.
     *
     * @param plan network-specific known log and state inputs
     * @param budget hard request, time, range, and result ceilings
     * @param scope explicit startup-only or full historical probe scope
     * @return immutable endpoint-free capability report
     * @throws IllegalArgumentException when the plan or request deadlines exceed the budget
     */
    public EvmRpcCapabilityReport probe(
            EvmRpcProbePlan plan,
            EvmRpcProbeBudget budget,
            EvmRpcProbeScope scope) {
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(budget, "budget");
        Objects.requireNonNull(scope, "scope");
        validateBudget(plan, budget, scope);

        Instant startedAt = clock.instant();
        ProbeContext context = new ProbeContext(startedAt, budget);
        long expectedChainId = parseEip155ChainId(config.network());

        observe(
                context,
                EvmRpcProbeOperation.CHAIN_ID,
                EvmRpcProbePurpose.NETWORK,
                OptionalLong.empty(),
                OptionalLong.empty(),
                () -> {
                    long actualChainId = client.chainId();
                    EvmRpcProbeStatus status = actualChainId == expectedChainId
                            ? EvmRpcProbeStatus.SUPPORTED
                            : EvmRpcProbeStatus.WRONG_NETWORK;
                    return ProbeValue.status(status);
                });
        observe(
                context,
                EvmRpcProbeOperation.BLOCK_NUMBER,
                EvmRpcProbePurpose.HEAD,
                OptionalLong.empty(),
                OptionalLong.empty(),
                () -> ProbeValue.block(client.latestBlockNumber(), null));
        observeBlock(context, EvmRpcProbeOperation.SAFE_BLOCK, EvmBlockTag.safe());
        if (scope != EvmRpcProbeScope.ROUTE) {
            observeBlock(context, EvmRpcProbeOperation.FINALIZED_BLOCK, EvmBlockTag.finalized());
        }
        observeState(
                context,
                plan,
                EvmRpcProbeOperation.CALL,
                EvmBlockTag.latest(),
                OptionalLong.empty(),
                EvmRpcProbePurpose.LIVE_STATE);
        if (scope != EvmRpcProbeScope.ROUTE) {
            observeState(
                    context,
                    plan,
                    EvmRpcProbeOperation.GET_CODE,
                    EvmBlockTag.latest(),
                    OptionalLong.empty(),
                    EvmRpcProbePurpose.LIVE_STATE);
        }
        if (scope == EvmRpcProbeScope.STARTUP) {
            return report(startedAt, context);
        }
        observeLogs(
                context,
                plan,
                EvmRpcProbePurpose.RECOVERY_LOGS,
                plan.recentLogsFromBlock(),
                plan.recentLogsToBlock());
        observeLogs(
                context,
                plan,
                EvmRpcProbePurpose.DISCOVERY_LOGS,
                plan.historicalLogsFromBlock(),
                plan.historicalLogsToBlock());
        EvmBlockTag historicalBlock = EvmBlockTag.number(plan.historicalStateBlock());
        OptionalLong historicalBlockNumber = OptionalLong.of(plan.historicalStateBlock());
        observeState(
                context,
                plan,
                EvmRpcProbeOperation.CALL,
                historicalBlock,
                historicalBlockNumber,
                EvmRpcProbePurpose.HISTORICAL_STATE);
        observeState(
                context,
                plan,
                EvmRpcProbeOperation.GET_CODE,
                historicalBlock,
                historicalBlockNumber,
                EvmRpcProbePurpose.HISTORICAL_STATE);

        return report(startedAt, context);
    }

    private EvmRpcCapabilityReport report(Instant startedAt, ProbeContext context) {
        return new EvmRpcCapabilityReport(
                upstreamId,
                config.network(),
                startedAt,
                clock.instant(),
                context.observations());
    }

    private void observeBlock(
            ProbeContext context,
            EvmRpcProbeOperation operation,
            EvmBlockTag blockTag) {
        observe(
                context,
                operation,
                EvmRpcProbePurpose.FINALITY,
                OptionalLong.empty(),
                OptionalLong.empty(),
                () -> {
                    EvmBlock block = client.getBlock(blockTag);
                    return ProbeValue.block(block.number(), block.hash());
                });
    }

    private void observeLogs(
            ProbeContext context,
            EvmRpcProbePlan plan,
            EvmRpcProbePurpose purpose,
            long fromBlock,
            long toBlock) {
        int remainingResults = context.remainingLogResults();
        if (remainingResults == 0) {
            context.add(observation(
                    EvmRpcProbeOperation.GET_LOGS,
                    purpose,
                    EvmRpcProbeStatus.RESULT_LIMIT,
                    OptionalLong.of(fromBlock),
                    OptionalLong.of(toBlock),
                    null,
                    OptionalInt.empty(),
                    OptionalInt.empty(),
                    clock.instant(),
                    Duration.ZERO));
            return;
        }
        observe(
                context,
                EvmRpcProbeOperation.GET_LOGS,
                purpose,
                OptionalLong.of(fromBlock),
                OptionalLong.of(toBlock),
                () -> {
                    List<EvmRpcLog> logs = client.getLogs(
                            plan.logFilter(), fromBlock, toBlock, remainingResults);
                    context.acceptLogResults(logs.size());
                    return ProbeValue.count(EvmRpcProbeStatus.SUPPORTED, logs.size());
                });
    }

    private void observeState(
            ProbeContext context,
            EvmRpcProbePlan plan,
            EvmRpcProbeOperation operation,
            EvmBlockTag blockTag,
            OptionalLong block,
            EvmRpcProbePurpose purpose) {
        observe(
                context,
                operation,
                purpose,
                block,
                block,
                () -> {
                    if (operation == EvmRpcProbeOperation.CALL) {
                        client.call(plan.stateContractAddress(), plan.callData(), blockTag);
                    } else {
                        client.code(plan.stateContractAddress(), blockTag);
                    }
                    return ProbeValue.status(EvmRpcProbeStatus.SUPPORTED);
                });
    }

    private void observe(
            ProbeContext context,
            EvmRpcProbeOperation operation,
            EvmRpcProbePurpose purpose,
            OptionalLong fromBlock,
            OptionalLong toBlock,
            Supplier<ProbeValue> request) {
        Instant operationStartedAt = clock.instant();
        if (!context.tryStart(clock.instant())) {
            context.add(observation(
                    operation,
                    purpose,
                    EvmRpcProbeStatus.BUDGET_EXHAUSTED,
                    fromBlock,
                    toBlock,
                    null,
                    OptionalInt.empty(),
                    OptionalInt.empty(),
                    clock.instant(),
                    Duration.ZERO));
            return;
        }
        try {
            ProbeValue value = request.get();
            context.add(observation(
                    operation,
                    purpose,
                    value.status(),
                    value.fromBlock().isPresent() ? value.fromBlock() : fromBlock,
                    value.toBlock().isPresent() ? value.toBlock() : toBlock,
                    value.blockHash(),
                    value.resultCount(),
                    OptionalInt.empty(),
                    clock.instant(),
                    elapsed(operationStartedAt)));
        } catch (RuntimeException exception) {
            if (exception instanceof EvmRpcResultLimitException) {
                context.exhaustLogResults();
            }
            context.add(observation(
                    operation,
                    purpose,
                    classify(operation, exception),
                    fromBlock,
                    toBlock,
                    null,
                    OptionalInt.empty(),
                    rpcErrorCode(exception),
                    clock.instant(),
                    elapsed(operationStartedAt)));
        }
    }

    private EvmRpcProbeObservation observation(
            EvmRpcProbeOperation operation,
            EvmRpcProbePurpose purpose,
            EvmRpcProbeStatus status,
            OptionalLong fromBlock,
            OptionalLong toBlock,
            String blockHash,
            OptionalInt resultCount,
            OptionalInt rpcErrorCode,
            Instant measuredAt,
            Duration elapsed) {
        return new EvmRpcProbeObservation(
                EvmRpcTransport.HTTP,
                operation,
                purpose,
                status,
                fromBlock,
                toBlock,
                blockHash,
                resultCount,
                rpcErrorCode,
                measuredAt,
                elapsed);
    }

    private Duration elapsed(Instant startedAt) {
        Duration elapsed = Duration.between(startedAt, clock.instant());
        return elapsed.isNegative() ? Duration.ZERO : elapsed;
    }

    private static EvmRpcProbeStatus classify(
            EvmRpcProbeOperation operation,
            RuntimeException exception) {
        if (exception instanceof UnsupportedOperationException) {
            return EvmRpcProbeStatus.UNSUPPORTED;
        }
        if (exception instanceof EvmRpcException rpcException) {
            if (rpcException.code() == 429) {
                return EvmRpcProbeStatus.RATE_LIMITED;
            }
            return EvmRpcProbeStatus.UNKNOWN;
        }
        if (exception instanceof EvmRpcResultLimitException) {
            return EvmRpcProbeStatus.RESULT_LIMIT;
        }
        if (hasCause(exception, HttpTimeoutException.class)) {
            return EvmRpcProbeStatus.TIMEOUT;
        }
        if (hasCause(exception, IOException.class)) {
            return EvmRpcProbeStatus.TRANSPORT_FAILURE;
        }
        return EvmRpcProbeStatus.MALFORMED_RESPONSE;
    }

    private static OptionalInt rpcErrorCode(RuntimeException exception) {
        return exception instanceof EvmRpcException rpcException
                ? OptionalInt.of(rpcException.code())
                : OptionalInt.empty();
    }

    private static boolean hasCause(Throwable throwable, Class<? extends Throwable> type) {
        Throwable current = throwable;
        while (current != null) {
            if (type.isInstance(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private void validateBudget(
            EvmRpcProbePlan plan,
            EvmRpcProbeBudget budget,
            EvmRpcProbeScope scope) {
        if (scope != EvmRpcProbeScope.STARTUP) {
            long recentBlocks = blockCount(plan.recentLogsFromBlock(), plan.recentLogsToBlock());
            long historicalBlocks = blockCount(
                    plan.historicalLogsFromBlock(), plan.historicalLogsToBlock());
            if (Math.addExact(recentBlocks, historicalBlocks) > budget.maxLogBlockRange()) {
                throw new IllegalArgumentException("log intervals exceed the probe budget");
            }
        }
        int operationCount = switch (scope) {
            case STARTUP -> STARTUP_OPERATION_COUNT;
            case ROUTE -> ROUTE_OPERATION_COUNT;
            case FULL -> FULL_OPERATION_COUNT;
        };
        long requestCount = Math.min(operationCount, budget.maxRequests());
        Duration worstCase = effectiveRequestTimeout(config).multipliedBy(requestCount);
        if (worstCase.compareTo(budget.maxWallClock()) > 0) {
            throw new IllegalArgumentException("request timeouts exceed the probe wall-clock budget");
        }
    }

    private static long blockCount(long fromBlock, long toBlock) {
        return Math.addExact(Math.subtractExact(toBlock, fromBlock), 1L);
    }

    private static long parseEip155ChainId(String network) {
        if (!network.startsWith("eip155:")) {
            throw new IllegalArgumentException("network must use the eip155 CAIP-2 namespace");
        }
        try {
            return Long.parseLong(network.substring("eip155:".length()));
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("network must contain a numeric EVM chain ID", exception);
        }
    }

    private static String validateUpstreamId(String upstreamId) {
        Objects.requireNonNull(upstreamId, "upstreamId");
        if (!upstreamId.matches(SAFE_ALIAS)) {
            throw new IllegalArgumentException("upstreamId must be a lowercase opaque alias");
        }
        return upstreamId;
    }

    private static Duration effectiveRequestTimeout(EvmRpcReaderConfig config) {
        return config.requestTimeout().compareTo(MAX_PROBE_REQUEST_TIMEOUT) > 0
                ? MAX_PROBE_REQUEST_TIMEOUT
                : config.requestTimeout();
    }

    private static URI endpoint(String httpUrl) {
        try {
            return URI.create(httpUrl);
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("httpUrl must be a valid URI");
        }
    }

    private record ProbeRequestConfig(Duration requestTimeout, int maxRetries)
            implements EvmRpcRequestConfig {}

    private record ProbeValue(
            EvmRpcProbeStatus status,
            OptionalLong fromBlock,
            OptionalLong toBlock,
            String blockHash,
            OptionalInt resultCount
    ) {
        private static ProbeValue status(EvmRpcProbeStatus status) {
            return new ProbeValue(
                    status,
                    OptionalLong.empty(),
                    OptionalLong.empty(),
                    null,
                    OptionalInt.empty());
        }

        private static ProbeValue block(long blockNumber, String blockHash) {
            return new ProbeValue(
                    EvmRpcProbeStatus.SUPPORTED,
                    OptionalLong.of(blockNumber),
                    OptionalLong.of(blockNumber),
                    blockHash,
                    OptionalInt.empty());
        }

        private static ProbeValue count(EvmRpcProbeStatus status, int count) {
            return new ProbeValue(
                    status,
                    OptionalLong.empty(),
                    OptionalLong.empty(),
                    null,
                    OptionalInt.of(count));
        }
    }

    private static final class ProbeContext {
        private final Instant startedAt;
        private final EvmRpcProbeBudget budget;
        private final List<EvmRpcProbeObservation> observations = new ArrayList<>();
        private int requests;
        private int returnedLogs;

        private ProbeContext(Instant startedAt, EvmRpcProbeBudget budget) {
            this.startedAt = startedAt;
            this.budget = budget;
        }

        private boolean tryStart(Instant now) {
            if (requests >= budget.maxRequests()) {
                return false;
            }
            if (Duration.between(startedAt, now).compareTo(budget.maxWallClock()) >= 0) {
                return false;
            }
            requests++;
            return true;
        }

        private void add(EvmRpcProbeObservation observation) {
            observations.add(observation);
        }

        private int remainingLogResults() {
            return budget.maxReturnedLogs() - returnedLogs;
        }

        private void acceptLogResults(int count) {
            returnedLogs = Math.addExact(returnedLogs, count);
        }

        private void exhaustLogResults() {
            returnedLogs = budget.maxReturnedLogs();
        }

        private List<EvmRpcProbeObservation> observations() {
            return List.copyOf(observations);
        }
    }
}
