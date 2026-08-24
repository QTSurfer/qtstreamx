package com.qtsurfer.qtstreamx.evm.rpc;

import com.qtsurfer.qtstreamx.ws.jdk.JdkWebSocketClient;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Builds one probe-gated active/passive capture route at the application boundary. */
public final class ActivePassiveEvmCaptureRoute {
    private static final Duration HTTP_PROBE_WALL_CLOCK = Duration.ofSeconds(30);
    private static final Duration WEBSOCKET_PROBE_WALL_CLOCK = Duration.ofSeconds(15);

    private ActivePassiveEvmCaptureRoute() {}

    /**
     * Probes both runtime bundles and creates a durable provider-neutral log stream.
     *
     * <p>The fixed probe performs no retries and caps both transports at 12 requests and 45
     * seconds per bundle. The resulting route never hedges: only the selected bundle consumes its
     * configured per-operation retry budget.
     *
     * @param endpoints exactly two runtime provider bundles, active first
     * @param streamId stable logical stream identity
     * @param checkpointDirectory durable checkpoint directory
     * @param filter complete capture filter
     * @param probeAddress one configured contract used for bounded capability probes
     * @param probeTopic one configured event topic used for bounded capability probes
     * @param stateCallData read-only ABI call known to be valid on the probe contract
     * @param config provider-neutral stream settings
     * @return durable active/passive log stream
     * @throws Exception when probing or checkpoint-store creation fails
     */
    public static ActivePassiveEvmLogStream create(
            List<EvmProviderEndpoint> endpoints,
            EvmLogStreamId streamId,
            Path checkpointDirectory,
            EvmLogFilter filter,
            String probeAddress,
            String probeTopic,
            byte[] stateCallData,
            Config config) throws Exception {
        Objects.requireNonNull(endpoints, "endpoints");
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(checkpointDirectory, "checkpointDirectory");
        Objects.requireNonNull(filter, "filter");
        Objects.requireNonNull(probeAddress, "probeAddress");
        Objects.requireNonNull(probeTopic, "probeTopic");
        Objects.requireNonNull(stateCallData, "stateCallData");
        Objects.requireNonNull(config, "config");
        if (endpoints.size() != 2) {
            throw new IllegalArgumentException("active/passive capture requires exactly two endpoints");
        }
        if (!streamId.network().equals(config.network())) {
            throw new IllegalArgumentException("stream identity and route network must match");
        }

        EvmLogFilter probeFilter = new EvmLogFilter(Set.of(probeAddress), Set.of(probeTopic));
        EvmRpcProbePlan plan = new EvmRpcProbePlan(
                probeFilter,
                config.startBlock(),
                config.startBlock(),
                config.startBlock(),
                config.startBlock(),
                probeAddress,
                stateCallData,
                config.startBlock());
        List<EvmProviderBundle> bundles = probeBundles(endpoints, probeFilter, plan, config);
        return createProbed(bundles, streamId, checkpointDirectory, config);
    }

    public static List<EvmProviderBundle> probeBundles(
            List<EvmProviderEndpoint> endpoints,
            EvmLogFilter probeFilter,
            EvmRpcProbePlan plan,
            Config config) {
        Objects.requireNonNull(endpoints, "endpoints");
        Objects.requireNonNull(probeFilter, "probeFilter");
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(config, "config");
        if (endpoints.size() != 2) {
            throw new IllegalArgumentException("active/passive capture requires exactly two endpoints");
        }
        EvmRpcProbeBudget defaults = EvmRpcProbeBudget.safeDefaults();
        EvmRpcProbeBudget httpBudget = new EvmRpcProbeBudget(
                8,
                HTTP_PROBE_WALL_CLOCK,
                defaults.maxLogBlockRange(),
                defaults.maxReturnedLogs());
        EvmRpcProbeBudget webSocketBudget = new EvmRpcProbeBudget(
                4,
                WEBSOCKET_PROBE_WALL_CLOCK,
                defaults.maxLogBlockRange(),
                defaults.maxReturnedLogs());
        return endpoints.stream()
                .map(endpoint -> probe(
                        endpoint,
                        config,
                        probeFilter,
                        plan,
                        httpBudget,
                        webSocketBudget))
                .toList();
    }

    public static ActivePassiveEvmLogStream createProbed(
            List<EvmProviderBundle> bundles,
            EvmLogStreamId streamId,
            Path checkpointDirectory,
            Config config) throws Exception {
        Objects.requireNonNull(bundles, "bundles");
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(checkpointDirectory, "checkpointDirectory");
        Objects.requireNonNull(config, "config");
        FileEvmLogCheckpointStore checkpointStore = new FileEvmLogCheckpointStore(checkpointDirectory);

        return new ActivePassiveEvmLogStream(
                bundles,
                streamId,
                checkpointStore,
                config.overlapBlocks(),
                config.maxReplayBlocks(),
                config.maximumProviderLagBlocks(),
                (bundle, store, policy) -> new EvmRpcLogStream(
                        new EvmLogStreamConfig(
                                config.network(),
                                bundle.webSocketUrl(),
                                bundle.httpUrl(),
                                config.startBlock(),
                                config.confirmationDepth(),
                                config.maxBlockRange(),
                                config.requestTimeout(),
                                config.maxRetries()),
                        JdkWebSocketClient::new,
                        store,
                        policy));
    }

    public static EvmDiscoveryProvider selectDiscoveryProvider(
            List<EvmProviderBundle> bundles,
            Config config) {
        EvmProviderBundle selected = EvmProviderBundleEligibility.selectDiscovery(
                bundles, config.network(), config.maximumProviderLagBlocks());
        return new EvmDiscoveryProvider(
                selected.upstreamId(),
                new EvmHttpRpcReader(new EvmRpcReaderConfig(
                        config.network(),
                        selected.httpUrl(),
                        config.maxBlockRange(),
                        config.requestTimeout(),
                        config.maxRetries())));
    }

    private static EvmProviderBundle probe(
            EvmProviderEndpoint endpoint,
            Config config,
            EvmLogFilter filter,
            EvmRpcProbePlan plan,
            EvmRpcProbeBudget httpBudget,
            EvmRpcProbeBudget webSocketBudget) {
        EvmRpcCapabilityReport http = new EvmHttpRpcCapabilityProbe(
                        new EvmRpcReaderConfig(
                                config.network(),
                                endpoint.httpUrl(),
                                config.maxBlockRange(),
                                config.requestTimeout(),
                                0),
                        endpoint.upstreamId())
                .probe(plan, httpBudget, EvmRpcProbeScope.ROUTE);
        EvmRpcCapabilityReport webSocket = new EvmWebSocketRpcCapabilityProbe(
                        new EvmRpcWebSocketProbeConfig(
                                config.network(),
                                endpoint.webSocketUrl(),
                                config.requestTimeout()),
                        endpoint.upstreamId(),
                        JdkWebSocketClient::new)
                .probe(filter, webSocketBudget);
        return new EvmProviderBundle(
                endpoint.upstreamId(),
                endpoint.httpUrl(),
                endpoint.webSocketUrl(),
                http.merge(webSocket));
    }

    /** Provider-neutral runtime limits for one durable capture route. */
    public record Config(
            String network,
            long startBlock,
            int confirmationDepth,
            int maxBlockRange,
            Duration requestTimeout,
            int maxRetries,
            int overlapBlocks,
            long maxReplayBlocks,
            long maximumProviderLagBlocks
    ) {
        public Config {
            Objects.requireNonNull(network, "network");
            Objects.requireNonNull(requestTimeout, "requestTimeout");
            if (startBlock < 0) {
                throw new IllegalArgumentException("startBlock must be non-negative");
            }
            if (confirmationDepth < 0 || overlapBlocks < 0) {
                throw new IllegalArgumentException("confirmation and overlap blocks must be non-negative");
            }
            if (maxBlockRange < 1 || maxReplayBlocks < 1) {
                throw new IllegalArgumentException("block range and replay bounds must be positive");
            }
            if (requestTimeout.isZero() || requestTimeout.isNegative()) {
                throw new IllegalArgumentException("requestTimeout must be positive");
            }
            if (maxRetries < 0 || maximumProviderLagBlocks < 0) {
                throw new IllegalArgumentException("retry and provider-lag bounds must be non-negative");
            }
        }
    }
}
