package com.qtsurfer.qtstreamx.canary;

import com.qtsurfer.qtstreamx.aggregation.CandleInterval;
import com.qtsurfer.qtstreamx.core.client.RecoverableMarketTradeStream;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.AddressBasedUniswapPairOrientation;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapDiscoveryLimits;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapDiscoveryPolicy;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapFactoryScan;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapNetworkTokenPolicy;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapV2MarketDiscovery;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapV3MarketDiscovery;
import com.qtsurfer.qtstreamx.dex.uniswap.v2.UniswapV2MarketDataStream;
import com.qtsurfer.qtstreamx.dex.uniswap.v2.UniswapV2Pair;
import com.qtsurfer.qtstreamx.dex.uniswap.v3.UniswapV3MarketDataStream;
import com.qtsurfer.qtstreamx.dex.uniswap.v3.UniswapV3Pool;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogFilter;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogStreamId;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcProbePlan;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcReader;
import com.qtsurfer.qtstreamx.evm.rpc.ActivePassiveEvmCaptureRoute;
import com.qtsurfer.qtstreamx.evm.rpc.ActivePassiveEvmLogStream;
import com.qtsurfer.qtstreamx.evm.rpc.EvmDiscoveryProvider;
import com.qtsurfer.qtstreamx.evm.rpc.EvmProviderBundle;
import com.qtsurfer.qtstreamx.evm.rpc.EvmProviderEndpoint;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Discovers selected Uniswap markets before starting a bounded V2 or V3 capture. */
public final class UniswapDiscoveryCaptureMain {

    private static final String V2_FACTORY_TOPIC =
            "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9";
    private static final String V3_FACTORY_TOPIC =
            "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118";
    private static final byte[] SYMBOL_CALL = {(byte) 0x95, (byte) 0xd8, (byte) 0x9b, 0x41};

    private static final Logger LOGGER =
            LoggerFactory.getLogger(UniswapDiscoveryCaptureMain.class);

    private UniswapDiscoveryCaptureMain() {}

    /**
     * Runs a one-shot discovery followed by a bounded live capture.
     *
     * @param args discovery policy, capture bounds, and runtime endpoint options
     * @throws Exception if configuration, discovery, capture, or artifact output fails
     */
    public static void main(String[] args) throws Exception {
        Arguments options = Arguments.parse(args);
        Version version = Version.parse(options.requireOne("version"));
        String network = options.requireOne("network");
        List<EvmProviderEndpoint> endpoints = List.of(
                new EvmProviderEndpoint(
                        options.one("active-alias", "active"),
                        runtimeEndpoint(options, "active-http-url", "QTSTREAMX_EVM_ACTIVE_HTTP_URL"),
                        runtimeEndpoint(options, "active-ws-url", "QTSTREAMX_EVM_ACTIVE_WS_URL")),
                new EvmProviderEndpoint(
                        options.one("passive-alias", "passive"),
                        runtimeEndpoint(options, "passive-http-url", "QTSTREAMX_EVM_PASSIVE_HTTP_URL"),
                        runtimeEndpoint(options, "passive-ws-url", "QTSTREAMX_EVM_PASSIVE_WS_URL")));
        int timeoutSeconds = options.positiveInt("timeout-seconds", 15);
        int retries = options.nonNegativeInt("retries", 3);
        int rpcMaxBlockRange = options.positiveInt("max-block-range", 2_000);
        int confirmations = options.nonNegativeInt("confirmations", 2);
        int overlapBlocks = options.nonNegativeInt("overlap-blocks", 2);
        long maxReplayBlocks = options.positiveLong("max-replay-blocks", 10_000);
        long maximumProviderLagBlocks = options.nonNegativeLong("max-provider-lag-blocks", 2);
        long safeHead = options.nonNegativeLong("discovery-safe-head");
        long factoryStartBlock = options.nonNegativeLong("factory-start-block");
        if (safeHead < factoryStartBlock) {
            throw new IllegalArgumentException(
                    "discovery-safe-head must not precede factory-start-block");
        }

        Set<String> quoteTokens = options.addresses("quote-token");
        Set<String> baseTokens = options.addresses("base-token");
        String factoryAddress = options.requireOne("factory");
        AddressBasedUniswapPairOrientation orientation =
                new AddressBasedUniswapPairOrientation(Map.of(
                        network, new UniswapNetworkTokenPolicy(quoteTokens, baseTokens)));
        long activityLookback = options.nonNegativeLong("activity-lookback-blocks", 0);
        UniswapDiscoveryPolicy policy = new UniswapDiscoveryPolicy(
                orientation,
                new UniswapDiscoveryLimits(
                        options.positiveLong("discovery-max-scan-blocks", 100_000),
                        options.positiveInt("discovery-max-metadata-calls", 2_000),
                        options.positiveInt("discovery-max-candidates", 1_000),
                        options.positiveInt("discovery-max-output", 100)),
                activityLookback == 0
                        ? OptionalLong.empty()
                        : OptionalLong.of(activityLookback));
        UniswapFactoryScan scan = new UniswapFactoryScan(
                network, factoryAddress, factoryStartBlock);
        DiscoveryCaptureDiagnostics diagnostics = new DiscoveryCaptureDiagnostics();

        long captureStartBlock = options.nonNegativeLong("capture-start-block");
        EvmLogStreamId streamId = new EvmLogStreamId(
                network, options.requireOne("stream-key"));
        ActivePassiveEvmCaptureRoute.Config routeConfig = new ActivePassiveEvmCaptureRoute.Config(
                network,
                captureStartBlock,
                confirmations,
                rpcMaxBlockRange,
                Duration.ofSeconds(timeoutSeconds),
                retries,
                overlapBlocks,
                maxReplayBlocks,
                maximumProviderLagBlocks);
        CandleInterval interval = new CandleInterval(
                options.one("interval-name", "1s"),
                options.positiveLong("interval-micros", 1_000_000));
        Path outputDirectory = Path.of(options.one(
                "out", "/tmp/canary/uniswap-" + version.cliName() + "-discovery"));
        Path checkpointDirectory = Path.of(options.one(
                "checkpoint-dir", outputDirectory.resolve("checkpoints").toString()));
        int durationSeconds = options.positiveInt("duration-seconds", 300);
        String factoryTopic = version == Version.V2 ? V2_FACTORY_TOPIC : V3_FACTORY_TOPIC;
        EvmLogFilter probeFilter = new EvmLogFilter(
                Set.of(factoryAddress), Set.of(factoryTopic));
        EvmRpcProbePlan probePlan = new EvmRpcProbePlan(
                probeFilter,
                captureStartBlock,
                captureStartBlock,
                factoryStartBlock,
                factoryStartBlock,
                quoteTokens.iterator().next(),
                SYMBOL_CALL,
                safeHead);
        List<EvmProviderBundle> bundles = ActivePassiveEvmCaptureRoute.probeBundles(
                endpoints, probeFilter, probePlan, routeConfig);
        EvmDiscoveryProvider discoveryProvider =
                ActivePassiveEvmCaptureRoute.selectDiscoveryProvider(bundles, routeConfig);
        LOGGER.info("Uniswap discovery selected upstream={}", discoveryProvider.upstreamId());

        switch (version) {
            case V2 -> captureV2(
                    scan,
                    discoveryProvider.reader(),
                    policy,
                    diagnostics,
                    safeHead,
                    bundles,
                    streamId,
                    checkpointDirectory,
                    routeConfig,
                    interval,
                    outputDirectory,
                    durationSeconds);
            case V3 -> captureV3(
                    scan,
                    discoveryProvider.reader(),
                    policy,
                    diagnostics,
                    safeHead,
                    bundles,
                    streamId,
                    checkpointDirectory,
                    routeConfig,
                    interval,
                    outputDirectory,
                    durationSeconds);
        }
    }

    private static void captureV2(
            UniswapFactoryScan scan,
            EvmRpcReader reader,
            UniswapDiscoveryPolicy policy,
            DiscoveryCaptureDiagnostics diagnostics,
            long safeHead,
            List<EvmProviderBundle> bundles,
            EvmLogStreamId streamId,
            Path checkpointDirectory,
            ActivePassiveEvmCaptureRoute.Config routeConfig,
            CandleInterval interval,
            Path outputDirectory,
            int durationSeconds) throws Exception {
        Set<UniswapV2Pair> selected = new UniswapV2MarketDiscovery(
                        scan, reader, policy, diagnostics)
                .refresh(safeHead)
                .toCompletableFuture()
                .join();
        requireSelection(selected, diagnostics);
        ActivePassiveEvmLogStream logStream = ActivePassiveEvmCaptureRoute.createProbed(
                bundles, streamId, checkpointDirectory, routeConfig);
        capture(
                new UniswapV2MarketDataStream(
                        logStream, selected),
                interval,
                outputDirectory,
                diagnostics.report(selected.size()),
                durationSeconds);
    }

    private static void captureV3(
            UniswapFactoryScan scan,
            EvmRpcReader reader,
            UniswapDiscoveryPolicy policy,
            DiscoveryCaptureDiagnostics diagnostics,
            long safeHead,
            List<EvmProviderBundle> bundles,
            EvmLogStreamId streamId,
            Path checkpointDirectory,
            ActivePassiveEvmCaptureRoute.Config routeConfig,
            CandleInterval interval,
            Path outputDirectory,
            int durationSeconds) throws Exception {
        Set<UniswapV3Pool> selected = new UniswapV3MarketDiscovery(
                        scan, reader, policy, diagnostics)
                .refresh(safeHead)
                .toCompletableFuture()
                .join();
        requireSelection(selected, diagnostics);
        ActivePassiveEvmLogStream logStream = ActivePassiveEvmCaptureRoute.createProbed(
                bundles, streamId, checkpointDirectory, routeConfig);
        capture(
                new UniswapV3MarketDataStream(
                        logStream, selected),
                interval,
                outputDirectory,
                diagnostics.report(selected.size()),
                durationSeconds);
    }

    private static void requireSelection(
            Set<?> selected,
            DiscoveryCaptureDiagnostics diagnostics) {
        DiscoveryCaptureReport report = diagnostics.report(selected.size());
        LOGGER.info(
                "Uniswap discovery complete: discovered={} selected={} rejected={}",
                report.discovered(),
                report.selected(),
                report.rejected());
        if (selected.isEmpty()) {
            throw new IllegalStateException("Uniswap discovery selected no markets");
        }
    }

    private static void capture(
            RecoverableMarketTradeStream stream,
            CandleInterval interval,
            Path outputDirectory,
            DiscoveryCaptureReport report,
            int durationSeconds) throws Exception {
        try (DexCaptureSession session =
                new DexCaptureSession(stream, interval, outputDirectory, report)) {
            session.startRecoverable();
            long endMillis = Math.addExact(
                    System.currentTimeMillis(), Math.multiplyExact(durationSeconds, 1_000L));
            while (System.currentTimeMillis() < endMillis) {
                long remaining = endMillis - System.currentTimeMillis();
                Thread.sleep(Math.min(1_000L, Math.max(1L, remaining)));
                session.advanceWatermark(System.currentTimeMillis() * 1_000L);
            }
            LOGGER.info(
                    "Uniswap discovery capture complete: connected={} trades={}",
                    session.isConnected(),
                    session.tradeCount());
        }
    }

    private static String runtimeEndpoint(
            Arguments options,
            String option,
            String environment) {
        return options.optionalOne(option)
                .filter(value -> !value.isBlank())
                .orElseGet(() -> requireEnvironment(environment));
    }

    private static String requireEnvironment(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required when its CLI option is absent");
        }
        return value;
    }

    private enum Version {
        V2("v2"),
        V3("v3");

        private final String cliName;

        Version(String cliName) {
            this.cliName = cliName;
        }

        private static Version parse(String value) {
            return Arrays.stream(values())
                    .filter(version -> version.cliName.equalsIgnoreCase(value))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("version must be v2 or v3"));
        }

        private String cliName() {
            return cliName;
        }
    }

    private record Arguments(Map<String, List<String>> values) {

        private static Arguments parse(String[] args) {
            Map<String, List<String>> values = new LinkedHashMap<>();
            for (int index = 0; index < args.length; index += 2) {
                if (!args[index].startsWith("--") || index + 1 >= args.length) {
                    throw new IllegalArgumentException("arguments must be --name value pairs");
                }
                values.computeIfAbsent(args[index].substring(2), ignored -> new ArrayList<>())
                        .add(args[index + 1]);
            }
            return new Arguments(values);
        }

        private List<String> all(String name) {
            return List.copyOf(values.getOrDefault(name, List.of()));
        }

        private Optional<String> optionalOne(String name) {
            List<String> matches = all(name);
            if (matches.size() > 1) {
                throw new IllegalArgumentException("--" + name + " may be supplied only once");
            }
            return matches.stream().findFirst();
        }

        private String requireOne(String name) {
            return optionalOne(name)
                    .filter(value -> !value.isBlank())
                    .orElseThrow(() -> new IllegalArgumentException("--" + name + " is required"));
        }

        private String one(String name, String defaultValue) {
            return optionalOne(name).orElse(defaultValue);
        }

        private Set<String> addresses(String name) {
            Set<String> addresses = new LinkedHashSet<>();
            for (String value : all(name)) {
                Arrays.stream(value.split(","))
                        .map(String::trim)
                        .filter(address -> !address.isEmpty())
                        .forEach(addresses::add);
            }
            if (addresses.isEmpty()) {
                throw new IllegalArgumentException("at least one --" + name + " is required");
            }
            return Set.copyOf(addresses);
        }

        private int positiveInt(String name, int defaultValue) {
            int value = Integer.parseInt(one(name, Integer.toString(defaultValue)));
            if (value <= 0) {
                throw new IllegalArgumentException("--" + name + " must be positive");
            }
            return value;
        }

        private int nonNegativeInt(String name, int defaultValue) {
            int value = Integer.parseInt(one(name, Integer.toString(defaultValue)));
            if (value < 0) {
                throw new IllegalArgumentException("--" + name + " must be non-negative");
            }
            return value;
        }

        private long positiveLong(String name, long defaultValue) {
            long value = Long.parseLong(one(name, Long.toString(defaultValue)));
            if (value <= 0) {
                throw new IllegalArgumentException("--" + name + " must be positive");
            }
            return value;
        }

        private long nonNegativeLong(String name) {
            long value = Long.parseLong(requireOne(name));
            if (value < 0) {
                throw new IllegalArgumentException("--" + name + " must be non-negative");
            }
            return value;
        }

        private long nonNegativeLong(String name, long defaultValue) {
            long value = Long.parseLong(one(name, Long.toString(defaultValue)));
            if (value < 0) {
                throw new IllegalArgumentException("--" + name + " must be non-negative");
            }
            return value;
        }
    }
}
