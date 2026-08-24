package com.qtsurfer.qtstreamx.canary;

import com.qtsurfer.qtstreamx.aggregation.CandleInterval;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.dex.core.EvmToken;
import com.qtsurfer.qtstreamx.dex.uniswap.v3.UniswapV3MarketDataStream;
import com.qtsurfer.qtstreamx.dex.uniswap.v3.UniswapV3Pool;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogFilter;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogStreamId;
import com.qtsurfer.qtstreamx.evm.rpc.ActivePassiveEvmCaptureRoute;
import com.qtsurfer.qtstreamx.evm.rpc.ActivePassiveEvmLogStream;
import com.qtsurfer.qtstreamx.evm.rpc.EvmProviderEndpoint;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Dedicated command-line entry point for read-only Uniswap v3 market-data capture. */
public final class UniswapV3CaptureMain {
    private static final int POOL_FIELDS = 10;
    private static final String SWAP_TOPIC =
            "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";
    private static final byte[] TOKEN_0_CALL = {0x0d, (byte) 0xfe, 0x16, (byte) 0x81};
    private static final Logger LOGGER = LoggerFactory.getLogger(UniswapV3CaptureMain.class);

    private UniswapV3CaptureMain() {}

    /**
     * Runs a bounded Uniswap v3 capture using runtime RPC endpoint options.
     *
     * @param args public capture and pool options; active/passive RPC URLs come from matching
     *     CLI options or {@code QTSTREAMX_EVM_ACTIVE_*}/{@code QTSTREAMX_EVM_PASSIVE_*}
     *     environment variables
     * @throws Exception if configuration, connection, capture, or output fails
     */
    public static void main(String[] args) throws Exception {
        Arguments options = Arguments.parse(args);
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
        int timeoutSeconds = options.intValue("timeout-seconds", 15);
        if (timeoutSeconds <= 0) {
            throw new IllegalArgumentException("timeout-seconds must be positive");
        }
        long startBlock = Long.parseLong(options.requireOne("start-block"));
        if (startBlock < 0) {
            throw new IllegalArgumentException("start-block must be non-negative");
        }
        EvmLogStreamId streamId = new EvmLogStreamId(
                network, options.requireOne("stream-key"));
        int confirmations = options.intValue("confirmations", 2);
        int maxBlockRange = options.intValue("max-block-range", 2_000);
        int retries = options.intValue("retries", 3);
        int overlapBlocks = options.intValue("overlap-blocks", 2);
        long maxReplayBlocks = options.longValue("max-replay-blocks", 10_000);
        long maximumProviderLagBlocks = options.longValue("max-provider-lag-blocks", 2);
        int durationSeconds = options.intValue("duration-seconds", 300);
        if (durationSeconds <= 0) {
            throw new IllegalArgumentException("duration-seconds must be positive");
        }
        List<UniswapV3Pool> pools = options.all("pool").stream()
                .map(descriptor -> parsePool(network, descriptor))
                .toList();
        if (pools.isEmpty()) {
            throw new IllegalArgumentException("at least one --pool is required");
        }
        CandleInterval interval = new CandleInterval(
                options.one("interval-name", "1m"),
                options.longValue("interval-micros", 60_000_000L));
        Path outputDirectory = Path.of(options.one("out", "/tmp/canary/uniswap-v3"));
        Path checkpointDirectory = Path.of(options.one(
                "checkpoint-dir", outputDirectory.resolve("checkpoints").toString()));
        EvmLogFilter filter = new EvmLogFilter(
                pools.stream().map(UniswapV3Pool::address).collect(Collectors.toUnmodifiableSet()),
                Set.of(SWAP_TOPIC));
        ActivePassiveEvmLogStream logStream = ActivePassiveEvmCaptureRoute.create(
                endpoints,
                streamId,
                checkpointDirectory,
                filter,
                pools.getFirst().address(),
                SWAP_TOPIC,
                TOKEN_0_CALL,
                new ActivePassiveEvmCaptureRoute.Config(
                        network,
                        startBlock,
                        confirmations,
                        maxBlockRange,
                        Duration.ofSeconds(timeoutSeconds),
                        retries,
                        overlapBlocks,
                        maxReplayBlocks,
                        maximumProviderLagBlocks));

        LOGGER.info(
                "Uniswap v3 capture starting: network={} pools={} startBlock={} confirmations={} duration={}s out={}",
                network,
                pools.size(),
                startBlock,
                confirmations,
                durationSeconds,
                outputDirectory);
        try (DexCaptureSession session = new DexCaptureSession(
                new UniswapV3MarketDataStream(
                        logStream, pools),
                interval,
                outputDirectory)) {
            session.startRecoverable();
            long endMillis = Math.addExact(
                    System.currentTimeMillis(), Math.multiplyExact(durationSeconds, 1_000L));
            while (System.currentTimeMillis() < endMillis) {
                long remaining = endMillis - System.currentTimeMillis();
                Thread.sleep(Math.min(1_000L, Math.max(1L, remaining)));
                session.advanceWatermark(System.currentTimeMillis() * 1_000L);
            }
            LOGGER.info(
                    "Uniswap v3 capture complete: connected={} trades={} supervisor={}",
                    session.isConnected(),
                    session.tradeCount(),
                    logStream.metrics());
        }
    }

    static UniswapV3Pool parsePool(String network, String descriptor) {
        String[] fields = descriptor.split("\\|", -1);
        if (fields.length != POOL_FIELDS) {
            throw new IllegalArgumentException(
                    "pool descriptor must contain " + POOL_FIELDS + " pipe-delimited fields");
        }
        return new UniswapV3Pool(
                network,
                fields[0],
                new EvmToken(fields[1], fields[2], Integer.parseInt(fields[3])),
                new EvmToken(fields[4], fields[5], Integer.parseInt(fields[6])),
                new Instrument(fields[7], fields[8]),
                Integer.parseInt(fields[9]));
    }

    private static String runtimeEndpoint(Arguments options, String option, String environment) {
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

        private java.util.Optional<String> optionalOne(String name) {
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

        private int intValue(String name, int defaultValue) {
            return Integer.parseInt(one(name, Integer.toString(defaultValue)));
        }

        private long longValue(String name, long defaultValue) {
            return Long.parseLong(one(name, Long.toString(defaultValue)));
        }
    }
}
