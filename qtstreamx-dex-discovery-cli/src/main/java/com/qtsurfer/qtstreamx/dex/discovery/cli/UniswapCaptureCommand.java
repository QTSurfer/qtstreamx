package com.qtsurfer.qtstreamx.dex.discovery.cli;

import com.qtsurfer.qtstreamx.evm.rpc.ActivePassiveEvmCaptureRoute;
import com.qtsurfer.qtstreamx.evm.rpc.EvmProviderEndpoint;
import com.qtsurfer.qtstreamx.core.client.RecoverableMarketTradeStream;
import com.qtsurfer.qtstreamx.core.model.MarketId;
import com.qtsurfer.qtstreamx.dex.capture.csv.CsvMarketTradeSink;
import com.qtsurfer.qtstreamx.dex.uniswap.v2.KnownUniswapV2Markets;
import com.qtsurfer.qtstreamx.dex.uniswap.v2.UniswapV2MarketDataStream;
import com.qtsurfer.qtstreamx.dex.uniswap.v2.UniswapV2Pair;
import com.qtsurfer.qtstreamx.dex.uniswap.v3.KnownUniswapV3Markets;
import com.qtsurfer.qtstreamx.dex.uniswap.v3.UniswapV3MarketDataStream;
import com.qtsurfer.qtstreamx.dex.uniswap.v3.UniswapV3Pool;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogFilter;
import com.qtsurfer.qtstreamx.evm.rpc.EvmBlockTimestampResolver;
import com.qtsurfer.qtstreamx.evm.rpc.EvmHttpRpcReader;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcReaderConfig;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogStream;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogStreamId;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Executes one bounded, reviewed-contract CSV capture with active/passive providers. */
final class UniswapCaptureCommand {
    private static final String V2_SWAP_TOPIC =
            "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822";
    private static final String V3_SWAP_TOPIC =
            "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";
    private static final byte[] TOKEN_0_CALL = {0x0d, (byte) 0xfe, 0x16, (byte) 0x81};

    CliResponse capture(CliRequest request, EndpointArguments endpoints) {
        String network = UniswapCliCatalog.resolveNetwork(request.requiredOption("network"));
        String version = request.requiredOption("version").toLowerCase(Locale.ROOT);
        int durationSeconds = request.intOption("duration-seconds", 300);
        if (durationSeconds <= 0) {
            throw new IllegalArgumentException("--duration-seconds must be positive");
        }
        Path output = Path.of(request.requiredOption("out"));
        Path checkpoints = request.option("checkpoint-dir")
                .map(Path::of)
                .orElseGet(() -> output.resolveSibling(output.getFileName() + ".checkpoints"));
        try {
            CaptureMarket market = market(network, version, request.requiredAddressArgument());
            endpoints.requireCaptureProviders();
            List<EvmProviderEndpoint> providers = providers(endpoints);
            long startBlock = startBlock(request, endpoints, network);
            EvmLogStreamId streamId = new EvmLogStreamId(
                    network, "uniswap-capture-" + market.version + "-" + market.contract.substring(2));
            EvmLogStream logs = ActivePassiveEvmCaptureRoute.create(
                    providers,
                    streamId,
                    checkpoints,
                    new EvmLogFilter(Set.of(market.contract), Set.of(market.swapTopic)),
                    market.contract,
                    market.swapTopic,
                    TOKEN_0_CALL,
                    config(network, startBlock, request));
            try (CsvMarketTradeSink sink = new CsvMarketTradeSink(output, market.marketId);
                    RecoverableMarketTradeStream stream = market.stream(logs)) {
                stream.startRecoverable(sink);
                Thread.sleep(Math.multiplyExact((long) durationSeconds, 1_000L));
            }
            return CliResponse.ok(request, Map.of(
                    "event_file", output.toAbsolutePath().toString(),
                    "metadata_file", output.toAbsolutePath().resolveSibling(output.getFileName() + ".metadata.csv").toString(),
                    "contract", market.contract,
                    "version", market.version));
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("capture interrupted", exception);
        } catch (Exception exception) {
            if (exception instanceof IllegalArgumentException illegalArgumentException) {
                throw illegalArgumentException;
            }
            throw new IllegalStateException("capture could not start or persist safely", exception);
        }
    }

    static String preflightProviders(CliRequest request, EndpointArguments endpoints) {
        String network = UniswapCliCatalog.resolveNetwork(request.requiredOption("network"));
        String version = request.requiredOption("version").toLowerCase(Locale.ROOT);
        market(network, version, request.requiredAddressArgument());
        endpoints.requireCaptureProviders();
        return endpoints.captureProvidersMessage();
    }

    private static long startBlock(CliRequest request, EndpointArguments endpoints, String network) {
        boolean block = request.option("start-block").isPresent();
        boolean date = request.option("start-date").isPresent();
        if (block == date) throw new IllegalArgumentException("exactly one of --start-block or --start-date is required");
        if (block) {
            long value = request.requiredLongOption("start-block");
            if (value < 0) throw new IllegalArgumentException("--start-block must be non-negative");
            return value;
        }
        String value = request.requiredOption("start-date");
        if (!value.endsWith("Z")) throw new IllegalArgumentException("--start-date must be an ISO-8601 UTC instant ending in Z");
        Instant instant;
        try { instant = Instant.parse(value); }
        catch (java.time.format.DateTimeParseException exception) { throw new IllegalArgumentException("--start-date must be an ISO-8601 UTC instant ending in Z"); }
        return new EvmBlockTimestampResolver(new EvmHttpRpcReader(new EvmRpcReaderConfig(
                network, endpoints.requireHttpUrl(),
                request.intOption("max-block-range", 2_000), Duration.ofSeconds(request.intOption("timeout-seconds", 15)), request.intOption("retries", 3))))
                .firstBlockAtOrAfter(instant.getEpochSecond());
    }

    private static List<EvmProviderEndpoint> providers(EndpointArguments endpoints) {
        return List.of(
                new EvmProviderEndpoint("active",
                        endpoints.httpUrl().orElseThrow(() -> new IllegalArgumentException(
                                "--http-url or QTSTREAMX_EVM_HTTP_URL is required")),
                        endpoints.wsUrl().orElseThrow(() -> new IllegalArgumentException(
                                "--ws-url or QTSTREAMX_EVM_WS_URL is required"))),
                new EvmProviderEndpoint("passive",
                        endpoints.passiveHttpUrl().orElseThrow(() -> new IllegalArgumentException(
                                "--passive-http-url or QTSTREAMX_EVM_PASSIVE_HTTP_URL is required")),
                        endpoints.passiveWsUrl().orElseThrow(() -> new IllegalArgumentException(
                                "--passive-ws-url or QTSTREAMX_EVM_PASSIVE_WS_URL is required"))));
    }

    private static ActivePassiveEvmCaptureRoute.Config config(
            String network, long startBlock, CliRequest request) {
        int timeout = request.intOption("timeout-seconds", 15);
        if (timeout <= 0) throw new IllegalArgumentException("--timeout-seconds must be positive");
        return new ActivePassiveEvmCaptureRoute.Config(
                network, startBlock, request.intOption("confirmations", 2),
                request.intOption("max-block-range", 2_000), Duration.ofSeconds(timeout),
                request.intOption("retries", 3), request.intOption("overlap-blocks", 2),
                request.intOption("max-replay-blocks", 10_000),
                request.intOption("max-provider-lag-blocks", 2));
    }

    private static CaptureMarket market(String network, String version, String address) {
        return switch (version) {
            case "v2" -> KnownUniswapV2Markets.all().stream()
                    .filter(pair -> pair.network().equals(network) && pair.address().equalsIgnoreCase(address))
                    .findFirst().map(CaptureMarket::v2)
                    .orElseThrow(() -> new IllegalArgumentException("contract is not a reviewed Uniswap v2 pair"));
            case "v3" -> KnownUniswapV3Markets.all().stream()
                    .filter(pool -> pool.network().equals(network) && pool.address().equalsIgnoreCase(address))
                    .findFirst().map(CaptureMarket::v3)
                    .orElseThrow(() -> new IllegalArgumentException("contract is not a reviewed Uniswap v3 pool"));
            default -> throw new IllegalArgumentException("--version must be v2 or v3");
        };
    }

    private record CaptureMarket(String version, String contract, String swapTopic, MarketId marketId,
                                 StreamFactory streamFactory) {
        static CaptureMarket v2(UniswapV2Pair pair) {
            return new CaptureMarket("v2", pair.address(), V2_SWAP_TOPIC,
                    new MarketId("uniswap-v2", pair.network(), pair.address(), pair.instrument()),
                    logs -> new UniswapV2MarketDataStream(logs, List.of(pair)));
        }
        static CaptureMarket v3(UniswapV3Pool pool) {
            return new CaptureMarket("v3", pool.address(), V3_SWAP_TOPIC,
                    new MarketId("uniswap-v3", pool.network(), pool.address(), pool.instrument()),
                    logs -> new UniswapV3MarketDataStream(logs, List.of(pool)));
        }
        RecoverableMarketTradeStream stream(EvmLogStream logs) { return streamFactory.create(logs); }
    }

    @FunctionalInterface
    private interface StreamFactory { RecoverableMarketTradeStream create(EvmLogStream logs); }
}
