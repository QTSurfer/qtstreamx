package com.qtsurfer.qtstreamx.canary;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.qtsurfer.qtstreamx.aggregation.CandleInterval;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.AddressBasedUniswapPairOrientation;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapDiscoveryLimits;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapDiscoveryPolicy;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapFactoryScan;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapNetworkTokenPolicy;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapV2MarketDiscovery;
import com.qtsurfer.qtstreamx.dex.uniswap.v2.UniswapV2MarketDataStream;
import com.qtsurfer.qtstreamx.dex.uniswap.v2.UniswapV2Pair;
import com.qtsurfer.qtstreamx.evm.rpc.EvmBlockTag;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLog;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogFilter;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogStream;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcLog;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcReader;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class UniswapV2DiscoveryCaptureTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String NETWORK = "eip155:1";
    private static final String FACTORY = "0x00000000000000000000000000000000000000f0";
    private static final String WETH = "0x0000000000000000000000000000000000000001";
    private static final String USDC = "0x0000000000000000000000000000000000000002";
    private static final String UNTRUSTED = "0x0000000000000000000000000000000000000003";
    private static final String PAIR = "0x00000000000000000000000000000000000000aa";
    private static final String SECOND_PAIR = "0x00000000000000000000000000000000000000ac";
    private static final String REJECTED_PAIR = "0x00000000000000000000000000000000000000ab";
    private static final String PAIR_CREATED_TOPIC =
            "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9";
    private static final String SWAP_TOPIC =
            "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822";

    @TempDir
    Path outputDirectory;

    @Test
    void discoversSelectedPairAndWritesOneSecondMarketArtifacts() throws Exception {
        DiscoveryReader reader = new DiscoveryReader();
        DiscoveryCaptureDiagnostics diagnostics = new DiscoveryCaptureDiagnostics();
        UniswapDiscoveryPolicy policy = new UniswapDiscoveryPolicy(
                new AddressBasedUniswapPairOrientation(Map.of(
                        NETWORK,
                        new UniswapNetworkTokenPolicy(Set.of(USDC), Set.of(WETH)))),
                new UniswapDiscoveryLimits(10, 4, 3, 2),
                OptionalLong.empty());
        UniswapV2MarketDiscovery discovery = new UniswapV2MarketDiscovery(
                new UniswapFactoryScan(NETWORK, FACTORY, 100),
                reader,
                policy,
                diagnostics);

        Set<UniswapV2Pair> selected = discovery.refresh(100).toCompletableFuture().join();
        RecordingLogStream stream = new RecordingLogStream();
        try (DexCaptureSession session = new DexCaptureSession(
                new UniswapV2MarketDataStream(stream, selected),
                new CandleInterval("1s", 1_000_000),
                outputDirectory,
                diagnostics.report(selected.size()))) {
            session.start();
            stream.emit(swapLog(PAIR, "0xblock-a", "0xswap-a", 0));
            stream.emit(swapLog(SECOND_PAIR, "0xblock-b", "0xswap-b", 1));
        }

        JsonNode summary = MAPPER.readTree(outputDirectory.resolve("summary.json").toFile());
        assertThat(selected).extracting(UniswapV2Pair::address)
                .containsExactly(PAIR, SECOND_PAIR);
        assertThat(reader.calls).hasSize(4);
        assertThat(summary.path("discovery").path("discovered").asInt()).isEqualTo(3);
        assertThat(summary.path("discovery").path("selected").asInt()).isEqualTo(2);
        assertThat(summary.path("discovery").path("rejected").asInt()).isEqualTo(1);
        assertThat(summary.path("discovery").path("reasons").path("ORIENTATION").asInt())
                .isEqualTo(1);
        List<String> markets = new ArrayList<>();
        summary.path("markets").forEach(node -> markets.add(node.path("nativeId").asText()));
        assertThat(markets).containsExactly(PAIR, SECOND_PAIR);
        assertThat(summary.path("trades").asInt()).isEqualTo(2);
        assertThat(summary.path("tickers").asInt()).isEqualTo(2);
        assertThat(summary.path("klines").asInt()).isEqualTo(4);
    }

    private static EvmLog swapLog(
            String pair,
            String blockHash,
            String transactionHash,
            int logIndex) {
        String data = "0x"
                + word(new BigInteger("2000000000"))
                + word(BigInteger.ZERO)
                + word(BigInteger.ZERO)
                + word(new BigInteger("1000000000000000000"));
        return new EvmLog(
                NETWORK,
                pair,
                List.of(SWAP_TOPIC, "0xsender", "0xto"),
                data,
                101,
                blockHash,
                transactionHash,
                0,
                logIndex,
                1_200_001L);
    }

    private static EvmRpcLog pairCreated(
            String token0, String token1, String pair, String transactionHash) {
        return new EvmRpcLog(
                FACTORY,
                List.of(PAIR_CREATED_TOPIC, addressWord(token0), addressWord(token1)),
                "0x" + addressWord(pair).substring(2) + word(BigInteger.ONE),
                100,
                "0xfactory-block",
                transactionHash,
                0,
                0,
                false);
    }

    private static String addressWord(String address) {
        return "0x" + "0".repeat(24) + address.substring(2);
    }

    private static String word(BigInteger value) {
        return "%064x".formatted(value);
    }

    private static byte[] dynamicString(String value) {
        byte[] text = value.getBytes(StandardCharsets.UTF_8);
        int paddedLength = (text.length + 31) / 32 * 32;
        return HexFormat.of().parseHex(
                word(BigInteger.valueOf(32))
                        + word(BigInteger.valueOf(text.length))
                        + HexFormat.of().formatHex(text)
                        + "00".repeat(paddedLength - text.length));
    }

    private static final class DiscoveryReader implements EvmRpcReader {
        private final List<String> calls = new ArrayList<>();

        @Override
        public long latestBlockNumber() {
            return 100;
        }

        @Override
        public List<EvmRpcLog> logs(EvmLogFilter filter, long fromBlock, long toBlock) {
            return List.of(
                    pairCreated(WETH, USDC, PAIR, "0xselected"),
                    pairCreated(WETH, USDC, SECOND_PAIR, "0xselected-2"),
                    pairCreated(WETH, UNTRUSTED, REJECTED_PAIR, "0xrejected"));
        }

        @Override
        public byte[] call(String contractAddress, byte[] data, EvmBlockTag blockTag) {
            String selector = HexFormat.of().formatHex(data);
            calls.add(contractAddress + ":" + selector);
            return switch (selector) {
                case "95d89b41" -> dynamicString(contractAddress.equals(WETH) ? "WETH" : "USDC");
                case "313ce567" -> HexFormat.of().parseHex(
                        word(BigInteger.valueOf(contractAddress.equals(WETH) ? 18 : 6)));
                default -> throw new AssertionError("unexpected selector");
            };
        }
    }

    private static final class RecordingLogStream implements EvmLogStream {
        private Consumer<EvmLog> handler;

        @Override
        public void start(EvmLogFilter filter, Consumer<EvmLog> handler) {
            this.handler = handler;
        }

        @Override
        public void onError(Consumer<Throwable> handler) {}

        @Override
        public boolean isConnected() {
            return true;
        }

        @Override
        public void close() {}

        private void emit(EvmLog log) {
            handler.accept(log);
        }
    }
}
