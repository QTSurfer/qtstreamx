package com.qtsurfer.qtstreamx.canary;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.qtsurfer.qtstreamx.aggregation.CandleInterval;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.dex.core.EvmToken;
import com.qtsurfer.qtstreamx.dex.uniswap.v2.UniswapV2MarketDataStream;
import com.qtsurfer.qtstreamx.dex.uniswap.v2.UniswapV2Pair;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLog;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogFilter;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class UniswapV2CaptureSessionTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String SWAP_TOPIC =
            "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822";
    private static final String PAIR_ADDRESS =
            "0x00000000000000000000000000000000000000ab";

    @TempDir
    Path outputDirectory;

    @Test
    void writesTradeTickerLiveAndClosedKlineArtifacts() throws Exception {
        RecordingLogStream source = new RecordingLogStream();
        UniswapV2Pair pair = pair(PAIR_ADDRESS);

        try (DexCaptureSession session = new DexCaptureSession(
                new UniswapV2MarketDataStream(source, List.of(pair)),
                CandleInterval.ONE_MINUTE,
                outputDirectory)) {
            session.start();
            source.emit(swapLog(PAIR_ADDRESS, "0xblock", "0xtx", 7, 120_000_001L));
        }

        List<JsonNode> trades = lines("trades.ndjson");
        List<JsonNode> tickers = lines("tickers.ndjson");
        List<JsonNode> klines = lines("klines.ndjson");
        JsonNode summary = MAPPER.readTree(outputDirectory.resolve("summary.json").toFile());

        assertThat(trades).singleElement().satisfies(trade -> {
            assertThat(trade.path("market").path("venue").asText()).isEqualTo("uniswap-v2");
            assertThat(trade.path("market").path("nativeId").asText()).isEqualTo(PAIR_ADDRESS);
            assertThat(trade.path("price").decimalValue()).isEqualByComparingTo("2000");
            assertThat(trade.path("eventId").asText()).isEqualTo("eip155:1:0xblock:0xtx:7");
        });
        assertThat(tickers).hasSize(1);
        assertThat(klines).hasSize(2);
        assertThat(klines.getFirst().path("kline").path("closed").asBoolean()).isFalse();
        assertThat(klines.getLast().path("kline").path("closed").asBoolean()).isTrue();
        assertThat(summary.path("trades").asLong()).isEqualTo(1);
        assertThat(summary.path("tickers").asLong()).isEqualTo(1);
        assertThat(summary.path("klines").asLong()).isEqualTo(2);
    }

    @Test
    void preservesTwoSameInstrumentPairsInEveryOutput() throws Exception {
        String otherPairAddress = "0x00000000000000000000000000000000000000ac";
        RecordingLogStream source = new RecordingLogStream();
        UniswapV2Pair first = pair(PAIR_ADDRESS);
        UniswapV2Pair second = pair(otherPairAddress);

        try (DexCaptureSession session = new DexCaptureSession(
                new UniswapV2MarketDataStream(source, List.of(first, second)),
                CandleInterval.ONE_MINUTE,
                outputDirectory)) {
            session.start();
            source.emit(swapLog(PAIR_ADDRESS, "0xblock-a", "0xtx-a", 1, 120_000_001L));
            source.emit(swapLog(otherPairAddress, "0xblock-b", "0xtx-b", 2, 120_000_002L));
        }

        assertThat(lines("trades.ndjson"))
                .extracting(line -> line.path("market").path("nativeId").asText())
                .containsExactly(PAIR_ADDRESS, otherPairAddress);
        assertThat(lines("tickers.ndjson"))
                .extracting(line -> line.path("market").path("nativeId").asText())
                .containsExactly(PAIR_ADDRESS, otherPairAddress);
        assertThat(lines("klines.ndjson"))
                .extracting(line -> line.path("market").path("nativeId").asText())
                .containsOnly(PAIR_ADDRESS, otherPairAddress);
    }

    @Test
    void neverWritesEndpointOrErrorMessageSecrets() throws Exception {
        String secretEndpoint = "https://example.invalid/redacted";
        RecordingLogStream source = new RecordingLogStream();

        try (DexCaptureSession session = new DexCaptureSession(
                new UniswapV2MarketDataStream(source, List.of(pair(PAIR_ADDRESS))),
                CandleInterval.ONE_MINUTE,
                outputDirectory)) {
            session.start();
            source.fail(new IllegalStateException("provider rejected " + secretEndpoint));
        }

        String allOutput;
        try (var paths = Files.walk(outputDirectory)) {
            allOutput = paths.filter(Files::isRegularFile)
                    .map(path -> {
                        try {
                            return Files.readString(path);
                        } catch (Exception exception) {
                            throw new IllegalStateException(exception);
                        }
                    })
                    .reduce("", String::concat);
        }
        assertThat(allOutput)
                .doesNotContain("alice", "secret", "rpc.invalid", "apiKey", "top-secret");
        assertThat(lines("diagnostics.ndjson"))
                .anySatisfy(line -> assertThat(line.path("errorType").asText())
                        .isEqualTo(IllegalStateException.class.getName()));
    }

    private List<JsonNode> lines(String fileName) throws Exception {
        try (var lines = Files.lines(outputDirectory.resolve(fileName))) {
            return lines.map(line -> {
                try {
                    return MAPPER.readTree(line);
                } catch (Exception exception) {
                    throw new IllegalStateException(exception);
                }
            }).toList();
        }
    }

    private static UniswapV2Pair pair(String pairAddress) {
        return new UniswapV2Pair(
                "eip155:1",
                pairAddress,
                new EvmToken("USDC", "0x0000000000000000000000000000000000000001", 6),
                new EvmToken("WETH", "0x0000000000000000000000000000000000000002", 18),
                new Instrument("WETH", "USDC"));
    }

    private static EvmLog swapLog(
            String pairAddress,
            String blockHash,
            String transactionHash,
            int logIndex,
            long timestamp) {
        String data = "0x"
                + word(new BigInteger("2000000000"))
                + word(BigInteger.ZERO)
                + word(BigInteger.ZERO)
                + word(new BigInteger("1000000000000000000"));
        return new EvmLog(
                "eip155:1",
                pairAddress,
                List.of(SWAP_TOPIC, "0xsender", "0xto"),
                data,
                100,
                blockHash,
                transactionHash,
                0,
                logIndex,
                timestamp);
    }

    private static String word(BigInteger value) {
        return "%064x".formatted(value);
    }

    private static final class RecordingLogStream implements EvmLogStream {
        private Consumer<EvmLog> handler;
        private Consumer<Throwable> errorHandler;

        @Override
        public void start(EvmLogFilter filter, Consumer<EvmLog> handler) {
            this.handler = handler;
        }

        @Override
        public void onError(Consumer<Throwable> handler) {
            errorHandler = handler;
        }

        @Override
        public boolean isConnected() {
            return true;
        }

        @Override
        public void close() {}

        private void emit(EvmLog log) {
            handler.accept(log);
        }

        private void fail(Throwable error) {
            errorHandler.accept(error);
        }
    }
}
