package com.qtsurfer.qtstreamx.canary;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.qtsurfer.qtstreamx.aggregation.CandleInterval;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.model.MarketId;
import com.qtsurfer.qtstreamx.dex.core.EvmToken;
import com.qtsurfer.qtstreamx.dex.uniswap.v3.UniswapV3MarketDataStream;
import com.qtsurfer.qtstreamx.dex.uniswap.v3.UniswapV3Pool;
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

class UniswapV3CaptureSessionTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String SWAP_TOPIC =
            "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";
    private static final String POOL_ADDRESS =
            "0x00000000000000000000000000000000000000ab";

    @TempDir
    Path outputDirectory;

    @Test
    void writesTradeTickerLiveAndClosedKlineArtifacts() throws Exception {
        RecordingLogStream source = new RecordingLogStream();
        UniswapV3Pool pool = pool(POOL_ADDRESS, "0x0000000000000000000000000000000000000001",
                "0x0000000000000000000000000000000000000002");

        try (DexCaptureSession session = new DexCaptureSession(
                new UniswapV3MarketDataStream(source, List.of(pool)),
                CandleInterval.ONE_MINUTE,
                outputDirectory)) {
            session.start();
            source.emit(swapLog(POOL_ADDRESS, "0xblock", "0xtx", 7, 120_000_001L));
        }

        List<JsonNode> trades = lines("trades.ndjson");
        List<JsonNode> tickers = lines("tickers.ndjson");
        List<JsonNode> klines = lines("klines.ndjson");
        JsonNode summary = MAPPER.readTree(outputDirectory.resolve("summary.json").toFile());

        assertThat(trades).singleElement().satisfies(trade -> {
            assertThat(trade.path("market").path("nativeId").asText()).isEqualTo(POOL_ADDRESS);
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
    void optionallyWritesPureTradeAndCaptureMetadataCsv() throws Exception {
        RecordingLogStream source = new RecordingLogStream();
        UniswapV3Pool pool = pool(
                POOL_ADDRESS,
                "0x0000000000000000000000000000000000000001",
                "0x0000000000000000000000000000000000000002");
        Path csvOutput = outputDirectory.resolve("capture/events.csv");
        MarketId market = new MarketId("uniswap-v3", pool.network(), pool.address(), pool.instrument());

        try (DexCaptureSession session = new DexCaptureSession(
                new UniswapV3MarketDataStream(source, List.of(pool)),
                CandleInterval.ONE_MINUTE,
                outputDirectory,
                csvOutput,
                market)) {
            session.start();
            source.emit(swapLog(POOL_ADDRESS, "0xblock", "0xtx", 7, 120_000_001L));
        }

        assertThat(Files.readAllLines(csvOutput))
                .containsExactly(
                        "event_id,timestamp_us,price,base_amount,quote_amount,side",
                        "eip155:1:0xblock:0xtx:7,120000001,0.000000000001,1.000000,0.000000000001000000,SELL");
        assertThat(Files.readAllLines(csvOutput.resolveSibling("events.csv.metadata.csv")))
                .containsExactly(
                        "venue,network,contract,instrument,date_from_us,date_to_us",
                        "uniswap-v3,eip155:1," + POOL_ADDRESS + ",BASE/QUOTE,120000001,120000001");
        assertThat(lines("trades.ndjson")).singleElement();
    }

    @Test
    void preservesTwoSameInstrumentPoolsInEveryOutput() throws Exception {
        String otherPoolAddress = "0x00000000000000000000000000000000000000ac";
        RecordingLogStream source = new RecordingLogStream();
        UniswapV3Pool first = pool(
                POOL_ADDRESS,
                "0x0000000000000000000000000000000000000001",
                "0x0000000000000000000000000000000000000002");
        UniswapV3Pool second = pool(
                otherPoolAddress,
                "0x0000000000000000000000000000000000000003",
                "0x0000000000000000000000000000000000000004");

        try (DexCaptureSession session = new DexCaptureSession(
                new UniswapV3MarketDataStream(source, List.of(first, second)),
                CandleInterval.ONE_MINUTE,
                outputDirectory)) {
            session.start();
            source.emit(swapLog(POOL_ADDRESS, "0xblock-a", "0xtx-a", 1, 120_000_001L));
            source.emit(swapLog(otherPoolAddress, "0xblock-b", "0xtx-b", 2, 120_000_002L));
        }

        assertThat(lines("trades.ndjson"))
                .extracting(line -> line.path("market").path("nativeId").asText())
                .containsExactly(POOL_ADDRESS, otherPoolAddress);
        assertThat(lines("tickers.ndjson"))
                .extracting(line -> line.path("market").path("nativeId").asText())
                .containsExactly(POOL_ADDRESS, otherPoolAddress);
        assertThat(lines("klines.ndjson"))
                .extracting(line -> line.path("market").path("nativeId").asText())
                .containsOnly(POOL_ADDRESS, otherPoolAddress);
    }

    @Test
    void neverWritesEndpointOrErrorMessageSecrets() throws Exception {
        String secretEndpoint =
                "https://example.invalid/redacted";
        RecordingLogStream source = new RecordingLogStream();
        UniswapV3Pool pool = pool(
                POOL_ADDRESS,
                "0x0000000000000000000000000000000000000001",
                "0x0000000000000000000000000000000000000002");

        try (DexCaptureSession session = new DexCaptureSession(
                new UniswapV3MarketDataStream(source, List.of(pool)),
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

    private static UniswapV3Pool pool(String poolAddress, String token0Address, String token1Address) {
        return new UniswapV3Pool(
                "eip155:1",
                poolAddress,
                new EvmToken("BASE", token0Address, 6),
                new EvmToken("QUOTE", token1Address, 18),
                new Instrument("BASE", "QUOTE"),
                3_000);
    }

    private static EvmLog swapLog(
            String poolAddress,
            String blockHash,
            String transactionHash,
            int logIndex,
            long timestamp) {
        String data = "0x"
                + signedWord(BigInteger.valueOf(1_000_000))
                + signedWord(BigInteger.valueOf(-1_000_000))
                + unsignedWord(BigInteger.ONE.shiftLeft(96))
                + unsignedWord(BigInteger.ONE)
                + signedWord(BigInteger.ZERO);
        return new EvmLog(
                "eip155:1",
                poolAddress,
                List.of(SWAP_TOPIC, "0xsender", "0xrecipient"),
                data,
                100,
                blockHash,
                transactionHash,
                0,
                logIndex,
                timestamp);
    }

    private static String signedWord(BigInteger value) {
        BigInteger encoded = value.signum() < 0
                ? value.add(BigInteger.ONE.shiftLeft(256))
                : value;
        return unsignedWord(encoded);
    }

    private static String unsignedWord(BigInteger value) {
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
