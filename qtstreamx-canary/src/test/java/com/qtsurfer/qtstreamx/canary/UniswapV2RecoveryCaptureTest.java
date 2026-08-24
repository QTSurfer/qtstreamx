package com.qtsurfer.qtstreamx.canary;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.qtsurfer.qtstreamx.aggregation.CandleInterval;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.ws.WebSocketClient;
import com.qtsurfer.qtstreamx.dex.core.EvmToken;
import com.qtsurfer.qtstreamx.dex.uniswap.v2.UniswapV2MarketDataStream;
import com.qtsurfer.qtstreamx.dex.uniswap.v2.UniswapV2Pair;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogStreamConfig;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcLogStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class UniswapV2RecoveryCaptureTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String SWAP_TOPIC =
            "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822";
    private static final String PAIR_ADDRESS =
            "0x00000000000000000000000000000000000000ab";

    @TempDir
    Path outputDirectory;

    @Test
    void recoversDisconnectGapWithoutMissingOrDuplicateTrades() throws Exception {
        try (RpcFixtureServer rpc = new RpcFixtureServer()) {
            RecordingWebSocket firstSocket = new RecordingWebSocket();
            RecordingWebSocket secondSocket = new RecordingWebSocket();
            Deque<WebSocketClient> sockets = new ArrayDeque<>(List.of(firstSocket, secondSocket));
            EvmLogStreamConfig config = new EvmLogStreamConfig(
                    "eip155:1",
                    "wss://rpc.invalid/ws",
                    rpc.url(),
                    100,
                    2,
                    2_000,
                    Duration.ofSeconds(5),
                    3);
            UniswapV2Pair pair = new UniswapV2Pair(
                    "eip155:1",
                    PAIR_ADDRESS,
                    new EvmToken(
                            "USDC", "0x0000000000000000000000000000000000000001", 6),
                    new EvmToken(
                            "WETH", "0x0000000000000000000000000000000000000002", 18),
                    new Instrument("WETH", "USDC"));

            try (DexCaptureSession session = new DexCaptureSession(
                    new UniswapV2MarketDataStream(
                            new EvmRpcLogStream(config, sockets::removeFirst), List.of(pair)),
                    CandleInterval.ONE_MINUTE,
                    outputDirectory)) {
                session.start();
                firstSocket.emit(logNotification(100, "0xblock100", "0xtx100", 1));

                rpc.latestBlock = 103;
                rpc.logs = List.of(
                        rpcLog(100, "0xblock100", "0xtx100", 1),
                        rpcLog(101, "0xblock101", "0xtx101", 2));
                firstSocket.disconnect();

                assertThat(secondSocket.isOpen()).isTrue();
            }
        }

        assertThat(readLines(outputDirectory.resolve("trades.ndjson")))
                .extracting(line -> line.path("eventId").asText())
                .containsExactly(
                        "eip155:1:0xblock100:0xtx100:1",
                        "eip155:1:0xblock101:0xtx101:2");
    }

    private static List<JsonNode> readLines(Path path) throws Exception {
        try (var lines = Files.lines(path)) {
            List<JsonNode> result = new ArrayList<>();
            for (String line : lines.toList()) {
                result.add(MAPPER.readTree(line));
            }
            return result;
        }
    }

    private static String logNotification(
            long blockNumber,
            String blockHash,
            String transactionHash,
            int logIndex) {
        return """
                {"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xlogs","result":%s}}
                """.formatted(rpcLog(blockNumber, blockHash, transactionHash, logIndex));
    }

    private static String rpcLog(
            long blockNumber,
            String blockHash,
            String transactionHash,
            int logIndex) {
        String data = "0x"
                + word(new BigInteger("2000000000"))
                + word(BigInteger.ZERO)
                + word(BigInteger.ZERO)
                + word(new BigInteger("1000000000000000000"));
        return """
                {"address":"%s","topics":["%s","0xsender","0xto"],"data":"%s",
                 "removed":false,"blockNumber":"0x%x","blockHash":"%s","transactionHash":"%s",
                 "transactionIndex":"0x0","logIndex":"0x%x"}
                """.formatted(
                PAIR_ADDRESS, SWAP_TOPIC, data, blockNumber, blockHash, transactionHash, logIndex);
    }

    private static String word(BigInteger value) {
        return "%064x".formatted(value);
    }

    private static final class RecordingWebSocket implements WebSocketClient {
        private Consumer<String> messageHandler = ignored -> {};
        private BiConsumer<Integer, String> closeHandler = (code, reason) -> {};
        private boolean open;

        @Override
        public void connect(String url) {
            open = true;
        }

        @Override
        public void send(String message) {}

        @Override
        public void onMessage(Consumer<String> handler) {
            messageHandler = handler;
        }

        @Override
        public void onClose(BiConsumer<Integer, String> handler) {
            closeHandler = handler;
        }

        @Override
        public void onError(Consumer<Throwable> handler) {}

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() {
            open = false;
            closeHandler.accept(1000, "closed");
        }

        private void emit(String message) {
            messageHandler.accept(message);
        }

        private void disconnect() {
            open = false;
            closeHandler.accept(1006, "forced");
        }
    }

    private static final class RpcFixtureServer implements AutoCloseable {
        private final HttpServer server;
        private volatile long latestBlock = 99;
        private volatile List<String> logs = List.of();

        private RpcFixtureServer() throws Exception {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", this::handle);
            server.start();
        }

        private String url() {
            return "http://127.0.0.1:" + server.getAddress().getPort() + "/";
        }

        private void handle(HttpExchange exchange) {
            try (exchange) {
                JsonNode request = MAPPER.readTree(exchange.getRequestBody());
                ObjectNode response = MAPPER.createObjectNode();
                response.put("jsonrpc", "2.0");
                response.set("id", request.path("id"));
                switch (request.path("method").asText()) {
                    case "eth_blockNumber" -> response.put(
                            "result", "0x" + Long.toHexString(latestBlock));
                    case "eth_getLogs" -> {
                        ArrayNode result = response.putArray("result");
                        for (String log : logs) {
                            result.add(MAPPER.readTree(log));
                        }
                    }
                    case "eth_getBlockByNumber" -> {
                        long number = Long.parseUnsignedLong(
                                request.path("params").get(0).asText().substring(2), 16);
                        ObjectNode block = response.putObject("result");
                        block.put("number", "0x" + Long.toHexString(number));
                        block.put("hash", "0xblock" + number);
                        block.put("timestamp", "0x" + Long.toHexString(120 + number - 100));
                    }
                    default -> throw new IllegalArgumentException("unexpected RPC method");
                }
                byte[] body = MAPPER.writeValueAsBytes(response);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
            } catch (Exception exception) {
                throw new IllegalStateException(exception);
            }
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
