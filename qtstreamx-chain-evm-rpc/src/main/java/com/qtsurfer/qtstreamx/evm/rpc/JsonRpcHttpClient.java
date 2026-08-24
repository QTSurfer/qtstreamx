package com.qtsurfer.qtstreamx.evm.rpc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

final class JsonRpcHttpClient implements EvmRpcHttpClient, EvmRpcProbeHttpClient {
    private static final Duration INITIAL_RETRY_DELAY = Duration.ofMillis(100);
    private static final Duration MAX_RETRY_DELAY = Duration.ofSeconds(5);

    private final EvmRpcRequestConfig config;
    private final JsonRpcHttpTransport transport;
    private final RetryDelay retryDelay;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AtomicLong requestIds = new AtomicLong();

    JsonRpcHttpClient(EvmRpcRequestConfig config, JsonRpcHttpTransport transport) {
        this(config, transport, Thread::sleep);
    }

    JsonRpcHttpClient(
            EvmRpcRequestConfig config,
            JsonRpcHttpTransport transport,
            RetryDelay retryDelay) {
        this.config = Objects.requireNonNull(config, "config");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.retryDelay = Objects.requireNonNull(retryDelay, "retryDelay");
    }

    @Override
    public long latestBlockNumber() {
        JsonNode result = request("eth_blockNumber", "[]");
        return parseHexLong(result.asText());
    }

    @Override
    public long chainId() {
        JsonNode result = request("eth_chainId", "[]");
        return parseHexLong(result.asText());
    }

    @Override
    public EvmBlock getBlock(long number) {
        return getBlock(EvmBlockTag.number(number));
    }

    @Override
    public EvmBlock getBlock(EvmBlockTag blockTag) {
        JsonNode result = request(
                "eth_getBlockByNumber",
                "[\"" + blockTag.rpcValue() + "\",false]");
        return new EvmBlock(
                parseHexLong(result.path("number").asText()),
                result.path("hash").asText(),
                Math.multiplyExact(parseHexLong(result.path("timestamp").asText()), 1_000_000L));
    }

    @Override
    public List<EvmRpcLog> getLogs(EvmLogFilter filter, long fromBlock, long toBlock) {
        return getLogs(filter, fromBlock, toBlock, Integer.MAX_VALUE);
    }

    @Override
    public List<EvmRpcLog> getLogs(
            EvmLogFilter filter,
            long fromBlock,
            long toBlock,
            int maxResults) {
        if (maxResults <= 0) {
            throw new IllegalArgumentException("maxResults must be positive");
        }
        ObjectNode query = objectMapper.createObjectNode();
        query.put("fromBlock", toHex(fromBlock));
        query.put("toBlock", toHex(toBlock));
        ArrayNode addresses = query.putArray("address");
        filter.addresses().stream().sorted().forEach(addresses::add);
        ArrayNode topicOptions = query.putArray("topics").addArray();
        filter.eventTopics().stream().sorted().forEach(topicOptions::add);
        ArrayNode params = objectMapper.createArrayNode().add(query);

        List<EvmRpcLog> logs = new ArrayList<>();
        for (JsonNode result : request("eth_getLogs", params.toString())) {
            if (logs.size() >= maxResults) {
                throw new EvmRpcResultLimitException();
            }
            logs.add(parseLog(result));
        }
        return List.copyOf(logs);
    }

    public byte[] call(String contractAddress, byte[] data, EvmBlockTag blockTag) {
        ObjectNode call = objectMapper.createObjectNode();
        call.put("to", contractAddress);
        call.put("data", "0x" + HexFormat.of().formatHex(data));
        ArrayNode params = objectMapper.createArrayNode()
                .add(call)
                .add(blockTag.rpcValue());
        return parseHexBytes(request("eth_call", params.toString()).asText(), "eth_call");
    }

    @Override
    public byte[] code(String contractAddress, EvmBlockTag blockTag) {
        ArrayNode params = objectMapper.createArrayNode()
                .add(contractAddress)
                .add(blockTag.rpcValue());
        return parseHexBytes(request("eth_getCode", params.toString()).asText(), "eth_getCode");
    }

    private static byte[] parseHexBytes(String result, String method) {
        if (!result.startsWith("0x") || (result.length() - 2) % 2 != 0) {
            throw new IllegalStateException("Invalid " + method + " result");
        }
        try {
            return HexFormat.of().parseHex(result.substring(2));
        } catch (IllegalArgumentException exception) {
            throw new IllegalStateException("Invalid " + method + " result", exception);
        }
    }

    private JsonNode request(String method, String params) {
        long id = requestIds.incrementAndGet();
        String payload = "{\"jsonrpc\":\"2.0\",\"id\":" + id
                + ",\"method\":\"" + method + "\",\"params\":" + params + "}";
        try {
            JsonNode response = objectMapper.readTree(postWithRetry(payload));
            if (response.hasNonNull("error")) {
                JsonNode error = response.path("error");
                throw new EvmRpcException(error.path("code").asInt());
            }
            return response.path("result");
        } catch (IOException exception) {
            throw new IllegalStateException("Invalid JSON-RPC response", exception);
        }
    }

    private String postWithRetry(String payload) {
        Duration delay = INITIAL_RETRY_DELAY;
        int retries = 0;
        while (true) {
            try {
                return transport.post(payload, config.requestTimeout());
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("JSON-RPC request interrupted", exception);
            } catch (IOException exception) {
                if (retries >= config.maxRetries()) {
                    throw new IllegalStateException("JSON-RPC request failed after retries", exception);
                }
                retries++;
                awaitRetry(delay);
                delay = delay.multipliedBy(2).compareTo(MAX_RETRY_DELAY) > 0
                        ? MAX_RETRY_DELAY
                        : delay.multipliedBy(2);
            }
        }
    }

    private void awaitRetry(Duration delay) {
        try {
            retryDelay.await(delay);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("JSON-RPC retry interrupted", exception);
        }
    }

    private static long parseHexLong(String value) {
        return Long.parseUnsignedLong(value.substring(2), 16);
    }

    private static int parseHexInt(String value) {
        return Integer.parseUnsignedInt(value.substring(2), 16);
    }

    private static String toHex(long value) {
        return "0x" + Long.toHexString(value);
    }

    private static EvmRpcLog parseLog(JsonNode result) {
        List<String> topics = new ArrayList<>();
        result.path("topics").forEach(topic -> topics.add(topic.asText()));
        return new EvmRpcLog(
                result.path("address").asText(),
                topics,
                result.path("data").asText(),
                parseHexLong(result.path("blockNumber").asText()),
                result.path("blockHash").asText(),
                result.path("transactionHash").asText(),
                parseHexInt(result.path("transactionIndex").asText()),
                parseHexInt(result.path("logIndex").asText()),
                result.path("removed").asBoolean());
    }
}
