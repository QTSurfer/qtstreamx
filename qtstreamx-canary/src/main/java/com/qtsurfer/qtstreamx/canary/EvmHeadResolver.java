package com.qtsurfer.qtstreamx.canary;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/** Resolves a recent EVM head for bounded canary lookback without exposing the endpoint. */
final class EvmHeadResolver {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String REQUEST =
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_blockNumber\",\"params\":[]}";

    private EvmHeadResolver() {}

    static long latestBlock(URI endpoint, Duration timeout) {
        try {
            HttpRequest request = HttpRequest.newBuilder(endpoint)
                    .timeout(timeout)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(REQUEST))
                    .build();
            HttpResponse<String> response = HttpClient.newHttpClient()
                    .send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IllegalStateException("EVM head request returned HTTP " + response.statusCode());
            }
            JsonNode body = MAPPER.readTree(response.body());
            if (body.hasNonNull("error")) {
                throw new IllegalStateException(
                        "EVM head request returned JSON-RPC error "
                                + body.path("error").path("code").asInt());
            }
            String result = body.path("result").asText();
            return Long.parseUnsignedLong(result.substring(2), 16);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("EVM head request interrupted");
        } catch (RuntimeException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new IllegalStateException("EVM head request failed");
        }
    }
}
