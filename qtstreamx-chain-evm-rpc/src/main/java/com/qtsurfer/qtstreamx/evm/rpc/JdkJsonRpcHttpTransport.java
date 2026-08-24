package com.qtsurfer.qtstreamx.evm.rpc;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;

final class JdkJsonRpcHttpTransport implements JsonRpcHttpTransport {
    private static final int DEFAULT_MAX_RESPONSE_BYTES = 64 * 1024 * 1024;

    private final URI endpoint;
    private final HttpClient httpClient;
    private final int maxResponseBytes;

    JdkJsonRpcHttpTransport(URI endpoint) {
        this(endpoint, HttpClient.newHttpClient(), DEFAULT_MAX_RESPONSE_BYTES);
    }

    JdkJsonRpcHttpTransport(URI endpoint, int maxResponseBytes) {
        this(endpoint, HttpClient.newHttpClient(), maxResponseBytes);
    }

    JdkJsonRpcHttpTransport(URI endpoint, HttpClient httpClient) {
        this(endpoint, httpClient, DEFAULT_MAX_RESPONSE_BYTES);
    }

    JdkJsonRpcHttpTransport(URI endpoint, HttpClient httpClient, int maxResponseBytes) {
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
        if (maxResponseBytes <= 0) {
            throw new IllegalArgumentException("maxResponseBytes must be positive");
        }
        this.maxResponseBytes = maxResponseBytes;
    }

    @Override
    public String post(String request, Duration timeout) throws IOException, InterruptedException {
        HttpRequest httpRequest = HttpRequest.newBuilder(endpoint)
                .timeout(timeout)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(request, StandardCharsets.UTF_8))
                .build();
        HttpResponse<InputStream> response = httpClient.send(
                httpRequest,
                HttpResponse.BodyHandlers.ofInputStream());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            response.body().close();
            throw new IOException("JSON-RPC HTTP status " + response.statusCode());
        }
        try (InputStream body = response.body()) {
            byte[] bytes = body.readNBytes(maxResponseBytes + 1);
            if (bytes.length > maxResponseBytes) {
                throw new IOException("JSON-RPC response exceeded the configured byte limit");
            }
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }
}
