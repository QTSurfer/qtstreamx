package com.qtsurfer.qtstreamx.evm.rpc;

import static com.qtsurfer.qtstreamx.evm.rpc.JsonRpcTestResponses.result;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class JdkJsonRpcHttpTransportTest {

    @Test
    void postsJsonRpcPayloadAndReturnsResponseBody() throws Exception {
        AtomicReference<String> receivedBody = new AtomicReference<>();
        AtomicReference<String> receivedContentType = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/rpc", exchange -> {
            receivedBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            receivedContentType.set(exchange.getRequestHeaders().getFirst("Content-Type"));
            byte[] response = result("\"0x1\"").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.start();

        try {
            URI endpoint = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/rpc");
            JdkJsonRpcHttpTransport transport = new JdkJsonRpcHttpTransport(endpoint);

            String response = transport.post("{\"method\":\"eth_blockNumber\"}", Duration.ofSeconds(2));

            assertThat(response).contains("\"result\":\"0x1\"");
            assertThat(receivedBody).hasValue("{\"method\":\"eth_blockNumber\"}");
            assertThat(receivedContentType).hasValue("application/json");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void rejectsAResponseBeyondTheConfiguredByteLimit() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/rpc", exchange -> {
            byte[] response = result("\"" + "x".repeat(100) + "\"")
                    .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.start();

        try {
            URI endpoint = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/rpc");
            JdkJsonRpcHttpTransport transport = new JdkJsonRpcHttpTransport(endpoint, 32);

            assertThatThrownBy(() -> transport.post("{}", Duration.ofSeconds(2)))
                    .isInstanceOf(java.io.IOException.class)
                    .hasMessage("JSON-RPC response exceeded the configured byte limit");
        } finally {
            server.stop(0);
        }
    }
}
