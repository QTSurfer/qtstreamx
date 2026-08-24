package com.qtsurfer.qtstreamx.evm.rpc;

import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.core.ws.WebSocketClient;
import java.net.http.HttpTimeoutException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class EvmWebSocketRpcCapabilityProbeTest {
    private static final String ENDPOINT = "wss://user:api-secret@example.test/rpc";
    private static final String ADDRESS = "0x1111111111111111111111111111111111111111";
    private static final String TOPIC =
            "0x2222222222222222222222222222222222222222222222222222222222222222";
    private static final Clock CLOCK =
            Clock.fixed(Instant.parse("2026-08-09T10:00:00Z"), ZoneOffset.UTC);

    @Test
    void acceptsBothSubscriptionAcknowledgementsWithoutRetainingIdentifiers() {
        FakeWebSocket webSocket = new FakeWebSocket(request -> {
            if (request.contains("\"id\":1")) {
                return "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"secret-log-subscription\"}";
            }
            if (request.contains("\"id\":2")) {
                return "{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":\"secret-head-subscription\"}";
            }
            if (request.contains("\"id\":3")) {
                return "{\"jsonrpc\":\"2.0\",\"id\":3,\"result\":\"0x1\"}";
            }
            return """
                    {"jsonrpc":"2.0","id":4,
                     "result":{"number":"0x64","hash":"0x%s"}}
                    """.formatted("1".repeat(64));
        });

        EvmRpcCapabilityReport report = probe(webSocket).probe(filter(), wsBudget(4));

        assertThat(report.observations()).hasSize(4);
        assertThat(report.observations())
                .allSatisfy(observation -> {
                    assertThat(observation.transport()).isEqualTo(EvmRpcTransport.WEBSOCKET);
                    assertThat(observation.status()).isEqualTo(EvmRpcProbeStatus.SUPPORTED);
                });
        assertThat(report.supports(EvmRpcProbePurpose.LIVE_SUBSCRIPTION)).isTrue();
        assertThat(report.supports(EvmRpcProbePurpose.NETWORK)).isTrue();
        assertThat(report.supports(EvmRpcProbePurpose.FINALITY)).isTrue();
        assertThat(webSocket.sent())
                .anySatisfy(request -> assertThat(request)
                        .contains("\"logs\"")
                        .contains(ADDRESS)
                        .contains(TOPIC))
                .anySatisfy(request -> assertThat(request).contains("\"newHeads\""));
        assertThat(webSocket.sent())
                .anySatisfy(request -> assertThat(request).contains("eth_chainId"))
                .anySatisfy(request -> assertThat(request)
                        .contains("eth_getBlockByNumber", "safe"));
        assertThat(report.toString())
                .doesNotContain("api-secret")
                .doesNotContain("example.test")
                .doesNotContain("secret-log-subscription")
                .doesNotContain("secret-head-subscription");
        assertThat(webSocket.closed()).isTrue();
    }

    @Test
    void classifiesProviderErrorsAndEmptyAcknowledgementsWithoutLeakingText() {
        FakeWebSocket webSocket = new FakeWebSocket(request -> {
            if (request.contains("\"id\":1")) {
                return """
                        {"jsonrpc":"2.0","id":1,
                         "error":{"code":-32601,"message":"api-secret provider detail"}}
                        """;
            }
            return "{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":\"\"}";
        });

        EvmRpcCapabilityReport report = probe(webSocket).probe(filter(), wsBudget(2));

        assertThat(report.observations())
                .anySatisfy(observation -> {
                    assertThat(observation.operation())
                            .isEqualTo(EvmRpcProbeOperation.LOG_SUBSCRIPTION);
                    assertThat(observation.status()).isEqualTo(EvmRpcProbeStatus.UNSUPPORTED);
                    assertThat(observation.rpcErrorCode()).hasValue(-32601);
                })
                .anySatisfy(observation -> {
                    assertThat(observation.operation())
                            .isEqualTo(EvmRpcProbeOperation.NEW_HEADS_SUBSCRIPTION);
                    assertThat(observation.status())
                            .isEqualTo(EvmRpcProbeStatus.MALFORMED_RESPONSE);
                });
        assertThat(report.toString()).doesNotContain("api-secret", "provider detail");
    }

    @Test
    void rejectsAWebSocketConnectedToAnotherNetwork() {
        FakeWebSocket webSocket = new FakeWebSocket(request -> {
            if (request.contains("\"id\":1")) {
                return "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"0xlogs\"}";
            }
            if (request.contains("\"id\":2")) {
                return "{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":\"0xheads\"}";
            }
            if (request.contains("\"id\":3")) {
                return "{\"jsonrpc\":\"2.0\",\"id\":3,\"result\":\"0x2\"}";
            }
            return """
                    {"jsonrpc":"2.0","id":4,
                     "result":{"number":"0x64","hash":"0x%s"}}
                    """.formatted("1".repeat(64));
        });

        EvmRpcCapabilityReport report = probe(webSocket).probe(filter(), wsBudget(4));

        assertThat(report.supports(EvmRpcProbePurpose.NETWORK)).isFalse();
        assertThat(report.observations())
                .filteredOn(observation -> observation.purpose() == EvmRpcProbePurpose.NETWORK)
                .singleElement()
                .extracting(EvmRpcProbeObservation::status)
                .isEqualTo(EvmRpcProbeStatus.WRONG_NETWORK);
    }

    @Test
    void doesNotSendBeyondTheRequestBudget() {
        FakeWebSocket webSocket = new FakeWebSocket(
                request -> "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"0x1\"}");

        EvmRpcCapabilityReport report = probe(webSocket).probe(filter(), wsBudget(1));

        assertThat(webSocket.sent()).hasSize(1);
        assertThat(report.observations())
                .filteredOn(observation -> observation.status() == EvmRpcProbeStatus.BUDGET_EXHAUSTED)
                .extracting(EvmRpcProbeObservation::operation)
                .containsExactly(
                        EvmRpcProbeOperation.NEW_HEADS_SUBSCRIPTION,
                        EvmRpcProbeOperation.CHAIN_ID,
                        EvmRpcProbeOperation.SAFE_BLOCK);
    }

    @Test
    void redactsConnectFailures() {
        FakeWebSocket webSocket = new FakeWebSocket(request -> null);
        webSocket.connectFailure = new IllegalStateException(
                "failed " + ENDPOINT + " with provider body");

        EvmRpcCapabilityReport report = probe(webSocket).probe(filter(), wsBudget(4));

        assertThat(report.observations())
                .allMatch(observation -> observation.status() == EvmRpcProbeStatus.TRANSPORT_FAILURE);
        assertThat(report.toString())
                .doesNotContain("api-secret")
                .doesNotContain("example.test")
                .doesNotContain("provider body");
    }

    @Test
    void appliesTheGlobalDeadlineToTheHandshakeWithoutWaiting() {
        FakeWebSocket webSocket = new FakeWebSocket(request -> null);
        webSocket.timedConnectFailure =
                new HttpTimeoutException("provider detail api-secret");

        EvmRpcCapabilityReport report = probe(webSocket).probe(filter(), wsBudget(4));

        assertThat(webSocket.connectTimeout()).isPositive().isLessThanOrEqualTo(Duration.ofSeconds(1));
        assertThat(report.observations())
                .allMatch(observation -> observation.status() == EvmRpcProbeStatus.TIMEOUT);
        assertThat(report.toString()).doesNotContain("api-secret", "provider detail");
    }

    @Test
    void redactsTheRuntimeConfiguration() {
        assertThat(config().toString())
                .contains("webSocketUrl=<redacted>")
                .doesNotContain("api-secret", "example.test");
    }

    private static EvmWebSocketRpcCapabilityProbe probe(FakeWebSocket webSocket) {
        return new EvmWebSocketRpcCapabilityProbe(
                config(),
                "ethereum-primary",
                () -> webSocket,
                CLOCK);
    }

    private static EvmRpcWebSocketProbeConfig config() {
        return new EvmRpcWebSocketProbeConfig(
                "eip155:1",
                ENDPOINT,
                Duration.ofSeconds(1));
    }

    private static EvmLogFilter filter() {
        return new EvmLogFilter(Set.of(ADDRESS), Set.of(TOPIC));
    }

    private static EvmRpcProbeBudget wsBudget(int requests) {
        return new EvmRpcProbeBudget(
                requests,
                Duration.ofSeconds(2),
                1,
                1);
    }

    private static final class FakeWebSocket implements WebSocketClient {
        private final Function<String, String> response;
        private final List<String> sent = new ArrayList<>();
        private Consumer<String> messageHandler = ignored -> {};
        private BiConsumer<Integer, String> closeHandler = (code, reason) -> {};
        private Consumer<Throwable> errorHandler = ignored -> {};
        private Exception connectFailure;
        private Exception timedConnectFailure;
        private Duration connectTimeout;
        private boolean open;
        private boolean closed;

        private FakeWebSocket(Function<String, String> response) {
            this.response = response;
        }

        @Override
        public void connect(String url) throws Exception {
            if (connectFailure != null) {
                throw connectFailure;
            }
            open = true;
        }

        @Override
        public void connect(String url, Duration timeout) throws Exception {
            connectTimeout = timeout;
            if (timedConnectFailure != null) {
                throw timedConnectFailure;
            }
            connect(url);
        }

        @Override
        public void send(String message) {
            sent.add(message);
            String acknowledgement = response.apply(message);
            if (acknowledgement != null) {
                messageHandler.accept(acknowledgement);
            }
        }

        @Override
        public void onMessage(Consumer<String> handler) {
            messageHandler = handler;
        }

        @Override
        public void onClose(BiConsumer<Integer, String> handler) {
            closeHandler = handler;
        }

        @Override
        public void onError(Consumer<Throwable> handler) {
            errorHandler = handler;
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() {
            open = false;
            closed = true;
        }

        private List<String> sent() {
            return List.copyOf(sent);
        }

        private boolean closed() {
            return closed;
        }

        private Duration connectTimeout() {
            return connectTimeout;
        }
    }
}
