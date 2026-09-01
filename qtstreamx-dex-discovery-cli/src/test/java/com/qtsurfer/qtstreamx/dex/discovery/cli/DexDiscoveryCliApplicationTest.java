package com.qtsurfer.qtstreamx.dex.discovery.cli;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.http.HttpTimeoutException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class DexDiscoveryCliApplicationTest {
    private static final String TOKEN = "0xc0a9531cae8bea6268bd19efec1dd205830cae2a";

    @Test
    void rendersStableJsonWithoutAnEndpoint() throws Exception {
        TestTerminal terminal = new TestTerminal();
        int exit = new DexDiscoveryCliApplication(terminal)
                .run(new String[] {"networks", "--output", "json"}, Map.of());

        JsonNode json = new ObjectMapper().readTree(terminal.output());
        assertThat(exit).isZero();
        assertThat(json.path("schemaVersion").asInt()).isEqualTo(1);
        assertThat(json.path("protocol").isNull()).isTrue();
        assertThat(json.path("command").asText()).isEqualTo("networks");
        assertThat(json.path("data").size()).isEqualTo(2);
        assertThat(terminal.errors()).isEmpty();
    }

    @Test
    void identifiesTheProtocolOwningAnAdapterCommand() throws Exception {
        TestTerminal terminal = new TestTerminal();

        int exit = new DexDiscoveryCliApplication(terminal).run(
                new String[] {"uniswap", "markets", "--network", "robinhood", "--output", "json"},
                Map.of());

        JsonNode json = new ObjectMapper().readTree(terminal.output());
        assertThat(exit).isZero();
        assertThat(json.path("protocol").asText()).isEqualTo("uniswap");
        assertThat(json.path("command").asText()).isEqualTo("markets");
        assertThat(json.path("data").get(0).path("instrument").asText())
                .isEqualTo("WETH/USDG");
    }

    @Test
    void interactiveAndScriptableRoutesDelegateTheSameTokenCommand() {
        TestTerminal terminal = new TestTerminal("1", "2", "3", TOKEN);
        AtomicReference<CliRequest> request = new AtomicReference<>();
        AtomicReference<String> endpoint = new AtomicReference<>();
        DexDiscoveryCliApplication application = new DexDiscoveryCliApplication(
                terminal,
                (protocol, network, httpUrl) -> {
                    assertThat(protocol).isEqualTo(CliRequest.Protocol.UNISWAP);
                    assertThat(network).isEqualTo("eip155:4663");
                    endpoint.set(httpUrl);
                    return command -> {
                        request.set(command);
                        return CliResponse.ok(command, List.of("delegated"));
                    };
                });

        int exit = application.run(new String[0], Map.of(
                "QTSTREAMX_EVM_HTTP_URL", "https://rpc.example/private"));

        assertThat(exit).isZero();
        assertThat(request.get().command()).isEqualTo(CliRequest.Command.TOKEN);
        assertThat(request.get().protocol()).isEqualTo(CliRequest.Protocol.UNISWAP);
        assertThat(request.get().arguments()).containsExactly(TOKEN);
        assertThat(request.get().requiredOption("network")).isEqualTo("robinhood");
        assertThat(endpoint.get()).isEqualTo("https://rpc.example/private");
        assertThat(terminal.output()).doesNotContain("rpc.example", "private");
    }

    @Test
    void preflightsAndIdentifiesTheConfiguredHttpProviderWithoutLeakingIt() {
        TestTerminal missingTerminal = new TestTerminal();
        AtomicReference<Boolean> missingFactoryCalled = new AtomicReference<>(false);
        DexDiscoveryCliApplication missing = new DexDiscoveryCliApplication(
                missingTerminal,
                (protocol, network, endpoint) -> {
                    missingFactoryCalled.set(true);
                    return request -> CliResponse.ok(request, List.of());
                });

        int missingExit = missing.run(
                new String[] {"uniswap", "token", "--network", "ethereum", TOKEN}, Map.of());

        assertThat(missingExit).isEqualTo(DexDiscoveryCliApplication.INVALID_INPUT);
        assertThat(missingFactoryCalled.get()).isFalse();
        assertThat(missingTerminal.errors()).contains("QTSTREAMX_EVM_HTTP_URL is required");

        TestTerminal configuredTerminal = new TestTerminal();
        DexDiscoveryCliApplication configured = new DexDiscoveryCliApplication(
                configuredTerminal,
                (protocol, network, endpoint) -> request -> CliResponse.ok(request, List.of("delegated")));

        int configuredExit = configured.run(
                new String[] {"uniswap", "token", "--network", "ethereum", TOKEN},
                Map.of("QTSTREAMX_EVM_HTTP_URL", "https://private.example/key"));

        assertThat(configuredExit).isZero();
        assertThat(configuredTerminal.output())
                .contains("RPC HTTP provider: QTSTREAMX_EVM_HTTP_URL (endpoint redacted)")
                .doesNotContain("private.example", "key");
    }

    @Test
    void mapsNoMarketAndProviderFailureToStableExitCodes() {
        TestTerminal noMarketTerminal = new TestTerminal();
        DexDiscoveryCliApplication noMarket = new DexDiscoveryCliApplication(
                noMarketTerminal,
                (protocol, network, endpoint) -> request ->
                        CliResponse.noSupportedMarket(request, List.of()));

        int noMarketExit = noMarket.run(
                new String[] {"uniswap", "pool", "--network", "ethereum", TOKEN,
                        "--http-url", "https://secret"},
                Map.of());

        TestTerminal failedTerminal = new TestTerminal();
        DexDiscoveryCliApplication failed = new DexDiscoveryCliApplication(
                failedTerminal,
                (protocol, network, endpoint) -> request -> {
                    throw new IllegalStateException("provider body contains https://secret/key");
                });
        int failedExit = failed.run(
                new String[] {"uniswap", "token", "--network", "ethereum", TOKEN,
                        "--http-url", "https://secret/key"},
                Map.of());

        assertThat(noMarketExit).isEqualTo(DexDiscoveryCliApplication.NO_SUPPORTED_MARKET);
        assertThat(failedExit).isEqualTo(DexDiscoveryCliApplication.PROVIDER_UNAVAILABLE);
        assertThat(failedTerminal.errors())
                .contains("RPC provider unavailable")
                .doesNotContain("https://secret", "provider body");
    }

    @Test
    void sanitizesInvalidHumanInput() {
        TestTerminal terminal = new TestTerminal();
        int exit = new DexDiscoveryCliApplication(terminal).run(
                new String[] {"uniswap", "markets", "--network", "bad\u001b[31m"},
                Map.of());

        assertThat(exit).isEqualTo(DexDiscoveryCliApplication.INVALID_INPUT);
        assertThat(terminal.errors()).doesNotContain("\u001b");
    }

    @Test
    void rejectsAnUnreviewedCaptureBeforeItRequiresProviderEndpoints() {
        TestTerminal terminal = new TestTerminal();

        int exit = new DexDiscoveryCliApplication(terminal).run(new String[] {
                "uniswap", "capture", "--network", "ethereum", "--version", "v2",
                "--start-block", "1", "--out", "ticks.csv", TOKEN}, Map.of());

        assertThat(exit).isEqualTo(DexDiscoveryCliApplication.INVALID_INPUT);
        assertThat(terminal.errors()).contains("not a reviewed Uniswap v2 pair");
    }

    @Test
    void handlesInteractiveEndOfInputWithoutAStackTrace() {
        TestTerminal terminal = new TestTerminal();

        int exit = new DexDiscoveryCliApplication(terminal).run(new String[0], Map.of());

        assertThat(exit).isEqualTo(DexDiscoveryCliApplication.INVALID_INPUT);
        assertThat(terminal.errors())
                .contains("input ended before command was complete")
                .doesNotContain("Exception", "at com.qtsurfer");
    }

    @Test
    void mapsAnInterruptedProviderCallWithoutLeakingItsFailure() {
        TestTerminal terminal = new TestTerminal();
        DexDiscoveryCliApplication application = new DexDiscoveryCliApplication(
                terminal,
                (protocol, network, endpoint) -> request -> {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("provider body contains secret material");
                });

        int exit;
        try {
            exit = application.run(
                    new String[] {"uniswap", "token", "--network", "ethereum", TOKEN,
                            "--http-url", "https://secret.example/key"},
                    Map.of());
        } finally {
            Thread.interrupted();
        }

        assertThat(exit).isEqualTo(130);
        assertThat(terminal.errors())
                .contains("Operation interrupted")
                .doesNotContain("secret", "provider body");
    }

    @Test
    void mapsAProviderTimeoutToTheStableUnavailableContract() {
        TestTerminal terminal = new TestTerminal();
        DexDiscoveryCliApplication application = new DexDiscoveryCliApplication(
                terminal,
                (protocol, network, endpoint) -> request -> {
                    throw new IllegalStateException(
                            "request timed out at " + endpoint,
                            new HttpTimeoutException("private timeout detail"));
                });

        int exit = application.run(
                new String[] {"uniswap", "token", "--network", "ethereum", TOKEN,
                        "--http-url", "https://secret.example/key"},
                Map.of());

        assertThat(exit).isEqualTo(DexDiscoveryCliApplication.PROVIDER_UNAVAILABLE);
        assertThat(terminal.errors())
                .contains("RPC provider unavailable")
                .doesNotContain("secret", "timed out", "private timeout detail");
    }

    private static final class TestTerminal implements CliTerminal {
        private final Queue<String> input;
        private final StringBuilder output = new StringBuilder();
        private final StringBuilder errors = new StringBuilder();

        private TestTerminal(String... input) {
            this.input = new ArrayDeque<>(List.of(input));
        }

        @Override
        public String readLine() throws IOException {
            return input.poll();
        }

        @Override
        public void write(String text) {
            output.append(text);
        }

        @Override
        public void writeError(String text) {
            errors.append(text);
        }

        private String output() {
            return output.toString();
        }

        private String errors() {
            return errors.toString();
        }
    }
}
