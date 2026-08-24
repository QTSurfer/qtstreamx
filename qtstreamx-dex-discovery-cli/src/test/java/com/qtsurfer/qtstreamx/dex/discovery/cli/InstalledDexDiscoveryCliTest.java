package com.qtsurfer.qtstreamx.dex.discovery.cli;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Black-box contract tests for the installed DEX discovery executable. */
class InstalledDexDiscoveryCliTest {
    private static Path executable;
    private static final String ROBINHOOD_POOL =
            "0x52e65b17fb6e5ba00ed806f37afcd2daa50271ca";

    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeAll
    static void requiresInstalledJvmDistribution() {
        String executablePath = System.getProperty("qtstreamx.cli.executable");
        assumeTrue(executablePath != null && !executablePath.isBlank(),
                "requires the installed JVM distribution");
        executable = Path.of(executablePath);
    }

    @Test
    void exposesProtocolNeutralAndUniswapJsonContracts() throws Exception {
        CommandResult protocols = run("", "protocols", "--output", "json");
        CommandResult markets = run(
                "",
                "uniswap",
                "markets",
                "--network",
                "robinhood",
                "--output",
                "json");

        JsonNode protocolJson = objectMapper.readTree(protocols.output());
        JsonNode marketJson = objectMapper.readTree(markets.output());
        assertThat(protocols.exitCode()).isZero();
        assertThat(protocolJson.path("schemaVersion").asInt()).isEqualTo(1);
        assertThat(protocolJson.path("protocol").isNull()).isTrue();
        assertThat(protocolJson.path("data").get(0).path("alias").asText())
                .isEqualTo("uniswap");
        assertThat(markets.exitCode()).isZero();
        assertThat(marketJson.path("protocol").asText()).isEqualTo("uniswap");
        assertThat(marketJson.path("data").get(0).path("address").asText())
                .isEqualTo(ROBINHOOD_POOL);
        assertThat(protocols.output()).doesNotContain("\u001b", "QTStreamX DEX Discovery");
        assertThat(markets.output()).doesNotContain("\u001b", "QTStreamX DEX Discovery");
        assertThat(protocols.error()).isEmpty();
        assertThat(markets.error()).isEmpty();
    }

    @Test
    void interactiveAndScriptableRoutesExposeTheSameReviewedMarket() throws Exception {
        CommandResult scriptable = run(
                "", "uniswap", "markets", "--network", "robinhood");
        CommandResult interactive = run("1\n2\n1\n");

        assertThat(scriptable.exitCode()).isZero();
        assertThat(interactive.exitCode()).isZero();
        assertThat(scriptable.output()).contains(ROBINHOOD_POOL, "WETH/USDG", "reviewed=true");
        assertThat(interactive.output())
                .contains("QTStreamX DEX Discovery", ROBINHOOD_POOL, "WETH/USDG", "reviewed=true");
        assertThat(scriptable.error()).isEmpty();
        assertThat(interactive.error()).isEmpty();
    }

    @Test
    void rejectsProtocolSpecificCommandsWithoutAPrefix() throws Exception {
        CommandResult result = run("", "markets", "--network", "robinhood");

        assertThat(result.exitCode()).isEqualTo(DexDiscoveryCliApplication.INVALID_INPUT);
        assertThat(result.output()).isEmpty();
        assertThat(result.error()).contains("protocol command requires a protocol prefix");
    }

    @Test
    void rejectsCaptureWithoutItsActiveAndPassiveRuntimeProviders() throws Exception {
        Path directory = Files.createTempDirectory("qtstreamx-capture-");
        Path eventFile = directory.resolve("events.csv");

        CommandResult result = run(
                "",
                "uniswap", "capture", "--network", "ethereum", "--version", "v3",
                "--start-block", "24000000", "--out", eventFile.toString(),
                "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640");

        assertThat(result.exitCode()).isEqualTo(DexDiscoveryCliApplication.INVALID_INPUT);
        assertThat(result.output()).isEmpty();
        assertThat(result.error()).contains("QTSTREAMX_EVM_HTTP_URL is required")
                .doesNotContain("http://", "https://", "ws://", "wss://");
        assertThat(eventFile).doesNotExist();
        assertThat(eventFile.resolveSibling("events.csv.metadata.csv")).doesNotExist();
    }

    private static CommandResult run(String input, String... arguments)
            throws IOException, InterruptedException {
        List<String> command = new ArrayList<>();
        command.add(executable.toString());
        command.addAll(Arrays.asList(arguments));
        Process process = new ProcessBuilder(command).start();
        process.getOutputStream().write(input.getBytes(StandardCharsets.UTF_8));
        process.getOutputStream().close();
        int exitCode = process.waitFor();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        String error = new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
        return new CommandResult(exitCode, output, error);
    }

    private record CommandResult(int exitCode, String output, String error) {}
}
