package com.qtsurfer.qtstreamx.canary;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.KnownUniswapDeployments;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapDeployment;
import com.qtsurfer.qtstreamx.dex.uniswap.v3.KnownUniswapV3Markets;
import com.qtsurfer.qtstreamx.dex.uniswap.v3.UniswapV3Pool;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Opt-in discovery and capture for the Ethereum Uniswap v3 USDC/WETH 0.05% pool. */
@Tag("it")
class UniswapV3CaptureLiveIT {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String POOL_CREATED_BLOCK = "12376729";

    @TempDir
    Path outputDirectory;

    @Test
    void capturesMainnetPoolIntoNormalizedArtifacts() throws Exception {
        String durationSeconds = System.getenv().getOrDefault(
                "QTSTREAMX_UNISWAP_V3_LIVE_SECONDS", "60");
        String activeHttpUrl = PublicEthereumRpc.activeHttpUrl();
        long captureStartBlock = Math.max(
                0,
                EvmHeadResolver.latestBlock(URI.create(activeHttpUrl), Duration.ofSeconds(15)) - 100);
        UniswapDeployment deployment = KnownUniswapDeployments.ETHEREUM_MAINNET_V3;
        UniswapV3Pool market = KnownUniswapV3Markets.ETHEREUM_MAINNET_WETH_USDC_500;

        UniswapDiscoveryCaptureMain.main(new String[] {
                "--version", "v3",
                "--network", deployment.network(),
                "--active-ws-url", PublicEthereumRpc.activeWebSocketUrl(),
                "--active-http-url", activeHttpUrl,
                "--passive-ws-url", PublicEthereumRpc.passiveWebSocketUrl(),
                "--passive-http-url", PublicEthereumRpc.passiveHttpUrl(),
                "--factory", deployment.factoryScan().factoryAddress(),
                "--factory-start-block", POOL_CREATED_BLOCK,
                "--discovery-safe-head", POOL_CREATED_BLOCK,
                "--discovery-max-scan-blocks", "1",
                "--quote-token", market.tokens().quoteToken().address(),
                "--base-token", market.tokens().baseToken().address(),
                "--capture-start-block", Long.toString(captureStartBlock),
                "--stream-key", "ethereum-uniswap-v3-live-it",
                "--confirmations", "2",
                "--duration-seconds", durationSeconds,
                "--out", outputDirectory.toString()
        });

        assertThat(Files.readAllLines(outputDirectory.resolve("trades.ndjson"))).isNotEmpty();
        assertThat(Files.readAllLines(outputDirectory.resolve("tickers.ndjson"))).isNotEmpty();
        assertThat(Files.readAllLines(outputDirectory.resolve("klines.ndjson"))).isNotEmpty();
        JsonNode summary = MAPPER.readTree(outputDirectory.resolve("summary.json").toFile());
        assertThat(summary.path("discovery").path("selected").asInt()).isEqualTo(1);
    }

}
