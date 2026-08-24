package com.qtsurfer.qtstreamx.canary;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.KnownUniswapDeployments;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapDeployment;
import com.qtsurfer.qtstreamx.dex.uniswap.v2.KnownUniswapV2Markets;
import com.qtsurfer.qtstreamx.dex.uniswap.v2.UniswapV2Pair;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Opt-in discovery and capture for the public Ethereum Uniswap v2 USDC/WETH pair. */
@Tag("it")
class UniswapV2CaptureLiveIT {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String PAIR_CREATED_BLOCK = "10008355";

    @TempDir
    Path outputDirectory;

    @Test
    void capturesMainnetPairIntoNormalizedArtifacts() throws Exception {
        String durationSeconds = System.getenv().getOrDefault(
                "QTSTREAMX_UNISWAP_V2_LIVE_SECONDS", "60");
        String lookbackBlocks = System.getenv().getOrDefault(
                "QTSTREAMX_UNISWAP_V2_LOOKBACK_BLOCKS", "100");
        String activeHttpUrl = PublicEthereumRpc.activeHttpUrl();
        long captureStartBlock = Math.max(
                0,
                EvmHeadResolver.latestBlock(URI.create(activeHttpUrl), Duration.ofSeconds(15))
                        - Long.parseLong(lookbackBlocks));
        UniswapDeployment deployment = KnownUniswapDeployments.ETHEREUM_MAINNET_V2;
        UniswapV2Pair market = KnownUniswapV2Markets.ETHEREUM_MAINNET_WETH_USDC;

        UniswapDiscoveryCaptureMain.main(new String[] {
                "--version", "v2",
                "--network", deployment.network(),
                "--active-ws-url", PublicEthereumRpc.activeWebSocketUrl(),
                "--active-http-url", activeHttpUrl,
                "--passive-ws-url", PublicEthereumRpc.passiveWebSocketUrl(),
                "--passive-http-url", PublicEthereumRpc.passiveHttpUrl(),
                "--factory", deployment.factoryScan().factoryAddress(),
                "--factory-start-block", PAIR_CREATED_BLOCK,
                "--discovery-safe-head", PAIR_CREATED_BLOCK,
                "--discovery-max-scan-blocks", "1",
                "--quote-token", market.tokens().quoteToken().address(),
                "--base-token", market.tokens().baseToken().address(),
                "--capture-start-block", Long.toString(captureStartBlock),
                "--stream-key", "ethereum-uniswap-v2-live-it",
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
