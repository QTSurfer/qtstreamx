package com.qtsurfer.qtstreamx.canary;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import com.qtsurfer.qtstreamx.dex.uniswap.v3.UniswapV3Pool;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class UniswapV3CaptureMainTest {

    @Test
    void parsesExplicitTokensAndIndependentBaseQuoteOrientation() {
        UniswapV3Pool pool = UniswapV3CaptureMain.parsePool(
                "eip155:1",
                "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
                        + "|USDC|0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48|6"
                        + "|WETH|0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2|18"
                        + "|WETH|USDC|500");

        assertThat(pool.token0().symbol()).isEqualTo("USDC");
        assertThat(pool.token1().symbol()).isEqualTo("WETH");
        assertThat(pool.instrument().base()).isEqualTo("WETH");
        assertThat(pool.instrument().quote()).isEqualTo("USDC");
        assertThat(pool.feeTier()).isEqualTo(500);
    }

    @Test
    void resolvesRecentHeadForBoundedLookback() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            byte[] response = "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"0xabc\"}"
                    .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.start();
        try {
            URI endpoint = URI.create(
                    "http://127.0.0.1:" + server.getAddress().getPort() + "/");
            assertThat(EvmHeadResolver.latestBlock(endpoint, Duration.ofSeconds(2)))
                    .isEqualTo(2_748L);
        } finally {
            server.stop(0);
        }
    }
}
