package com.qtsurfer.qtstreamx.dex.uniswap.v3;

import com.qtsurfer.qtstreamx.dex.core.EvmToken;

import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogStream;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogStreamConfig;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcLogStream;
import com.qtsurfer.qtstreamx.ws.jdk.JdkWebSocketClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Live opt-in smoke test for one caller-configured Uniswap v3 pool. */
@Tag("it")
class UniswapV3LiveIT {

    @Test
    void emitsTradeFromConfiguredPool() throws Exception {
        String network = environment("NETWORK");
        String wsUrl = environment("WS_URL");
        String httpUrl = environment("HTTP_URL");
        String startBlock = environment("START_BLOCK");
        String poolAddress = environment("POOL_ADDRESS");
        String base = environment("BASE");
        String quote = environment("QUOTE");
        String token0Symbol = environment("TOKEN0_SYMBOL");
        String token0Address = environment("TOKEN0_ADDRESS");
        String token0Decimals = environment("TOKEN0_DECIMALS");
        String token1Symbol = environment("TOKEN1_SYMBOL");
        String token1Address = environment("TOKEN1_ADDRESS");
        String token1Decimals = environment("TOKEN1_DECIMALS");
        String feeTier = environment("FEE_TIER");
        Assumptions.assumeTrue(
                Stream.of(
                                network, wsUrl, httpUrl, startBlock, poolAddress,
                                base, quote, token0Symbol, token0Address,
                                token0Decimals, token1Symbol, token1Address,
                                token1Decimals, feeTier)
                        .allMatch(value -> value != null && !value.isBlank()),
                "live Uniswap environment is not configured");

        EvmLogStreamConfig rpcConfig = new EvmLogStreamConfig(
                network, wsUrl, httpUrl, Long.parseLong(startBlock),
                1, 2_000, Duration.ofSeconds(15), 3);
        UniswapV3Pool pool = new UniswapV3Pool(
                network,
                poolAddress,
                new EvmToken(
                        token0Symbol,
                        token0Address,
                        Integer.parseInt(token0Decimals)),
                new EvmToken(
                        token1Symbol,
                        token1Address,
                        Integer.parseInt(token1Decimals)),
                new Instrument(base, quote),
                Integer.parseInt(feeTier));
        CountDownLatch received = new CountDownLatch(1);
        List<Throwable> errors = new ArrayList<>();
        EvmLogStream source = new EvmRpcLogStream(rpcConfig, JdkWebSocketClient::new);

        try (UniswapV3MarketDataStream stream =
                new UniswapV3MarketDataStream(source, List.of(pool))) {
            stream.onError(errors::add);
            stream.start(trade -> received.countDown());
            assertThat(received.await(60, TimeUnit.SECONDS)).isTrue();
        }

        assertThat(errors).isEmpty();
    }

    private static String environment(String suffix) {
        return System.getenv("QTSTREAMX_UNISWAP_" + suffix);
    }
}
