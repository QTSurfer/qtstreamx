package com.qtsurfer.qtstreamx.dex.discovery.uniswap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.qtsurfer.qtstreamx.dex.uniswap.v3.KnownUniswapV3Markets;
import com.qtsurfer.qtstreamx.dex.uniswap.v3.UniswapV3Pool;
import com.qtsurfer.qtstreamx.evm.rpc.EvmBlockTag;
import com.qtsurfer.qtstreamx.evm.rpc.EvmHttpRpcReader;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcReader;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcReaderConfig;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Opt-in Robinhood evidence for token metadata and targeted V3 pool lookup. */
@Tag("it")
class UniswapOnChainLookupLiveIT {

    private static final String RUBY = "0xc0a9531cae8bea6268bd19efec1dd205830cae2a";

    @Test
    void inspectsArbitraryTokenAndFindsReviewedPoolWithoutRestApis() {
        String httpUrl = System.getenv("QTSTREAMX_ROBINHOOD_HTTP_URL");
        assumeTrue(httpUrl != null && !httpUrl.isBlank(),
                "QTSTREAMX_ROBINHOOD_HTTP_URL is required");
        EvmRpcReader reader = new EvmHttpRpcReader(new EvmRpcReaderConfig(
                "eip155:4663",
                httpUrl,
                2_000,
                Duration.ofSeconds(15),
                3));
        UniswapOnChainLookup lookup = new UniswapOnChainLookup(reader);
        UniswapV3Pool reviewed = KnownUniswapV3Markets.ROBINHOOD_MAINNET_WETH_USDG_100;

        Erc20TokenInspection arbitraryToken = lookup.inspectToken(
                "eip155:4663", RUBY, EvmBlockTag.latest());
        List<UniswapV3PoolInspection> reviewedPools = lookup.findV3Pools(
                KnownUniswapDeployments.ROBINHOOD_MAINNET_V3,
                reviewed.token0().address(),
                Set.of(reviewed.token1().address()),
                Set.of(reviewed.feeTier()),
                EvmBlockTag.latest());

        assertThat(arbitraryToken.name()).contains("The Reddit Dog");
        assertThat(arbitraryToken.symbol()).contains("RUBY");
        assertThat(arbitraryToken.decimals()).hasValue(9);
        assertThat(reviewedPools).singleElement().satisfies(pool -> {
            assertThat(pool.address()).isEqualTo(reviewed.address());
            assertThat(pool.currentLiquidity()).isPositive();
        });
    }
}
