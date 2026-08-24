package com.qtsurfer.qtstreamx.dex.discovery.uniswap.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.dex.discovery.uniswap.KnownUniswapDeployments;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapDeployment;
import com.qtsurfer.qtstreamx.dex.uniswap.v2.KnownUniswapV2Markets;
import com.qtsurfer.qtstreamx.dex.uniswap.v2.UniswapV2Pair;
import com.qtsurfer.qtstreamx.dex.uniswap.v3.KnownUniswapV3Markets;
import com.qtsurfer.qtstreamx.dex.uniswap.v3.UniswapV3Pool;
import org.junit.jupiter.api.Test;

class PublicKnownUniswapPresetsTest {

    @Test
    void consumesKnownMarketsAndDeploymentsThroughPublicInterfaces() {
        UniswapV2Pair ethereumV2 = KnownUniswapV2Markets.ETHEREUM_MAINNET_WETH_USDC;
        UniswapV3Pool ethereumV3 = KnownUniswapV3Markets.ETHEREUM_MAINNET_WETH_USDC_500;
        UniswapV3Pool robinhoodV3 = KnownUniswapV3Markets.ROBINHOOD_MAINNET_WETH_USDG_100;
        UniswapDeployment robinhood = KnownUniswapDeployments.ROBINHOOD_MAINNET_V3;

        assertThat(KnownUniswapDeployments.all()).hasSize(3);
        assertThat(ethereumV2.instrument()).isEqualTo(ethereumV3.instrument());
        assertThat(ethereumV2.address()).isNotEqualTo(ethereumV3.address());
        assertThat(robinhood.network()).isEqualTo(robinhoodV3.network());
        assertThat(robinhood.orientation()
                        .orient(robinhoodV3.network(), robinhoodV3.token0(), robinhoodV3.token1()))
                .contains(robinhoodV3.instrument());
    }
}
