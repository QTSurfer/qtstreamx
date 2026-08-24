package com.qtsurfer.qtstreamx.dex.uniswap.v3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class KnownUniswapV3MarketsTest {

    @Test
    void exposesImmutableReviewedMarketDescriptorsAcrossNetworks() {
        UniswapV3Pool ethereum = KnownUniswapV3Markets.ETHEREUM_MAINNET_WETH_USDC_500;
        UniswapV3Pool robinhood = KnownUniswapV3Markets.ROBINHOOD_MAINNET_WETH_USDG_100;

        assertThat(ethereum.network()).isEqualTo("eip155:1");
        assertThat(ethereum.instrument().symbol()).isEqualTo("WETH/USDC");
        assertThat(ethereum.feeTier()).isEqualTo(500);
        assertThat(robinhood.network()).isEqualTo("eip155:4663");
        assertThat(robinhood.instrument().symbol()).isEqualTo("WETH/USDG");
        assertThat(robinhood.feeTier()).isEqualTo(100);
        assertThat(KnownUniswapV3Markets.all()).containsExactly(ethereum, robinhood);
        assertThatThrownBy(() -> KnownUniswapV3Markets.all().add(ethereum))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
