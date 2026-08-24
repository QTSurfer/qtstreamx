package com.qtsurfer.qtstreamx.dex.uniswap.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class KnownUniswapV2MarketsTest {

    @Test
    void exposesImmutableReviewedMarketDescriptors() {
        UniswapV2Pair pair = KnownUniswapV2Markets.ETHEREUM_MAINNET_WETH_USDC;

        assertThat(pair.network()).isEqualTo("eip155:1");
        assertThat(pair.address()).isEqualTo("0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc");
        assertThat(pair.instrument().symbol()).isEqualTo("WETH/USDC");
        assertThat(KnownUniswapV2Markets.all()).containsExactly(pair);
        assertThatThrownBy(() -> KnownUniswapV2Markets.all().add(pair))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
