package com.qtsurfer.qtstreamx.canary;

import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.dex.uniswap.v2.UniswapV2Pair;
import org.junit.jupiter.api.Test;

class UniswapV2CaptureMainTest {

    @Test
    void parsesExplicitTokensAndIndependentBaseQuoteOrientation() {
        UniswapV2Pair pair = UniswapV2CaptureMain.parsePair(
                "eip155:1",
                "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"
                        + "|USDC|0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48|6"
                        + "|WETH|0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2|18"
                        + "|WETH|USDC");

        assertThat(pair.token0().symbol()).isEqualTo("USDC");
        assertThat(pair.token1().symbol()).isEqualTo("WETH");
        assertThat(pair.instrument().base()).isEqualTo("WETH");
        assertThat(pair.instrument().quote()).isEqualTo("USDC");
    }
}
