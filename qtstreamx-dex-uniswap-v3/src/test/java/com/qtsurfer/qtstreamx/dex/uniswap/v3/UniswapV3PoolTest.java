package com.qtsurfer.qtstreamx.dex.uniswap.v3;

import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.dex.core.EvmToken;
import org.junit.jupiter.api.Test;

class UniswapV3PoolTest {

    private static final String WETH_ADDRESS = "0x0000000000000000000000000000000000000001";
    private static final String USDC_ADDRESS = "0x0000000000000000000000000000000000000002";

    @Test
    void mapsInstrumentBaseAndQuoteToExplicitPoolTokens() {
        EvmToken weth = new EvmToken("WETH", WETH_ADDRESS, 18);
        EvmToken usdc = new EvmToken("USDC", USDC_ADDRESS, 6);

        UniswapV3Pool pool = new UniswapV3Pool(
                "eip155:1",
                "0x00000000000000000000000000000000000000ab",
                weth,
                usdc,
                new Instrument("WETH", "USDC"),
                500);

        assertThat(pool.token0()).isEqualTo(weth);
        assertThat(pool.token1()).isEqualTo(usdc);
        assertThat(pool.instrument().base()).isEqualTo(pool.token0().symbol());
        assertThat(pool.instrument().quote()).isEqualTo(pool.token1().symbol());
    }
}
