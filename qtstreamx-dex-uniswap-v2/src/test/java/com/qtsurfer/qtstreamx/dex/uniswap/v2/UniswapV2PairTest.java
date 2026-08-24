package com.qtsurfer.qtstreamx.dex.uniswap.v2;

import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.dex.core.EvmToken;
import org.junit.jupiter.api.Test;

class UniswapV2PairTest {

    @Test
    void mapsLogicalInstrumentToExplicitPairTokens() {
        EvmToken quote = new EvmToken(
                "USDC", "0x0000000000000000000000000000000000000001", 6);
        EvmToken base = new EvmToken(
                "WETH", "0x0000000000000000000000000000000000000002", 18);

        UniswapV2Pair pair = new UniswapV2Pair(
                "eip155:1",
                "0x00000000000000000000000000000000000000ab",
                quote,
                base,
                new Instrument("WETH", "USDC"));

        assertThat(pair.token0()).isEqualTo(quote);
        assertThat(pair.token1()).isEqualTo(base);
        assertThat(pair.instrument().base()).isEqualTo(pair.token1().symbol());
        assertThat(pair.instrument().quote()).isEqualTo(pair.token0().symbol());
    }

    @Test
    void normalizesEvmAddresses() {
        UniswapV2Pair pair = new UniswapV2Pair(
                "eip155:1",
                "0x00000000000000000000000000000000000000AB",
                new EvmToken("BASE", "0x000000000000000000000000000000000000000A", 18),
                new EvmToken("QUOTE", "0x000000000000000000000000000000000000000B", 6),
                new Instrument("BASE", "QUOTE"));

        assertThat(pair.address()).endsWith("ab");
        assertThat(pair.token0().address()).endsWith("0a");
        assertThat(pair.token1().address()).endsWith("0b");
    }
}
