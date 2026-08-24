package com.qtsurfer.qtstreamx.dex.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import org.junit.jupiter.api.Test;

class EvmTokenPairTest {

    private static final EvmToken TOKEN0 = new EvmToken(
            "BASE", "0x0000000000000000000000000000000000000001", 18);
    private static final EvmToken TOKEN1 = new EvmToken(
            "QUOTE", "0x0000000000000000000000000000000000000002", 6);

    @Test
    void resolvesLogicalBaseWhenItIsToken1() {
        EvmToken usdc = new EvmToken(
                "USDC", "0x000000000000000000000000000000000000000A", 6);
        EvmToken weth = new EvmToken(
                "WETH", "0x000000000000000000000000000000000000000B", 18);

        EvmTokenPair tokens = new EvmTokenPair(usdc, weth, new Instrument("WETH", "USDC"));

        assertThat(tokens.token0().address())
                .isEqualTo("0x000000000000000000000000000000000000000a");
        assertThat(tokens.baseToken()).isEqualTo(weth);
        assertThat(tokens.quoteToken()).isEqualTo(usdc);
        assertThat(tokens.token0IsBase()).isFalse();
    }

    @Test
    void resolvesLogicalBaseWhenItIsToken0() {
        EvmTokenPair tokens = new EvmTokenPair(
                TOKEN0, TOKEN1, new Instrument("BASE", "QUOTE"));

        assertThat(tokens.baseToken()).isEqualTo(TOKEN0);
        assertThat(tokens.quoteToken()).isEqualTo(TOKEN1);
        assertThat(tokens.token0IsBase()).isTrue();
    }

    @Test
    void rejectsTokensOutsideContractAddressOrder() {
        assertThatThrownBy(() -> new EvmTokenPair(
                        TOKEN1, TOKEN0, new Instrument("BASE", "QUOTE")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("address order");
    }

    @Test
    void rejectsInstrumentThatDoesNotMatchBothTokens() {
        assertThatThrownBy(() -> new EvmTokenPair(
                        TOKEN0, TOKEN1, new Instrument("OTHER", "QUOTE")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("instrument");
    }

    @Test
    void rejectsDerivativeInstrument() {
        assertThatThrownBy(() -> new EvmTokenPair(
                        TOKEN0, TOKEN1, new Instrument("BASE", "QUOTE", "QUOTE")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("spot");
    }

    @Test
    void rejectsDuplicateTokenSymbols() {
        EvmToken duplicateSymbol = new EvmToken(
                "BASE", "0x0000000000000000000000000000000000000002", 6);

        assertThatThrownBy(() -> new EvmTokenPair(
                        TOKEN0, duplicateSymbol, new Instrument("BASE", "BASE")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("distinct");
    }
}
