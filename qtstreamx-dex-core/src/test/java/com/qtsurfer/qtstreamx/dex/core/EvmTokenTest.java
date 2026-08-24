package com.qtsurfer.qtstreamx.dex.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class EvmTokenTest {

    @Test
    void normalizesValidTokenMetadata() {
        EvmToken token = new EvmToken(
                "WETH", "0x000000000000000000000000000000000000000A", 18);

        assertThat(token.symbol()).isEqualTo("WETH");
        assertThat(token.address()).isEqualTo("0x000000000000000000000000000000000000000a");
        assertThat(token.decimals()).isEqualTo(18);
    }

    @Test
    void rejectsIncompleteOrInvalidMetadata() {
        assertThatThrownBy(() -> new EvmToken(" ", validAddress(), 18))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("symbol");
        assertThatThrownBy(() -> new EvmToken("WETH", "0x1234", 18))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("address");
        assertThatThrownBy(() -> new EvmToken("WETH", validAddress(), 256))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("decimals");
    }

    private static String validAddress() {
        return "0x0000000000000000000000000000000000000001";
    }
}
