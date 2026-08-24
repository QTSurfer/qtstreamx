package com.qtsurfer.qtstreamx.evm.rpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class EvmBlockTagTest {

    @Test
    void exposesOnlyValidatedNumericAndNamedTags() {
        assertThat(EvmBlockTag.number(123)).hasToString("0x7b");
        assertThat(EvmBlockTag.latest()).hasToString("latest");
        assertThat(EvmBlockTag.safe()).hasToString("safe");
        assertThat(EvmBlockTag.finalized()).hasToString("finalized");
        assertThatThrownBy(() -> EvmBlockTag.number(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-negative");
    }
}
