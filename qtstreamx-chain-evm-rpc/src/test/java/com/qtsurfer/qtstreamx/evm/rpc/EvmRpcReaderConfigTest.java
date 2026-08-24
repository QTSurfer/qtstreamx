package com.qtsurfer.qtstreamx.evm.rpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class EvmRpcReaderConfigTest {

    @Test
    void redactsTheEndpointFromDiagnostics() {
        EvmRpcReaderConfig config = new EvmRpcReaderConfig(
                "eip155:1",
                "https://user:secret@rpc.invalid/private",
                2_000,
                Duration.ofSeconds(5),
                3);

        assertThat(config.toString())
                .contains("network=eip155:1")
                .contains("httpUrl=<redacted>")
                .doesNotContain("secret")
                .doesNotContain("rpc.invalid");
    }

    @Test
    void rejectsUnsafeRequestBounds() {
        assertThatThrownBy(() -> new EvmRpcReaderConfig(
                "eip155:1",
                "https://rpc.invalid",
                0,
                Duration.ofSeconds(5),
                3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxBlockRange");
    }
}
