package com.qtsurfer.qtstreamx.canary;

import com.qtsurfer.qtstreamx.evm.rpc.EvmProviderBundle;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class EvmProviderBundleTest {

    @Test
    void diagnosticDescriptionRedactsBothRuntimeEndpoints() {
        EvmProviderBundle bundle = new EvmProviderBundle(
                "primary",
                "https://rpc.invalid/secret-http-token",
                "wss://rpc.invalid/secret-ws-token",
                ActivePassiveEvmLogStreamTest.liveCapabilities("primary"));

        assertThat(bundle.toString())
                .contains("primary", "<redacted>")
                .doesNotContain("rpc.invalid", "secret-http-token", "secret-ws-token");
    }
}
