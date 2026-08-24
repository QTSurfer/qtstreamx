package com.qtsurfer.qtstreamx.canary;

import com.qtsurfer.qtstreamx.evm.rpc.EvmProviderEndpoint;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class EvmProviderEndpointTest {

    @Test
    void neverRendersRuntimeEndpointsOrCredentials() {
        EvmProviderEndpoint endpoint = new EvmProviderEndpoint(
                "active",
                "https://example.invalid/redacted",
                "wss://example.invalid/redacted");

        assertThat(endpoint.toString())
                .isEqualTo("EvmProviderEndpoint[upstreamId=active, httpUrl=<redacted>, webSocketUrl=<redacted>]")
                .doesNotContain("alice", "secret", "rpc.invalid", "apiKey");
    }
}
