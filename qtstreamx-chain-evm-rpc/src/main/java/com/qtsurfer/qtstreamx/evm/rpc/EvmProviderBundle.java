package com.qtsurfer.qtstreamx.evm.rpc;

import java.util.Objects;

/** Runtime HTTP and WebSocket endpoints attributed to one opaque provider alias. */
public record EvmProviderBundle(
        String upstreamId,
        String httpUrl,
        String webSocketUrl,
        EvmRpcCapabilityReport capabilities
) {
    private static final String SAFE_ALIAS = "[a-z][a-z0-9-]{0,62}";

    public EvmProviderBundle {
        Objects.requireNonNull(upstreamId, "upstreamId");
        Objects.requireNonNull(httpUrl, "httpUrl");
        Objects.requireNonNull(webSocketUrl, "webSocketUrl");
        Objects.requireNonNull(capabilities, "capabilities");
        if (!upstreamId.matches(SAFE_ALIAS)) {
            throw new IllegalArgumentException("upstreamId must be a lowercase opaque alias");
        }
        if (httpUrl.isBlank() || webSocketUrl.isBlank()) {
            throw new IllegalArgumentException("provider endpoints must not be blank");
        }
        if (!upstreamId.equals(capabilities.upstreamId())) {
            throw new IllegalArgumentException("capability report must match the provider alias");
        }
    }

    @Override
    public String toString() {
        return "EvmProviderBundle[upstreamId=" + upstreamId
                + ", httpUrl=<redacted>, webSocketUrl=<redacted>]";
    }
}
