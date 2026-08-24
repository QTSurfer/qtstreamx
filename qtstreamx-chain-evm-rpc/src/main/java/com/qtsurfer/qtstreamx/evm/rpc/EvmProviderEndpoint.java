package com.qtsurfer.qtstreamx.evm.rpc;

import java.util.Objects;

/** Runtime-only HTTP and WebSocket endpoints attributed to one opaque provider alias. */
public record EvmProviderEndpoint(String upstreamId, String httpUrl, String webSocketUrl) {
    private static final String SAFE_ALIAS = "[a-z][a-z0-9-]{0,62}";

    public EvmProviderEndpoint {
        Objects.requireNonNull(upstreamId, "upstreamId");
        Objects.requireNonNull(httpUrl, "httpUrl");
        Objects.requireNonNull(webSocketUrl, "webSocketUrl");
        if (!upstreamId.matches(SAFE_ALIAS)) {
            throw new IllegalArgumentException("upstreamId must be a lowercase opaque alias");
        }
        if (httpUrl.isBlank() || webSocketUrl.isBlank()) {
            throw new IllegalArgumentException("provider endpoints must not be blank");
        }
    }

    @Override
    public String toString() {
        return "EvmProviderEndpoint[upstreamId=" + upstreamId
                + ", httpUrl=<redacted>, webSocketUrl=<redacted>]";
    }
}
