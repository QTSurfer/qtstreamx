package com.qtsurfer.qtstreamx.evm.rpc;

import java.time.Duration;
import java.util.Objects;

/**
 * Runtime configuration for a bounded EVM WebSocket subscription probe.
 *
 * @param network stable CAIP-2 network identifier independent from the endpoint
 * @param webSocketUrl WebSocket JSON-RPC endpoint
 * @param responseTimeout maximum wait for probe responses
 */
public record EvmRpcWebSocketProbeConfig(
        String network,
        String webSocketUrl,
        Duration responseTimeout
) {
    private static final String CAIP_2 = "[-a-z0-9]{3,8}:[-_a-zA-Z0-9]{1,32}";

    /** Validates the runtime WebSocket probe configuration. */
    public EvmRpcWebSocketProbeConfig {
        Objects.requireNonNull(network, "network");
        Objects.requireNonNull(webSocketUrl, "webSocketUrl");
        Objects.requireNonNull(responseTimeout, "responseTimeout");
        if (!network.matches(CAIP_2)) {
            throw new IllegalArgumentException("network must be a CAIP-2 identifier");
        }
        if (webSocketUrl.isBlank()) {
            throw new IllegalArgumentException("webSocketUrl must not be blank");
        }
        if (responseTimeout.isZero() || responseTimeout.isNegative()) {
            throw new IllegalArgumentException("responseTimeout must be positive");
        }
    }

    /** Returns a diagnostic description with the endpoint redacted. */
    @Override
    public String toString() {
        return "EvmRpcWebSocketProbeConfig[network=" + network
                + ", webSocketUrl=<redacted>"
                + ", responseTimeout=" + responseTimeout
                + "]";
    }
}
