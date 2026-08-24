package com.qtsurfer.qtstreamx.evm.rpc;

import java.time.Duration;
import java.util.Objects;

/**
 * Runtime configuration for read-only EVM JSON-RPC access.
 *
 * @param network stable network identifier independent from the endpoint
 * @param httpUrl HTTP JSON-RPC endpoint
 * @param maxBlockRange maximum blocks requested in one log query
 * @param requestTimeout timeout for one RPC request
 * @param maxRetries maximum transient transport retries per request
 */
public record EvmRpcReaderConfig(
        String network,
        String httpUrl,
        int maxBlockRange,
        Duration requestTimeout,
        int maxRetries
) implements EvmRpcRequestConfig {

    /** Validates the reader configuration. */
    public EvmRpcReaderConfig {
        Objects.requireNonNull(network, "network");
        Objects.requireNonNull(httpUrl, "httpUrl");
        Objects.requireNonNull(requestTimeout, "requestTimeout");
        if (network.isBlank()) {
            throw new IllegalArgumentException("network must not be blank");
        }
        if (httpUrl.isBlank()) {
            throw new IllegalArgumentException("httpUrl must not be blank");
        }
        if (maxBlockRange <= 0) {
            throw new IllegalArgumentException("maxBlockRange must be positive");
        }
        if (requestTimeout.isZero() || requestTimeout.isNegative()) {
            throw new IllegalArgumentException("requestTimeout must be positive");
        }
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be non-negative");
        }
    }

    /** Returns a diagnostic description with the endpoint redacted. */
    @Override
    public String toString() {
        return "EvmRpcReaderConfig[network=" + network
                + ", httpUrl=<redacted>"
                + ", maxBlockRange=" + maxBlockRange
                + ", requestTimeout=" + requestTimeout
                + ", maxRetries=" + maxRetries
                + "]";
    }
}
