package com.qtsurfer.qtstreamx.evm.rpc;

import java.time.Duration;
import java.util.Objects;

/**
 * Runtime configuration for a confirmed EVM log stream.
 *
 * @param network stable network identifier independent from the endpoint
 * @param webSocketUrl WebSocket JSON-RPC endpoint
 * @param httpUrl HTTP JSON-RPC endpoint
 * @param startBlock first block eligible for catch-up
 * @param confirmationDepth required canonical descendant blocks
 * @param maxBlockRange maximum blocks requested in one catch-up query
 * @param requestTimeout timeout for one RPC request
 * @param maxRetries maximum transient retries per operation
 */
public record EvmLogStreamConfig(
        String network,
        String webSocketUrl,
        String httpUrl,
        long startBlock,
        int confirmationDepth,
        int maxBlockRange,
        Duration requestTimeout,
        int maxRetries
) implements EvmRpcRequestConfig {
    /** Validates the stream configuration. */
    public EvmLogStreamConfig {
        Objects.requireNonNull(network, "network");
        Objects.requireNonNull(webSocketUrl, "webSocketUrl");
        Objects.requireNonNull(httpUrl, "httpUrl");
        Objects.requireNonNull(requestTimeout, "requestTimeout");
        if (confirmationDepth < 0) {
            throw new IllegalArgumentException("confirmationDepth must be non-negative");
        }
        if (maxBlockRange <= 0) {
            throw new IllegalArgumentException("maxBlockRange must be positive");
        }
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be non-negative");
        }
    }

    /** Returns a diagnostic description with both RPC endpoints redacted. */
    @Override
    public String toString() {
        return "EvmLogStreamConfig[network=" + network
                + ", webSocketUrl=<redacted>"
                + ", httpUrl=<redacted>"
                + ", startBlock=" + startBlock
                + ", confirmationDepth=" + confirmationDepth
                + ", maxBlockRange=" + maxBlockRange
                + ", requestTimeout=" + requestTimeout
                + ", maxRetries=" + maxRetries
                + "]";
    }
}
