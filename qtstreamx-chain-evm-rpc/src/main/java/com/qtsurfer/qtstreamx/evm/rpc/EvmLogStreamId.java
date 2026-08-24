package com.qtsurfer.qtstreamx.evm.rpc;

import java.util.Objects;

/**
 * Stable provider-neutral identity for one logical EVM log stream.
 *
 * @param network CAIP-2 network identifier
 * @param streamKey opaque application-defined market or subscription key
 */
public record EvmLogStreamId(String network, String streamKey) {
    private static final String CAIP_2 = "[-a-z0-9]{3,8}:[-_a-zA-Z0-9]{1,32}";
    private static final String SAFE_STREAM_KEY = "[-_a-zA-Z0-9:.]{1,128}";

    /** Validates the stable identity without accepting endpoint material. */
    public EvmLogStreamId {
        Objects.requireNonNull(network, "network");
        Objects.requireNonNull(streamKey, "streamKey");
        if (!network.matches(CAIP_2)) {
            throw new IllegalArgumentException("network must be a CAIP-2 identifier");
        }
        if (!streamKey.matches(SAFE_STREAM_KEY)) {
            throw new IllegalArgumentException("streamKey must be an opaque safe identifier");
        }
    }
}
