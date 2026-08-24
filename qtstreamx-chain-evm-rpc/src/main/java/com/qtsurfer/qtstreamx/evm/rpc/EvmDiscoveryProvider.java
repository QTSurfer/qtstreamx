package com.qtsurfer.qtstreamx.evm.rpc;

import java.util.Objects;

/** Purpose-selected discovery reader attributed only by its safe upstream alias. */
public record EvmDiscoveryProvider(String upstreamId, EvmRpcReader reader) {
    public EvmDiscoveryProvider {
        Objects.requireNonNull(upstreamId, "upstreamId");
        Objects.requireNonNull(reader, "reader");
    }
}
