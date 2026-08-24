package com.qtsurfer.qtstreamx.evm.rpc;

import java.util.Objects;

/**
 * Last explicitly acknowledged canonical block for a logical log stream.
 *
 * @param streamId stable provider-neutral stream identity
 * @param blockNumber acknowledged block number
 * @param blockHash canonical hash observed at acknowledgement time
 */
public record EvmLogCheckpoint(
        EvmLogStreamId streamId,
        long blockNumber,
        String blockHash
) {
    /** Validates the checkpoint value. */
    public EvmLogCheckpoint {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(blockHash, "blockHash");
        if (blockNumber < 0) {
            throw new IllegalArgumentException("blockNumber must be non-negative");
        }
        if (blockHash.isBlank()) {
            throw new IllegalArgumentException("blockHash must not be blank");
        }
    }
}
