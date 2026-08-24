package com.qtsurfer.qtstreamx.evm.rpc;

import java.util.List;
import java.util.Objects;

/**
 * Ordered confirmed logs and canonical end cursor offered at one acknowledgement boundary.
 *
 * @param streamId stable provider-neutral stream identity
 * @param fromBlock first previously unacknowledged block in this batch
 * @param toBlock last confirmed block in this batch
 * @param toBlockHash canonical hash of the last confirmed block
 * @param logs immutable logs in canonical chain order; may be empty
 */
public record EvmLogBatch(
        EvmLogStreamId streamId,
        long fromBlock,
        long toBlock,
        String toBlockHash,
        List<EvmLog> logs
) {
    /** Validates and snapshots the acknowledgement batch. */
    public EvmLogBatch {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(toBlockHash, "toBlockHash");
        Objects.requireNonNull(logs, "logs");
        if (fromBlock < 0 || toBlock < fromBlock) {
            throw new IllegalArgumentException("batch block range must be non-negative and ordered");
        }
        if (toBlockHash.isBlank()) {
            throw new IllegalArgumentException("toBlockHash must not be blank");
        }
        logs = List.copyOf(logs);
    }
}
