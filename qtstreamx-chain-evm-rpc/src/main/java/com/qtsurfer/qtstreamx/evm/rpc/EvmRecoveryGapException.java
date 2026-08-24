package com.qtsurfer.qtstreamx.evm.rpc;

/** Terminal signal that bounded recovery cannot cover the missing safe interval. */
public final class EvmRecoveryGapException extends RuntimeException {

    /**
     * Creates an endpoint-free recovery error.
     *
     * @param streamId logical stream identity
     * @param missingBlocks number of uncheckpointed safe blocks
     * @param maxReplayBlocks configured replay ceiling
     * @param upstreamId safe current-upstream alias
     */
    public EvmRecoveryGapException(
            EvmLogStreamId streamId,
            long missingBlocks,
            long maxReplayBlocks,
            String upstreamId) {
        super("Recovery gap for stream " + streamId.streamKey()
                + " on " + streamId.network()
                + " via " + upstreamId
                + " is " + missingBlocks
                + " blocks, exceeding limit " + maxReplayBlocks);
    }
}
