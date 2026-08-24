package com.qtsurfer.qtstreamx.evm.rpc;

/** Terminal signal that an upstream safe head is behind the validated checkpoint. */
public final class EvmStaleHeadException extends RuntimeException {

    /**
     * Creates an endpoint-free stale-upstream error.
     *
     * @param streamId logical stream identity
     * @param safeBlock upstream safe head
     * @param checkpointBlock validated checkpoint height
     * @param upstreamId safe current-upstream alias
     */
    public EvmStaleHeadException(
            EvmLogStreamId streamId,
            long safeBlock,
            long checkpointBlock,
            String upstreamId) {
        super("Safe head " + safeBlock
                + " for stream " + streamId.streamKey()
                + " on " + streamId.network()
                + " via " + upstreamId
                + " is behind checkpoint " + checkpointBlock);
    }
}
