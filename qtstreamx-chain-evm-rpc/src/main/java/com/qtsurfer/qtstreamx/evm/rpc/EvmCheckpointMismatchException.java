package com.qtsurfer.qtstreamx.evm.rpc;

/** Terminal signal that a stored checkpoint no longer matches the canonical chain. */
public final class EvmCheckpointMismatchException extends RuntimeException {

    /**
     * Creates an endpoint-free checkpoint divergence error.
     *
     * @param streamId logical stream identity
     * @param blockNumber divergent checkpoint block
     * @param upstreamId safe current-upstream alias
     */
    public EvmCheckpointMismatchException(
            EvmLogStreamId streamId,
            long blockNumber,
            String upstreamId) {
        super("Checkpoint mismatch for stream " + streamId.streamKey()
                + " on " + streamId.network()
                + " at block " + blockNumber
                + " via " + upstreamId);
    }
}
