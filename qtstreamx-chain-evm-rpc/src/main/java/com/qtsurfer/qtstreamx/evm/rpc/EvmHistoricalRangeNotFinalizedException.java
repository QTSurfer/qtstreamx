package com.qtsurfer.qtstreamx.evm.rpc;

/**
 * Terminal signal that a requested bounded historical range extends past the confirmed safe head.
 *
 * <p>This is deliberately a different condition from {@link EvmRecoveryGapException}: that guard
 * protects a live tailing stream from silently skipping a backlog it fell behind on. A bounded
 * historical replay has no live tail and no backlog to fall behind on — it is asking for blocks
 * that have not been confirmed yet, which is a request-shape error, not a recovery failure.
 */
public final class EvmHistoricalRangeNotFinalizedException extends RuntimeException {

    /**
     * Creates an endpoint-free historical-range error.
     *
     * @param network stable network identifier independent from the endpoint
     * @param toBlock last block requested, inclusive
     * @param safeBlock last block currently confirmed
     */
    public EvmHistoricalRangeNotFinalizedException(String network, long toBlock, long safeBlock) {
        super("Requested historical range on " + network
                + " ends at block " + toBlock
                + " but only " + safeBlock
                + " is confirmed safe");
    }
}
