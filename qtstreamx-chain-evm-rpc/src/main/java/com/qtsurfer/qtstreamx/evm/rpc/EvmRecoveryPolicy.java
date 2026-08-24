package com.qtsurfer.qtstreamx.evm.rpc;

import java.util.Objects;

/**
 * Bounded durable-recovery policy for one logical stream and current upstream.
 *
 * @param streamId stable provider-neutral stream identity
 * @param upstreamId opaque alias used for safe metrics attribution
 * @param overlapBlocks number of already acknowledged blocks fetched again for verification
 * @param maxReplayBlocks maximum uncheckpointed safe blocks accepted during catch-up
 */
public record EvmRecoveryPolicy(
        EvmLogStreamId streamId,
        String upstreamId,
        int overlapBlocks,
        long maxReplayBlocks
) {
    private static final String SAFE_ALIAS = "[a-z][a-z0-9-]{0,62}";

    /** Validates recovery bounds and the endpoint-free upstream alias. */
    public EvmRecoveryPolicy {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(upstreamId, "upstreamId");
        if (!upstreamId.matches(SAFE_ALIAS)) {
            throw new IllegalArgumentException("upstreamId must be a lowercase opaque alias");
        }
        if (overlapBlocks < 0) {
            throw new IllegalArgumentException("overlapBlocks must be non-negative");
        }
        if (maxReplayBlocks < 1) {
            throw new IllegalArgumentException("maxReplayBlocks must be positive");
        }
    }
}
