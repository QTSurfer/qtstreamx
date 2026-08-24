package com.qtsurfer.qtstreamx.evm.rpc;

import java.util.Objects;

/** Endpoint-free terminal failure at a durable recovery transition. */
public final class EvmRecoveryTransitionException extends RuntimeException {

    /** Safe transition stages suitable for metrics labels. */
    public enum Stage {
        /** The durable checkpoint could not be loaded. */
        CHECKPOINT_LOAD,
        /** Replayed overlap data disagreed with canonical block data. */
        OVERLAP_VALIDATION,
        /** The downstream batch effect failed or was rejected. */
        BATCH_DELIVERY,
        /** The acknowledged cursor could not be persisted. */
        CHECKPOINT_SAVE,
        /** An unclassified upstream recovery operation failed. */
        UPSTREAM_RECOVERY
    }

    /** Safe failed-stage classification. */
    private final Stage stage;

    /**
     * Creates a safe terminal transition error without retaining a provider-controlled cause.
     *
     * @param stage failed recovery stage
     * @param streamId logical stream identity
     * @param upstreamId safe current-upstream alias
     */
    public EvmRecoveryTransitionException(
            Stage stage,
            EvmLogStreamId streamId,
            String upstreamId) {
        super("Recovery transition " + Objects.requireNonNull(stage, "stage")
                + " failed for stream " + Objects.requireNonNull(streamId, "streamId").streamKey()
                + " on " + streamId.network()
                + " via " + Objects.requireNonNull(upstreamId, "upstreamId"));
        this.stage = stage;
    }

    /**
     * Returns the safe failed-stage classification.
     *
     * @return recovery transition stage
     */
    public Stage stage() {
        return stage;
    }
}
