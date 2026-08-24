package com.qtsurfer.qtstreamx.evm.rpc;


/** Endpoint-free terminal failure labels for supervised capture metrics. */
public enum ActivePassiveEvmTerminalReason {
    GAP_EXHAUSTED,
    STALE_HEAD,
    CHECKPOINT_MISMATCH,
    CHECKPOINT_LOAD,
    OVERLAP_VALIDATION,
    BATCH_DELIVERY,
    CHECKPOINT_SAVE,
    UPSTREAM_EXHAUSTED,
    UNCLASSIFIED;

    public static ActivePassiveEvmTerminalReason classify(Throwable failure) {
        if (failure instanceof EvmRecoveryGapException) {
            return GAP_EXHAUSTED;
        }
        if (failure instanceof EvmStaleHeadException) {
            return STALE_HEAD;
        }
        if (failure instanceof EvmCheckpointMismatchException) {
            return CHECKPOINT_MISMATCH;
        }
        if (failure instanceof EvmRecoveryTransitionException transition) {
            return switch (transition.stage()) {
                case CHECKPOINT_LOAD -> CHECKPOINT_LOAD;
                case OVERLAP_VALIDATION -> OVERLAP_VALIDATION;
                case BATCH_DELIVERY -> BATCH_DELIVERY;
                case CHECKPOINT_SAVE -> CHECKPOINT_SAVE;
                case UPSTREAM_RECOVERY -> UPSTREAM_EXHAUSTED;
            };
        }
        return UNCLASSIFIED;
    }
}
