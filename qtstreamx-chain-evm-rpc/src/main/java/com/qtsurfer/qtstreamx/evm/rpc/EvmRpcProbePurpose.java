package com.qtsurfer.qtstreamx.evm.rpc;

/** Correctness purpose for which a provider operation is measured. */
public enum EvmRpcProbePurpose {
    /** Verifies the configured CAIP-2 network. */
    NETWORK,

    /** Measures the provider's current head. */
    HEAD,

    /** Measures named safe/finalized block-tag behavior. */
    FINALITY,

    /** Verifies state reads at a mutable current-state tag. */
    LIVE_STATE,

    /** Verifies recent logs needed for bounded recovery. */
    RECOVERY_LOGS,

    /** Verifies old logs needed for factory discovery. */
    DISCOVERY_LOGS,

    /** Verifies state reads at an exact historical block. */
    HISTORICAL_STATE,

    /** Verifies low-latency live subscription hints. */
    LIVE_SUBSCRIPTION
}
