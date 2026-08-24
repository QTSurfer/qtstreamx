package com.qtsurfer.qtstreamx.evm.rpc;

/** Explicit operation scope for a bounded HTTP capability probe. */
public enum EvmRpcProbeScope {
    /** Network, head/finality, and current-state checks only. */
    STARTUP,

    /** Purpose-minimal live, recovery, discovery, and historical checks for one routed bundle. */
    ROUTE,

    /** Startup checks plus recent logs, old logs, and exact historical state. */
    FULL
}
