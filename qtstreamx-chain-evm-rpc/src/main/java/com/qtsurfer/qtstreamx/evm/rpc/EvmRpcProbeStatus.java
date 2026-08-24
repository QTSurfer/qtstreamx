package com.qtsurfer.qtstreamx.evm.rpc;

/** Redacted outcome of one bounded provider capability probe. */
public enum EvmRpcProbeStatus {
    /** No safe conclusion can be drawn from the available evidence. */
    UNKNOWN,

    /** The provider completed the operation within the configured bounds. */
    SUPPORTED,

    /** The provider or adapter does not implement the operation. */
    UNSUPPORTED,

    /** The provider rejected the requested log interval. */
    RANGE_REJECTED,

    /** The response exceeded the configured result ceiling. */
    RESULT_LIMIT,

    /** The provider explicitly rate-limited the probe. */
    RATE_LIMITED,

    /** The request did not complete within its configured timeout. */
    TIMEOUT,

    /** The transport failed without exposing provider-controlled text. */
    TRANSPORT_FAILURE,

    /** The response could not be parsed as the expected JSON-RPC result. */
    MALFORMED_RESPONSE,

    /** The endpoint reported a chain other than the configured network. */
    WRONG_NETWORK,

    /** The measured head was outside an accepted lag bound. */
    STALE_HEAD,

    /** Equal block numbers produced inconsistent canonical hashes. */
    DIVERGENT_HASH,

    /** The operation was not run because the predeclared budget was exhausted. */
    BUDGET_EXHAUSTED
}
