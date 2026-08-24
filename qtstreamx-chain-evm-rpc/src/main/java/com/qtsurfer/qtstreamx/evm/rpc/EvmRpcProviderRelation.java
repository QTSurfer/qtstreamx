package com.qtsurfer.qtstreamx.evm.rpc;

/** Classified relationship between two upstreams' safe or finalized heads. */
public enum EvmRpcProviderRelation {
    /** Both upstreams reported the same block number and canonical hash. */
    CONSISTENT,

    /** The left upstream is behind the accepted lag bound. */
    LEFT_STALE,

    /** The right upstream is behind the accepted lag bound. */
    RIGHT_STALE,

    /** Equal block numbers have different canonical hashes. */
    DIVERGENT_HASH,

    /** At least one upstream failed its configured network check. */
    WRONG_NETWORK,

    /** The available observations cannot support a safe conclusion. */
    UNKNOWN
}
