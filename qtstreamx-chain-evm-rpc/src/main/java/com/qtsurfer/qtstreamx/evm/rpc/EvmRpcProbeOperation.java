package com.qtsurfer.qtstreamx.evm.rpc;

/** Provider-neutral operation measured by a capability probe. */
public enum EvmRpcProbeOperation {
    /** {@code eth_chainId}. */
    CHAIN_ID,

    /** {@code eth_blockNumber}. */
    BLOCK_NUMBER,

    /** {@code eth_getBlockByNumber} with the {@code safe} tag. */
    SAFE_BLOCK,

    /** {@code eth_getBlockByNumber} with the {@code finalized} tag. */
    FINALIZED_BLOCK,

    /** {@code eth_getLogs}. */
    GET_LOGS,

    /** {@code eth_call}. */
    CALL,

    /** {@code eth_getCode}. */
    GET_CODE,

    /** {@code eth_subscribe} for log notifications. */
    LOG_SUBSCRIPTION,

    /** {@code eth_subscribe} for new-head notifications. */
    NEW_HEADS_SUBSCRIPTION
}
