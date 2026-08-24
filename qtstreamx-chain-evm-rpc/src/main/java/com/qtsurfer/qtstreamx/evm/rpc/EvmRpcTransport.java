package com.qtsurfer.qtstreamx.evm.rpc;

/** Transport used for one EVM JSON-RPC capability observation. */
public enum EvmRpcTransport {
    /** HTTP JSON-RPC request/response transport. */
    HTTP,

    /** WebSocket JSON-RPC request/subscription transport. */
    WEBSOCKET
}
