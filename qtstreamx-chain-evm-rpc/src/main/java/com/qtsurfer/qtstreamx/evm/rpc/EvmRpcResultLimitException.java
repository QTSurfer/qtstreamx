package com.qtsurfer.qtstreamx.evm.rpc;

final class EvmRpcResultLimitException extends RuntimeException {
    EvmRpcResultLimitException() {
        super("JSON-RPC result exceeded the configured limit");
    }
}
