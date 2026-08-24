package com.qtsurfer.qtstreamx.evm.rpc;

/** A JSON-RPC protocol error with provider-controlled details removed from diagnostics. */
public final class EvmRpcException extends RuntimeException {
    /** Provider-supplied JSON-RPC error code. */
    private final int code;

    EvmRpcException(int code) {
        super("JSON-RPC request failed with code " + code);
        this.code = code;
    }

    /**
     * Returns the JSON-RPC error code supplied by the provider.
     *
     * @return protocol error code
     */
    public int code() {
        return code;
    }
}
