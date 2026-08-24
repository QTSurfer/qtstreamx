package com.qtsurfer.qtstreamx.evm.rpc;

/** Builds JSON-RPC response envelopes for unit tests. */
final class JsonRpcTestResponses {

    private JsonRpcTestResponses() {}

    static String result(String result) {
        return result(1, result);
    }

    static String result(long id, String result) {
        return """
                {"jsonrpc":"2.0","id":%d,"result":%s}
                """.formatted(id, result);
    }

    static String error(int code, String message) {
        return """
                {"jsonrpc":"2.0","id":1,"error":{"code":%d,"message":"%s"}}
                """.formatted(code, message);
    }
}
