package com.qtsurfer.qtstreamx.evm.rpc;

import java.io.IOException;
import java.time.Duration;

@FunctionalInterface
interface JsonRpcHttpTransport {
    String post(String request, Duration timeout) throws IOException, InterruptedException;
}
