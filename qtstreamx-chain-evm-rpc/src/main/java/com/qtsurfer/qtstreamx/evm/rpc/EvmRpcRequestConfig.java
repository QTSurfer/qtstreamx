package com.qtsurfer.qtstreamx.evm.rpc;

import java.time.Duration;

interface EvmRpcRequestConfig {
    Duration requestTimeout();

    int maxRetries();
}
