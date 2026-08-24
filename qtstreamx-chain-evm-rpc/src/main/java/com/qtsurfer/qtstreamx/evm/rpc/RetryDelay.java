package com.qtsurfer.qtstreamx.evm.rpc;

import java.time.Duration;

@FunctionalInterface
interface RetryDelay {
    void await(Duration duration) throws InterruptedException;
}
