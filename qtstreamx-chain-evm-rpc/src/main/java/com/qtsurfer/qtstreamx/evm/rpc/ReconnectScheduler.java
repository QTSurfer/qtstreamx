package com.qtsurfer.qtstreamx.evm.rpc;

import java.time.Duration;

interface ReconnectScheduler extends AutoCloseable {
    void schedule(Runnable task, Duration delay);

    @Override
    void close();

    static ReconnectScheduler immediate() {
        return new ReconnectScheduler() {
            @Override
            public void schedule(Runnable task, Duration delay) {
                task.run();
            }

            @Override
            public void close() {}
        };
    }
}
