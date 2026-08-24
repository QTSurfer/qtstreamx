package com.qtsurfer.qtstreamx.evm.rpc;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

final class ScheduledReconnectScheduler implements ReconnectScheduler {
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "qtstreamx-evm-reconnect");
        thread.setDaemon(true);
        return thread;
    });

    @Override
    public void schedule(Runnable task, Duration delay) {
        executor.schedule(task, delay.toNanos(), TimeUnit.NANOSECONDS);
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
