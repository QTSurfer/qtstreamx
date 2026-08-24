package com.qtsurfer.qtstreamx.evm.rpc;

import java.util.function.Consumer;

/** Streams ordered, confirmation-gated logs from one EVM network. */
public interface EvmLogStream extends AutoCloseable {

    /**
     * Starts streaming logs matching the filter.
     *
     * @param filter contracts and event topics to observe
     * @param handler non-blocking confirmed-log consumer
     * @throws Exception when the initial connection cannot be established
     */
    void start(EvmLogFilter filter, Consumer<EvmLog> handler) throws Exception;

    /**
     * Starts streaming with durable, explicitly acknowledged batch recovery.
     *
     * <p>Implementations that only support the legacy per-log callback may retain the default
     * unsupported behavior.
     *
     * @param filter contracts and event topics to observe
     * @param handler idempotent confirmed-batch handler
     * @throws Exception when checkpoint restore, catch-up, or initial connection fails
     */
    default void startRecoverable(EvmLogFilter filter, EvmLogBatchHandler handler) throws Exception {
        throw new UnsupportedOperationException("Durable EVM log recovery is not supported");
    }

    /**
     * Registers the terminal asynchronous error handler.
     *
     * @param handler error consumer
     */
    void onError(Consumer<Throwable> handler);

    /**
     * Returns whether the live WebSocket transport is open.
     *
     * @return {@code true} while the live transport is open
     */
    boolean isConnected();

    /**
     * Returns the current recovery and transport lifecycle state.
     *
     * @return observable stream status
     */
    default EvmLogStreamStatus status() {
        return isConnected() ? EvmLogStreamStatus.LIVE : EvmLogStreamStatus.IDLE;
    }

    /**
     * Returns endpoint-free recovery metrics for this stream.
     *
     * @return current immutable metrics snapshot
     */
    default EvmLogStreamMetrics recoveryMetrics() {
        return EvmLogStreamMetrics.empty();
    }
}
