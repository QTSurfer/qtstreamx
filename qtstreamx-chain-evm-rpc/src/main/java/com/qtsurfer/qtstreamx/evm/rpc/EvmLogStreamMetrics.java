package com.qtsurfer.qtstreamx.evm.rpc;

/**
 * Endpoint-free recovery counters and the current cursor lag for one EVM log stream.
 *
 * @param cursorLagBlocks confirmed safe blocks not yet committed
 * @param recoveryPages HTTP log-range requests, including range bisection
 * @param retries reconnect attempts scheduled after a failed recovery
 * @param gaps recovery gaps rejected by the configured replay ceiling
 * @param reorgs removed or non-canonical log observations
 * @param duplicateSuppressions repeated log observations discarded before delivery
 * @param terminalFailures failures that made the stream terminal
 */
public record EvmLogStreamMetrics(
        long cursorLagBlocks,
        long recoveryPages,
        long retries,
        long gaps,
        long reorgs,
        long duplicateSuppressions,
        long terminalFailures
) {
    /** Validates that every gauge and counter is non-negative. */
    public EvmLogStreamMetrics {
        if (cursorLagBlocks < 0
                || recoveryPages < 0
                || retries < 0
                || gaps < 0
                || reorgs < 0
                || duplicateSuppressions < 0
                || terminalFailures < 0) {
            throw new IllegalArgumentException("EVM log stream metrics must be non-negative");
        }
    }

    /**
     * Returns an empty snapshot for implementations without recovery instrumentation.
     *
     * @return zero-valued metrics
     */
    public static EvmLogStreamMetrics empty() {
        return new EvmLogStreamMetrics(0, 0, 0, 0, 0, 0, 0);
    }
}
