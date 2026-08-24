package com.qtsurfer.qtstreamx.evm.rpc;

import java.time.Duration;
import java.util.Objects;

/**
 * Hard ceilings applied to one provider/network capability probe.
 *
 * @param maxRequests maximum JSON-RPC requests, including subscription requests
 * @param maxWallClock maximum elapsed time for the whole probe
 * @param maxLogBlockRange maximum inclusive block count across all log observations
 * @param maxReturnedLogs maximum log values accepted across one observation
 */
public record EvmRpcProbeBudget(
        int maxRequests,
        Duration maxWallClock,
        int maxLogBlockRange,
        int maxReturnedLogs
) {
    /** Validates that every ceiling is finite and safe to apply. */
    public EvmRpcProbeBudget {
        Objects.requireNonNull(maxWallClock, "maxWallClock");
        if (maxRequests <= 0) {
            throw new IllegalArgumentException("maxRequests must be positive");
        }
        if (maxWallClock.isZero() || maxWallClock.isNegative()) {
            throw new IllegalArgumentException("maxWallClock must be positive");
        }
        if (maxLogBlockRange <= 0) {
            throw new IllegalArgumentException("maxLogBlockRange must be positive");
        }
        if (maxReturnedLogs <= 0) {
            throw new IllegalArgumentException("maxReturnedLogs must be positive");
        }
    }

    /**
     * Returns the conservative default budget used unless an operator supplies lower ceilings.
     *
     * @return safe default probe budget
     */
    public static EvmRpcProbeBudget safeDefaults() {
        return new EvmRpcProbeBudget(
                12,
                Duration.ofSeconds(45),
                10_000,
                10_000);
    }
}
