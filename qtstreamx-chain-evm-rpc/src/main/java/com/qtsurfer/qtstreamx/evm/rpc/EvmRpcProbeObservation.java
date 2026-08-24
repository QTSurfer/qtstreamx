package com.qtsurfer.qtstreamx.evm.rpc;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.OptionalLong;

/**
 * Redacted evidence from one provider operation.
 *
 * <p>Free-form provider text, endpoint data, return bytes, and subscription identifiers cannot be
 * represented by this type.
 *
 * @param transport transport used by the operation
 * @param operation measured JSON-RPC operation
 * @param purpose correctness purpose of the observation
 * @param status classified outcome
 * @param fromBlock first relevant block, when applicable
 * @param toBlock last relevant block, when applicable
 * @param blockHash canonical block hash, only for head/finality observations
 * @param resultCount returned item count, when applicable
 * @param rpcErrorCode numeric JSON-RPC error code, when available
 * @param measuredAt UTC instant at which the operation completed
 * @param elapsed elapsed time for the operation
 */
public record EvmRpcProbeObservation(
        EvmRpcTransport transport,
        EvmRpcProbeOperation operation,
        EvmRpcProbePurpose purpose,
        EvmRpcProbeStatus status,
        OptionalLong fromBlock,
        OptionalLong toBlock,
        String blockHash,
        OptionalInt resultCount,
        OptionalInt rpcErrorCode,
        Instant measuredAt,
        Duration elapsed
) {
    /** Validates the closed, public-safe observation shape. */
    public EvmRpcProbeObservation {
        Objects.requireNonNull(transport, "transport");
        Objects.requireNonNull(operation, "operation");
        Objects.requireNonNull(purpose, "purpose");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(fromBlock, "fromBlock");
        Objects.requireNonNull(toBlock, "toBlock");
        Objects.requireNonNull(resultCount, "resultCount");
        Objects.requireNonNull(rpcErrorCode, "rpcErrorCode");
        Objects.requireNonNull(measuredAt, "measuredAt");
        Objects.requireNonNull(elapsed, "elapsed");
        if (elapsed.isNegative()) {
            throw new IllegalArgumentException("elapsed must be non-negative");
        }
        if (blockHash != null && !blockHash.matches("0x[0-9a-fA-F]{64}")) {
            throw new IllegalArgumentException("blockHash must be null or a 32-byte hex value");
        }
    }
}
