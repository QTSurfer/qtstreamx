package com.qtsurfer.qtstreamx.evm.rpc;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/** Provider-neutral safe/finalized head comparison for routing eligibility. */
public final class EvmRpcProviderComparison {
    private EvmRpcProviderComparison() {}

    /**
     * Compares the latest successful safe or finalized observations from two reports.
     *
     * <p>Latency is deliberately absent. A stale, wrong-network, or divergent upstream cannot win
     * this correctness comparison because it responded faster.
     *
     * @param left first upstream report
     * @param right second upstream report
     * @param operation {@link EvmRpcProbeOperation#SAFE_BLOCK} or
     *     {@link EvmRpcProbeOperation#FINALIZED_BLOCK}
     * @param maximumLagBlocks accepted non-negative head-number difference
     * @param measuredAt comparison time
     * @param maximumEvidenceAge maximum accepted age for either observation
     * @return classified upstream relationship
     */
    public static EvmRpcProviderRelation compare(
            EvmRpcCapabilityReport left,
            EvmRpcCapabilityReport right,
            EvmRpcProbeOperation operation,
            long maximumLagBlocks,
            Instant measuredAt,
            Duration maximumEvidenceAge) {
        Objects.requireNonNull(left, "left");
        Objects.requireNonNull(right, "right");
        Objects.requireNonNull(operation, "operation");
        Objects.requireNonNull(measuredAt, "measuredAt");
        Objects.requireNonNull(maximumEvidenceAge, "maximumEvidenceAge");
        if (operation != EvmRpcProbeOperation.SAFE_BLOCK
                && operation != EvmRpcProbeOperation.FINALIZED_BLOCK) {
            throw new IllegalArgumentException("operation must identify a safe or finalized block");
        }
        if (maximumLagBlocks < 0) {
            throw new IllegalArgumentException("maximumLagBlocks must be non-negative");
        }
        if (maximumEvidenceAge.isNegative()) {
            throw new IllegalArgumentException("maximumEvidenceAge must be non-negative");
        }
        if (!left.network().equals(right.network())
                || !left.supports(EvmRpcProbePurpose.NETWORK)
                || !right.supports(EvmRpcProbePurpose.NETWORK)) {
            return EvmRpcProviderRelation.WRONG_NETWORK;
        }

        Instant oldestAccepted = measuredAt.minus(maximumEvidenceAge);
        Optional<EvmRpcProbeObservation> leftHead = head(left, operation, oldestAccepted, measuredAt);
        Optional<EvmRpcProbeObservation> rightHead = head(right, operation, oldestAccepted, measuredAt);
        if (leftHead.isEmpty() || rightHead.isEmpty()) {
            return EvmRpcProviderRelation.UNKNOWN;
        }

        long leftNumber = leftHead.orElseThrow().fromBlock().orElseThrow();
        long rightNumber = rightHead.orElseThrow().fromBlock().orElseThrow();
        if (leftNumber == rightNumber) {
            String leftHash = leftHead.orElseThrow().blockHash();
            String rightHash = rightHead.orElseThrow().blockHash();
            if (leftHash == null || rightHash == null) {
                return EvmRpcProviderRelation.UNKNOWN;
            }
            return leftHash.equals(rightHash)
                    ? EvmRpcProviderRelation.CONSISTENT
                    : EvmRpcProviderRelation.DIVERGENT_HASH;
        }

        long lag = leftNumber > rightNumber
                ? leftNumber - rightNumber
                : rightNumber - leftNumber;
        if (lag <= maximumLagBlocks) {
            return EvmRpcProviderRelation.UNKNOWN;
        }
        return leftNumber < rightNumber
                ? EvmRpcProviderRelation.LEFT_STALE
                : EvmRpcProviderRelation.RIGHT_STALE;
    }

    private static Optional<EvmRpcProbeObservation> head(
            EvmRpcCapabilityReport report,
            EvmRpcProbeOperation operation,
            Instant oldestAccepted,
            Instant measuredAt) {
        return report.observations().stream()
                .filter(observation -> observation.operation() == operation)
                .filter(observation -> observation.status() == EvmRpcProbeStatus.SUPPORTED)
                .filter(observation -> observation.fromBlock().isPresent())
                .filter(observation -> !observation.measuredAt().isBefore(oldestAccepted))
                .filter(observation -> !observation.measuredAt().isAfter(measuredAt))
                .reduce((first, second) -> second);
    }
}
