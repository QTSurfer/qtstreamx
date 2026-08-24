package com.qtsurfer.qtstreamx.evm.rpc;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * Immutable, endpoint-free capability evidence for one upstream alias and network.
 *
 * @param upstreamId opaque operator alias containing no URL or credential material
 * @param network CAIP-2 network identifier
 * @param startedAt probe start time
 * @param finishedAt probe completion time
 * @param observations immutable operation evidence
 */
public record EvmRpcCapabilityReport(
        String upstreamId,
        String network,
        Instant startedAt,
        Instant finishedAt,
        List<EvmRpcProbeObservation> observations
) {
    private static final String SAFE_ALIAS = "[a-z][a-z0-9-]{0,62}";
    private static final String CAIP_2 = "[-a-z0-9]{3,8}:[-_a-zA-Z0-9]{1,32}";

    /** Validates identifiers and snapshots all capability observations. */
    public EvmRpcCapabilityReport {
        Objects.requireNonNull(upstreamId, "upstreamId");
        Objects.requireNonNull(network, "network");
        Objects.requireNonNull(startedAt, "startedAt");
        Objects.requireNonNull(finishedAt, "finishedAt");
        Objects.requireNonNull(observations, "observations");
        if (!upstreamId.matches(SAFE_ALIAS)) {
            throw new IllegalArgumentException("upstreamId must be a lowercase opaque alias");
        }
        if (!network.matches(CAIP_2)) {
            throw new IllegalArgumentException("network must be a CAIP-2 identifier");
        }
        if (finishedAt.isBefore(startedAt)) {
            throw new IllegalArgumentException("finishedAt must not precede startedAt");
        }
        observations = List.copyOf(observations);
    }

    /**
     * Returns whether every observation for a purpose completed successfully.
     *
     * @param purpose request purpose to inspect
     * @return {@code true} when at least one observation exists and all are supported
     */
    public boolean supports(EvmRpcProbePurpose purpose) {
        Objects.requireNonNull(purpose, "purpose");
        List<EvmRpcProbeObservation> matching = observations.stream()
                .filter(observation -> observation.purpose() == purpose)
                .toList();
        return !matching.isEmpty()
                && matching.stream().allMatch(
                        observation -> observation.status() == EvmRpcProbeStatus.SUPPORTED);
    }

    /**
     * Returns the oldest block proven for discovery log access.
     *
     * @return oldest successful discovery-log block, or empty when unproven
     */
    public OptionalLong earliestProvenLogBlock() {
        return observations.stream()
                .filter(observation -> observation.purpose() == EvmRpcProbePurpose.DISCOVERY_LOGS)
                .filter(observation -> observation.status() == EvmRpcProbeStatus.SUPPORTED)
                .map(EvmRpcProbeObservation::fromBlock)
                .filter(OptionalLong::isPresent)
                .mapToLong(OptionalLong::getAsLong)
                .min();
    }

    /**
     * Returns the exact block proven for both historical call and bytecode access.
     *
     * @return proven historical-state block, or empty when either operation failed
     */
    public OptionalLong earliestProvenStateBlock() {
        if (!supports(EvmRpcProbePurpose.HISTORICAL_STATE)) {
            return OptionalLong.empty();
        }
        return observations.stream()
                .filter(observation -> observation.purpose() == EvmRpcProbePurpose.HISTORICAL_STATE)
                .map(EvmRpcProbeObservation::fromBlock)
                .filter(OptionalLong::isPresent)
                .mapToLong(OptionalLong::getAsLong)
                .min();
    }

    /**
     * Returns the widest single log interval completed without pagination or bisection.
     *
     * @return maximum proven inclusive block count, or empty when no direct log probe succeeded
     */
    public OptionalLong maximumProvenLogRange() {
        return observations.stream()
                .filter(observation -> observation.operation() == EvmRpcProbeOperation.GET_LOGS)
                .filter(observation -> observation.status() == EvmRpcProbeStatus.SUPPORTED)
                .filter(observation -> observation.fromBlock().isPresent())
                .filter(observation -> observation.toBlock().isPresent())
                .mapToLong(observation -> Math.addExact(
                        Math.subtractExact(
                                observation.toBlock().getAsLong(),
                                observation.fromBlock().getAsLong()),
                        1L))
                .max();
    }

    /**
     * Combines HTTP and WebSocket evidence for the same upstream alias and network.
     *
     * @param other additional capability report
     * @return immutable report spanning both observation sets
     * @throws IllegalArgumentException when the reports identify different upstreams or networks
     */
    public EvmRpcCapabilityReport merge(EvmRpcCapabilityReport other) {
        Objects.requireNonNull(other, "other");
        if (!upstreamId.equals(other.upstreamId) || !network.equals(other.network)) {
            throw new IllegalArgumentException("reports must identify the same upstream and network");
        }
        List<EvmRpcProbeObservation> merged = new java.util.ArrayList<>(observations);
        merged.addAll(other.observations);
        return new EvmRpcCapabilityReport(
                upstreamId,
                network,
                startedAt.isBefore(other.startedAt) ? startedAt : other.startedAt,
                finishedAt.isAfter(other.finishedAt) ? finishedAt : other.finishedAt,
                merged);
    }
}
