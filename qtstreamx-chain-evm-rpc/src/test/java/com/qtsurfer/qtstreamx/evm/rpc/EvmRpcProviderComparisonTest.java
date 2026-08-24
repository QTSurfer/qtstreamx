package com.qtsurfer.qtstreamx.evm.rpc;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class EvmRpcProviderComparisonTest {
    private static final String HASH_A =
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    private static final String HASH_B =
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    @Test
    void classifiesConsistentDivergentAndStaleHeads() {
        EvmRpcCapabilityReport left = report("left", 100, HASH_A, EvmRpcProbeStatus.SUPPORTED);

        assertThat(EvmRpcProviderComparison.compare(
                        left,
                        report("right", 100, HASH_A, EvmRpcProbeStatus.SUPPORTED),
                        EvmRpcProbeOperation.SAFE_BLOCK,
                        2,
                        Instant.EPOCH,
                        Duration.ZERO))
                .isEqualTo(EvmRpcProviderRelation.CONSISTENT);
        assertThat(EvmRpcProviderComparison.compare(
                        left,
                        report("right", 100, HASH_B, EvmRpcProbeStatus.SUPPORTED),
                        EvmRpcProbeOperation.SAFE_BLOCK,
                        2,
                        Instant.EPOCH,
                        Duration.ZERO))
                .isEqualTo(EvmRpcProviderRelation.DIVERGENT_HASH);
        assertThat(EvmRpcProviderComparison.compare(
                        left,
                        report("right", 105, HASH_B, EvmRpcProbeStatus.SUPPORTED),
                        EvmRpcProbeOperation.SAFE_BLOCK,
                        2,
                        Instant.EPOCH,
                        Duration.ZERO))
                .isEqualTo(EvmRpcProviderRelation.LEFT_STALE);
        assertThat(EvmRpcProviderComparison.compare(
                        left,
                        report("right", 102, HASH_B, EvmRpcProbeStatus.SUPPORTED),
                        EvmRpcProbeOperation.SAFE_BLOCK,
                        2,
                        Instant.EPOCH,
                        Duration.ZERO))
                .isEqualTo(EvmRpcProviderRelation.UNKNOWN);
    }

    @Test
    void rejectsAReportWhoseNetworkProbeFailed() {
        assertThat(EvmRpcProviderComparison.compare(
                        report("left", 100, HASH_A, EvmRpcProbeStatus.WRONG_NETWORK),
                        report("right", 100, HASH_A, EvmRpcProbeStatus.SUPPORTED),
                        EvmRpcProbeOperation.SAFE_BLOCK,
                        2,
                        Instant.EPOCH,
                        Duration.ZERO))
                .isEqualTo(EvmRpcProviderRelation.WRONG_NETWORK);
    }

    @Test
    void rejectsStaleEvidence() {
        assertThat(EvmRpcProviderComparison.compare(
                        report("left", 100, HASH_A, EvmRpcProbeStatus.SUPPORTED),
                        report("right", 100, HASH_A, EvmRpcProbeStatus.SUPPORTED),
                        EvmRpcProbeOperation.SAFE_BLOCK,
                        2,
                        Instant.EPOCH.plusSeconds(2),
                        Duration.ofSeconds(1)))
                .isEqualTo(EvmRpcProviderRelation.UNKNOWN);
    }

    private static EvmRpcCapabilityReport report(
            String upstreamId,
            long head,
            String hash,
            EvmRpcProbeStatus networkStatus) {
        return new EvmRpcCapabilityReport(
                upstreamId,
                "eip155:1",
                Instant.EPOCH,
                Instant.EPOCH,
                List.of(
                        observation(
                                EvmRpcProbeOperation.CHAIN_ID,
                                EvmRpcProbePurpose.NETWORK,
                                networkStatus,
                                OptionalLong.empty(),
                                null),
                        observation(
                                EvmRpcProbeOperation.SAFE_BLOCK,
                                EvmRpcProbePurpose.FINALITY,
                                EvmRpcProbeStatus.SUPPORTED,
                                OptionalLong.of(head),
                                hash)));
    }

    private static EvmRpcProbeObservation observation(
            EvmRpcProbeOperation operation,
            EvmRpcProbePurpose purpose,
            EvmRpcProbeStatus status,
            OptionalLong block,
            String hash) {
        return new EvmRpcProbeObservation(
                EvmRpcTransport.HTTP,
                operation,
                purpose,
                status,
                block,
                block,
                hash,
                OptionalInt.empty(),
                OptionalInt.empty(),
                Instant.EPOCH,
                Duration.ZERO);
    }
}
