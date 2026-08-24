package com.qtsurfer.qtstreamx.evm.rpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class EvmRpcCapabilityReportTest {
    @Test
    void keepsHistoricalLogsAndStateAsIndependentEvidence() {
        List<EvmRpcProbeObservation> observations = List.of(
                observation(
                        EvmRpcProbeOperation.GET_LOGS,
                        EvmRpcProbePurpose.DISCOVERY_LOGS,
                        EvmRpcProbeStatus.SUPPORTED,
                        100,
                        200),
                observation(
                        EvmRpcProbeOperation.CALL,
                        EvmRpcProbePurpose.HISTORICAL_STATE,
                        EvmRpcProbeStatus.SUPPORTED,
                        100,
                        100),
                observation(
                        EvmRpcProbeOperation.GET_CODE,
                        EvmRpcProbePurpose.HISTORICAL_STATE,
                        EvmRpcProbeStatus.UNSUPPORTED,
                        100,
                        100));

        EvmRpcCapabilityReport report = report(observations);

        assertThat(report.earliestProvenLogBlock()).hasValue(100);
        assertThat(report.earliestProvenStateBlock()).isEmpty();
        assertThat(report.maximumProvenLogRange()).hasValue(101);
        assertThat(report.supports(EvmRpcProbePurpose.DISCOVERY_LOGS)).isTrue();
        assertThat(report.supports(EvmRpcProbePurpose.HISTORICAL_STATE)).isFalse();
    }

    @Test
    void snapshotsObservationsAndRejectsEndpointShapedAliases() {
        List<EvmRpcProbeObservation> observations = new ArrayList<>();
        observations.add(observation(
                EvmRpcProbeOperation.BLOCK_NUMBER,
                EvmRpcProbePurpose.HEAD,
                EvmRpcProbeStatus.SUPPORTED,
                123,
                123));

        EvmRpcCapabilityReport report = report(observations);
        observations.clear();

        assertThat(report.observations()).hasSize(1);
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new EvmRpcCapabilityReport(
                        "https://key@example.test",
                        "eip155:1",
                        Instant.EPOCH,
                        Instant.EPOCH,
                        List.of()));
    }

    private static EvmRpcCapabilityReport report(List<EvmRpcProbeObservation> observations) {
        return new EvmRpcCapabilityReport(
                "ethereum-primary",
                "eip155:1",
                Instant.EPOCH,
                Instant.EPOCH.plusSeconds(1),
                observations);
    }

    private static EvmRpcProbeObservation observation(
            EvmRpcProbeOperation operation,
            EvmRpcProbePurpose purpose,
            EvmRpcProbeStatus status,
            long fromBlock,
            long toBlock) {
        return new EvmRpcProbeObservation(
                EvmRpcTransport.HTTP,
                operation,
                purpose,
                status,
                OptionalLong.of(fromBlock),
                OptionalLong.of(toBlock),
                null,
                OptionalInt.empty(),
                OptionalInt.empty(),
                Instant.EPOCH,
                Duration.ZERO);
    }
}
