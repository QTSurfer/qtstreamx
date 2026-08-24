package com.qtsurfer.qtstreamx.canary;

import com.qtsurfer.qtstreamx.evm.rpc.EvmProviderBundle;
import com.qtsurfer.qtstreamx.evm.rpc.EvmProviderBundleEligibility;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcCapabilityReport;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcProbeObservation;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcProbeOperation;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcProbePurpose;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcProbeStatus;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcTransport;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class EvmProviderBundleEligibilityTest {

    @Test
    void selectsHistoricalDiscoveryCapabilityIndependentlyFromLiveEligibility() {
        EvmProviderBundle active = bundle("active", liveCapabilities("active"));
        EvmProviderBundle passive = bundle(
                "passive",
                capabilities(
                        "passive",
                        EvmRpcProbePurpose.NETWORK,
                        EvmRpcProbePurpose.HEAD,
                        EvmRpcProbePurpose.FINALITY,
                        EvmRpcProbePurpose.LIVE_STATE,
                        EvmRpcProbePurpose.RECOVERY_LOGS,
                        EvmRpcProbePurpose.LIVE_SUBSCRIPTION,
                        EvmRpcProbePurpose.DISCOVERY_LOGS,
                        EvmRpcProbePurpose.HISTORICAL_STATE));

        EvmProviderBundle selected = EvmProviderBundleEligibility.selectDiscovery(
                List.of(active, passive), "eip155:1", 0);

        assertThat(selected.upstreamId()).isEqualTo("passive");
    }

    @Test
    void rejectsDiscoveryWhenOldLogsDoNotProveHistoricalState() {
        EvmProviderBundle active = bundle("active", capabilities(
                "active",
                EvmRpcProbePurpose.NETWORK,
                EvmRpcProbePurpose.HEAD,
                EvmRpcProbePurpose.FINALITY,
                EvmRpcProbePurpose.DISCOVERY_LOGS));
        EvmProviderBundle passive = bundle("passive", capabilities(
                "passive",
                EvmRpcProbePurpose.NETWORK,
                EvmRpcProbePurpose.HEAD,
                EvmRpcProbePurpose.FINALITY,
                EvmRpcProbePurpose.DISCOVERY_LOGS));

        assertThatThrownBy(() -> EvmProviderBundleEligibility.selectDiscovery(
                        List.of(active, passive), "eip155:1", 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("purpose-specific discovery")
                .hasMessageNotContaining("rpc.invalid");
    }

    private static EvmRpcCapabilityReport liveCapabilities(String upstreamId) {
        return capabilities(
                upstreamId,
                EvmRpcProbePurpose.NETWORK,
                EvmRpcProbePurpose.HEAD,
                EvmRpcProbePurpose.FINALITY,
                EvmRpcProbePurpose.LIVE_STATE,
                EvmRpcProbePurpose.RECOVERY_LOGS,
                EvmRpcProbePurpose.LIVE_SUBSCRIPTION);
    }

    private static EvmProviderBundle bundle(
            String upstreamId,
            EvmRpcCapabilityReport report) {
        return new EvmProviderBundle(
                upstreamId,
                "https://rpc.invalid/secret-" + upstreamId,
                "wss://rpc.invalid/secret-" + upstreamId,
                report);
    }

    private static EvmRpcCapabilityReport capabilities(
            String upstreamId,
            EvmRpcProbePurpose... purposes) {
        Instant measuredAt = Instant.parse("2026-08-09T00:00:00Z");
        return new EvmRpcCapabilityReport(
                upstreamId,
                "eip155:1",
                measuredAt,
                measuredAt,
                Arrays.stream(purposes)
                        .map(purpose -> observation(purpose, measuredAt))
                        .toList());
    }

    private static EvmRpcProbeObservation observation(
            EvmRpcProbePurpose purpose,
            Instant measuredAt) {
        EvmRpcProbeOperation operation = switch (purpose) {
            case NETWORK -> EvmRpcProbeOperation.CHAIN_ID;
            case HEAD -> EvmRpcProbeOperation.BLOCK_NUMBER;
            case FINALITY -> EvmRpcProbeOperation.SAFE_BLOCK;
            case LIVE_STATE, HISTORICAL_STATE -> EvmRpcProbeOperation.CALL;
            case RECOVERY_LOGS, DISCOVERY_LOGS -> EvmRpcProbeOperation.GET_LOGS;
            case LIVE_SUBSCRIPTION -> EvmRpcProbeOperation.LOG_SUBSCRIPTION;
        };
        return new EvmRpcProbeObservation(
                purpose == EvmRpcProbePurpose.LIVE_SUBSCRIPTION
                        ? EvmRpcTransport.WEBSOCKET
                        : EvmRpcTransport.HTTP,
                operation,
                purpose,
                EvmRpcProbeStatus.SUPPORTED,
                purpose == EvmRpcProbePurpose.FINALITY
                        ? OptionalLong.of(100)
                        : OptionalLong.empty(),
                OptionalLong.empty(),
                purpose == EvmRpcProbePurpose.FINALITY ? "0x" + "1".repeat(64) : null,
                OptionalInt.empty(),
                OptionalInt.empty(),
                measuredAt,
                Duration.ZERO);
    }
}
