package com.qtsurfer.qtstreamx.evm.rpc.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.core.ws.WebSocketClient;
import com.qtsurfer.qtstreamx.evm.rpc.EvmHttpRpcCapabilityProbe;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogFilter;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcCapabilityReport;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcProbeBudget;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcProbeObservation;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcProbeOperation;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcProbePlan;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcProbePurpose;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcProbeScope;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcProbeStatus;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcReaderConfig;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcTransport;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcWebSocketProbeConfig;
import com.qtsurfer.qtstreamx.evm.rpc.EvmWebSocketRpcCapabilityProbe;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class PublicEvmRpcCapabilityProbeTest {
    @Test
    void constructsThePublicProviderNeutralContractFromAnExternalPackage() {
        EvmRpcReaderConfig httpConfig = new EvmRpcReaderConfig(
                "eip155:1",
                "https://user:secret@example.test/rpc",
                2_000,
                Duration.ofSeconds(3),
                0);
        EvmRpcProbePlan plan = new EvmRpcProbePlan(
                new EvmLogFilter(
                        Set.of("0x1111111111111111111111111111111111111111"),
                        Set.of("0x2222222222222222222222222222222222222222222222222222222222222222")),
                100,
                109,
                10,
                19,
                "0x1111111111111111111111111111111111111111",
                new byte[] {1},
                10);
        EvmRpcProbeObservation observation = new EvmRpcProbeObservation(
                EvmRpcTransport.HTTP,
                EvmRpcProbeOperation.GET_LOGS,
                EvmRpcProbePurpose.DISCOVERY_LOGS,
                EvmRpcProbeStatus.SUPPORTED,
                OptionalLong.of(10),
                OptionalLong.of(19),
                null,
                OptionalInt.of(0),
                OptionalInt.empty(),
                Instant.EPOCH,
                Duration.ZERO);
        EvmRpcCapabilityReport report = new EvmRpcCapabilityReport(
                "ethereum-primary",
                "eip155:1",
                Instant.EPOCH,
                Instant.EPOCH,
                List.of(observation));

        EvmHttpRpcCapabilityProbe httpProbe =
                new EvmHttpRpcCapabilityProbe(httpConfig, "ethereum-primary");
        EvmWebSocketRpcCapabilityProbe webSocketProbe = new EvmWebSocketRpcCapabilityProbe(
                new EvmRpcWebSocketProbeConfig(
                        "eip155:1",
                        "wss://user:secret@example.test/rpc",
                        Duration.ofSeconds(1)),
                "ethereum-primary",
                NoOpWebSocket::new);

        assertThat(httpProbe).isNotNull();
        assertThat(webSocketProbe).isNotNull();
        assertThat(plan.callData()).containsExactly(1);
        assertThat(EvmRpcProbeBudget.safeDefaults().maxRequests()).isEqualTo(12);
        assertThat(EvmRpcProbeScope.values())
                .containsExactly(
                        EvmRpcProbeScope.STARTUP,
                        EvmRpcProbeScope.ROUTE,
                        EvmRpcProbeScope.FULL);
        assertThat(report.earliestProvenLogBlock()).hasValue(10);
        assertThat(report.toString()).doesNotContain("secret", "example.test");
    }

    private static final class NoOpWebSocket implements WebSocketClient {
        @Override
        public void connect(String url) {}

        @Override
        public void send(String message) {}

        @Override
        public void onMessage(Consumer<String> handler) {}

        @Override
        public void onClose(BiConsumer<Integer, String> handler) {}

        @Override
        public void onError(Consumer<Throwable> handler) {}

        @Override
        public boolean isOpen() {
            return false;
        }

        @Override
        public void close() {}
    }
}
