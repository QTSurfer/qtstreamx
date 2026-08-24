package com.qtsurfer.qtstreamx.canary;

import com.qtsurfer.qtstreamx.evm.rpc.ActivePassiveEvmLogStream;
import com.qtsurfer.qtstreamx.evm.rpc.ActivePassiveEvmTerminalReason;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogStreamFactory;
import com.qtsurfer.qtstreamx.evm.rpc.EvmProviderBundle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.qtsurfer.qtstreamx.evm.rpc.EvmLog;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogAcknowledgement;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogBatch;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogBatchHandler;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogCheckpoint;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogCheckpointStore;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogFilter;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogStream;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogStreamId;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogStreamMetrics;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogStreamStatus;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRecoveryPolicy;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRecoveryGapException;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRecoveryTransitionException;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcCapabilityReport;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcProbeObservation;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcProbeOperation;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcProbePurpose;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcProbeStatus;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcTransport;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class ActivePassiveEvmLogStreamTest {

    private static final EvmLogStreamId STREAM_ID =
            new EvmLogStreamId("eip155:1", "uniswap-v2-usdc-weth");
    private static final EvmLogFilter FILTER =
            new EvmLogFilter(Set.of("0xpool"), Set.of("0xswap"));

    @Test
    void replacesFailedActiveBundleWithPassiveUsingSameDurableIdentity() throws Exception {
        InMemoryCheckpointStore checkpoints = new InMemoryCheckpointStore();
        List<EvmRecoveryPolicy> policies = new ArrayList<>();
        FailingStream active = new FailingStream(new EvmRecoveryTransitionException(
                EvmRecoveryTransitionException.Stage.UPSTREAM_RECOVERY,
                STREAM_ID,
                "active"));
        DeliveringStream passive = new DeliveringStream();
        EvmLogStreamFactory factory = (bundle, store, policy) -> {
            assertThat(store).isSameAs(checkpoints);
            policies.add(policy);
            return bundle.upstreamId().equals("active") ? active : passive;
        };
        ActivePassiveEvmLogStream supervisor = new ActivePassiveEvmLogStream(
                List.of(
                        bundle("active", liveCapabilities("active")),
                        bundle("passive", liveCapabilities("passive"))),
                STREAM_ID,
                checkpoints,
                2,
                100,
                factory);
        List<EvmLogBatch> delivered = new ArrayList<>();

        supervisor.startRecoverable(FILTER, batch -> {
            delivered.add(batch);
            return EvmLogAcknowledgement.ACKNOWLEDGED;
        });

        assertThat(active.closed).isTrue();
        assertThat(supervisor.status()).isEqualTo(EvmLogStreamStatus.LIVE);
        assertThat(supervisor.isConnected()).isTrue();
        assertThat(policies)
                .extracting(EvmRecoveryPolicy::streamId)
                .containsExactly(STREAM_ID, STREAM_ID);
        assertThat(policies)
                .extracting(EvmRecoveryPolicy::upstreamId)
                .containsExactly("active", "passive");
        assertThat(delivered).singleElement().satisfies(batch -> {
            assertThat(batch.streamId()).isEqualTo(STREAM_ID);
            assertThat(batch.fromBlock()).isEqualTo(101);
            assertThat(batch.toBlock()).isEqualTo(101);
        });
    }

    @Test
    void rejectsBundleWithoutLiveSubscriptionEvidence() {
        EvmRpcCapabilityReport incomplete = report(
                "passive",
                EvmRpcProbePurpose.NETWORK,
                EvmRpcProbePurpose.HEAD,
                EvmRpcProbePurpose.FINALITY,
                EvmRpcProbePurpose.LIVE_STATE,
                EvmRpcProbePurpose.RECOVERY_LOGS);

        assertThatThrownBy(() -> new ActivePassiveEvmLogStream(
                        List.of(
                                bundle("active", liveCapabilities("active")),
                                bundle("passive", incomplete)),
                        STREAM_ID,
                        new InMemoryCheckpointStore(),
                        2,
                        100,
                        (bundle, store, policy) -> new DeliveringStream()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("passive", "LIVE_SUBSCRIPTION")
                .hasMessageNotContaining("rpc.invalid");
    }

    @Test
    void rejectsEqualHeightDivergentProviderEvidence() {
        EvmRpcCapabilityReport divergent = liveCapabilities(
                "passive", "0x" + "2".repeat(64));

        assertThatThrownBy(() -> new ActivePassiveEvmLogStream(
                        List.of(
                                bundle("active", liveCapabilities("active")),
                                bundle("passive", divergent)),
                        STREAM_ID,
                        new InMemoryCheckpointStore(),
                        2,
                        100,
                        (bundle, store, policy) -> new DeliveringStream()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("DIVERGENT_HASH")
                .hasMessageNotContaining("rpc.invalid")
                .hasMessageNotContaining("0x2222");
    }

    @Test
    void acceptsSequentialProbeSkewWithinConfiguredSafeHeadLag() {
        ActivePassiveEvmLogStream supervisor = new ActivePassiveEvmLogStream(
                List.of(
                        bundle("active", liveCapabilities("active", 101, "0x" + "1".repeat(64))),
                        bundle("passive", liveCapabilities("passive", 100, "0x" + "2".repeat(64)))),
                STREAM_ID,
                new InMemoryCheckpointStore(),
                2,
                100,
                2,
                (bundle, store, policy) -> new DeliveringStream());

        assertThat(supervisor.status()).isEqualTo(EvmLogStreamStatus.IDLE);
    }

    @Test
    void rejectsProviderOutsideConfiguredSafeHeadLag() {
        assertThatThrownBy(() -> new ActivePassiveEvmLogStream(
                        List.of(
                                bundle("active", liveCapabilities("active", 103, "0x" + "1".repeat(64))),
                                bundle("passive", liveCapabilities("passive", 100, "0x" + "2".repeat(64)))),
                        STREAM_ID,
                        new InMemoryCheckpointStore(),
                        2,
                        100,
                        2,
                        (bundle, store, policy) -> new DeliveringStream()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("RIGHT_STALE");
    }

    @Test
    void rejectsSequentialProbeSkewWithoutCanonicalHashes() {
        assertThatThrownBy(() -> new ActivePassiveEvmLogStream(
                        List.of(
                                bundle("active", liveCapabilities("active", 101, null)),
                                bundle("passive", liveCapabilities("passive", 100, "0x" + "2".repeat(64)))),
                        STREAM_ID,
                        new InMemoryCheckpointStore(),
                        2,
                        100,
                        2,
                        (bundle, store, policy) -> new DeliveringStream()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("safe-head evidence");
    }

    @Test
    void rejectsHttpAndWebSocketSafeHeadDivergenceWithinOneBundle() {
        EvmRpcCapabilityReport divergentTransport = withWebSocketSafeHead(
                liveCapabilities("active"), 100, "0x" + "2".repeat(64));

        assertThatThrownBy(() -> new ActivePassiveEvmLogStream(
                        List.of(
                                bundle("active", divergentTransport),
                                bundle("passive", liveCapabilities("passive"))),
                        STREAM_ID,
                        new InMemoryCheckpointStore(),
                        2,
                        100,
                        2,
                        (bundle, store, policy) -> new DeliveringStream()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("active", "hashes diverge")
                .hasMessageNotContaining("rpc.invalid")
                .hasMessageNotContaining("0x2222");
    }

    @Test
    void switchesAfterAsynchronousActiveTransportExhaustion() throws Exception {
        InMemoryCheckpointStore checkpoints = new InMemoryCheckpointStore();
        DeliveringStream active = new DeliveringStream(101);
        DeliveringStream passive = new DeliveringStream(102);
        ActivePassiveEvmLogStream supervisor = new ActivePassiveEvmLogStream(
                List.of(
                        bundle("active", liveCapabilities("active")),
                        bundle("passive", liveCapabilities("passive"))),
                STREAM_ID,
                checkpoints,
                2,
                100,
                (bundle, store, policy) -> bundle.upstreamId().equals("active")
                        ? active
                        : passive);
        List<Long> deliveredThrough = new ArrayList<>();
        supervisor.startRecoverable(FILTER, batch -> {
            deliveredThrough.add(batch.toBlock());
            return EvmLogAcknowledgement.ACKNOWLEDGED;
        });

        active.fail(new EvmRecoveryTransitionException(
                EvmRecoveryTransitionException.Stage.UPSTREAM_RECOVERY,
                STREAM_ID,
                "active"));

        assertThat(active.closed).isTrue();
        assertThat(supervisor.status()).isEqualTo(EvmLogStreamStatus.LIVE);
        assertThat(deliveredThrough).containsExactly(101L, 102L);
        assertThat(supervisor.metrics().selectedUpstream()).isEqualTo("passive");
        assertThat(supervisor.metrics().switches()).isEqualTo(1);
        assertThat(supervisor.metrics().selections())
                .containsEntry("active", 1L)
                .containsEntry("passive", 1L);
        assertThat(supervisor.metrics().terminalFailures()).isEmpty();
    }

    @Test
    void aggregatesEndpointFreeRecoveryMetricsAcrossProviderReplacement() throws Exception {
        DeliveringStream active = new DeliveringStream(
                101,
                new EvmLogStreamMetrics(3, 2, 1, 0, 1, 1, 1));
        DeliveringStream passive = new DeliveringStream(
                102,
                new EvmLogStreamMetrics(0, 4, 0, 0, 0, 2, 0));
        ActivePassiveEvmLogStream supervisor = new ActivePassiveEvmLogStream(
                List.of(
                        bundle("active", liveCapabilities("active", 100, "0x" + "1".repeat(64))),
                        bundle("passive", liveCapabilities("passive", 101, "0x" + "2".repeat(64)))),
                STREAM_ID,
                new InMemoryCheckpointStore(),
                2,
                100,
                2,
                (bundle, store, policy) -> bundle.upstreamId().equals("active")
                        ? active
                        : passive);
        supervisor.startRecoverable(FILTER, batch -> EvmLogAcknowledgement.ACKNOWLEDGED);

        assertThat(supervisor.metrics().headLagBlocks()).isEqualTo(1);
        active.fail(new EvmRecoveryTransitionException(
                EvmRecoveryTransitionException.Stage.UPSTREAM_RECOVERY,
                STREAM_ID,
                "active"));

        assertThat(supervisor.metrics()).satisfies(snapshot -> {
            assertThat(snapshot.headLagBlocks()).isZero();
            assertThat(snapshot.cursorLagBlocks()).isZero();
            assertThat(snapshot.recoveryPages()).isEqualTo(6);
            assertThat(snapshot.retries()).isEqualTo(1);
            assertThat(snapshot.gaps()).isZero();
            assertThat(snapshot.reorgs()).isEqualTo(1);
            assertThat(snapshot.duplicateSuppressions()).isEqualTo(3);
            assertThat(snapshot.streamTerminalFailures()).isEqualTo(1);
        });
    }

    @Test
    void neverSwitchesForDownstreamOrCheckpointTransitionFailures() {
        for (EvmRecoveryTransitionException.Stage stage : List.of(
                EvmRecoveryTransitionException.Stage.CHECKPOINT_LOAD,
                EvmRecoveryTransitionException.Stage.OVERLAP_VALIDATION,
                EvmRecoveryTransitionException.Stage.BATCH_DELIVERY,
                EvmRecoveryTransitionException.Stage.CHECKPOINT_SAVE)) {
            AtomicInteger creations = new AtomicInteger();
            ActivePassiveEvmLogStream supervisor = new ActivePassiveEvmLogStream(
                    List.of(
                            bundle("active", liveCapabilities("active")),
                            bundle("passive", liveCapabilities("passive"))),
                    STREAM_ID,
                    new InMemoryCheckpointStore(),
                    2,
                    100,
                    (bundle, store, policy) -> {
                        creations.incrementAndGet();
                        return new FailingStream(new EvmRecoveryTransitionException(
                                stage, STREAM_ID, bundle.upstreamId()));
                    });

            assertThatThrownBy(() -> supervisor.startRecoverable(
                            FILTER, batch -> EvmLogAcknowledgement.ACKNOWLEDGED))
                    .isInstanceOf(EvmRecoveryTransitionException.class)
                    .extracting(error -> ((EvmRecoveryTransitionException) error).stage())
                    .isEqualTo(stage);
            assertThat(creations).hasValue(1);
        }
    }

    @Test
    void neverSwitchesPastAnExhaustedRecoveryGap() throws Exception {
        AtomicInteger creations = new AtomicInteger();
        DeliveringStream active = new DeliveringStream();
        ActivePassiveEvmLogStream supervisor = new ActivePassiveEvmLogStream(
                List.of(
                        bundle("active", liveCapabilities("active")),
                        bundle("passive", liveCapabilities("passive"))),
                STREAM_ID,
                new InMemoryCheckpointStore(),
                2,
                100,
                (bundle, store, policy) -> {
                    creations.incrementAndGet();
                    return active;
                });
        List<Throwable> errors = new ArrayList<>();
        supervisor.onError(errors::add);
        supervisor.startRecoverable(FILTER, batch -> EvmLogAcknowledgement.ACKNOWLEDGED);

        active.fail(new EvmRecoveryGapException(STREAM_ID, 101, 100, "active"));

        assertThat(creations).hasValue(1);
        assertThat(errors).singleElement().isInstanceOf(EvmRecoveryGapException.class);
        assertThat(supervisor.metrics().terminalFailures())
                .containsEntry(ActivePassiveEvmTerminalReason.GAP_EXHAUSTED, 1L);
        assertThat(supervisor.metrics().toString())
                .doesNotContain("rpc.invalid", "secret-active", "secret-passive");
    }

    private static EvmProviderBundle bundle(
            String upstreamId,
            EvmRpcCapabilityReport capabilities) {
        return new EvmProviderBundle(
                upstreamId,
                "https://rpc.invalid/secret-" + upstreamId,
                "wss://rpc.invalid/secret-" + upstreamId,
                capabilities);
    }

    static EvmRpcCapabilityReport liveCapabilities(String upstreamId) {
        return liveCapabilities(upstreamId, "0x" + "1".repeat(64));
    }

    private static EvmRpcCapabilityReport liveCapabilities(
            String upstreamId,
            String safeBlockHash) {
        return liveCapabilities(upstreamId, 100, safeBlockHash);
    }

    private static EvmRpcCapabilityReport liveCapabilities(
            String upstreamId,
            long safeBlock,
            String safeBlockHash) {
        EvmRpcCapabilityReport base = report(
                upstreamId,
                safeBlock,
                safeBlockHash,
                EvmRpcProbePurpose.NETWORK,
                EvmRpcProbePurpose.HEAD,
                EvmRpcProbePurpose.FINALITY,
                EvmRpcProbePurpose.LIVE_STATE,
                EvmRpcProbePurpose.RECOVERY_LOGS,
                EvmRpcProbePurpose.LIVE_SUBSCRIPTION);
        List<EvmRpcProbeObservation> observations = new ArrayList<>(base.observations());
        observations.add(new EvmRpcProbeObservation(
                EvmRpcTransport.WEBSOCKET,
                EvmRpcProbeOperation.CHAIN_ID,
                EvmRpcProbePurpose.NETWORK,
                EvmRpcProbeStatus.SUPPORTED,
                OptionalLong.empty(),
                OptionalLong.empty(),
                null,
                OptionalInt.empty(),
                OptionalInt.empty(),
                base.finishedAt(),
                Duration.ZERO));
        observations.add(new EvmRpcProbeObservation(
                EvmRpcTransport.WEBSOCKET,
                EvmRpcProbeOperation.SAFE_BLOCK,
                EvmRpcProbePurpose.FINALITY,
                EvmRpcProbeStatus.SUPPORTED,
                OptionalLong.of(safeBlock),
                OptionalLong.empty(),
                safeBlockHash,
                OptionalInt.empty(),
                OptionalInt.empty(),
                base.finishedAt(),
                Duration.ZERO));
        return new EvmRpcCapabilityReport(
                upstreamId,
                base.network(),
                base.startedAt(),
                base.finishedAt(),
                observations);
    }

    private static EvmRpcCapabilityReport withWebSocketSafeHead(
            EvmRpcCapabilityReport report,
            long safeBlock,
            String safeBlockHash) {
        List<EvmRpcProbeObservation> observations = report.observations().stream()
                .filter(observation -> !(observation.transport() == EvmRpcTransport.WEBSOCKET
                        && observation.operation() == EvmRpcProbeOperation.SAFE_BLOCK))
                .collect(Collectors.toCollection(ArrayList::new));
        observations.add(new EvmRpcProbeObservation(
                EvmRpcTransport.WEBSOCKET,
                EvmRpcProbeOperation.SAFE_BLOCK,
                EvmRpcProbePurpose.FINALITY,
                EvmRpcProbeStatus.SUPPORTED,
                OptionalLong.of(safeBlock),
                OptionalLong.empty(),
                safeBlockHash,
                OptionalInt.empty(),
                OptionalInt.empty(),
                report.finishedAt(),
                Duration.ZERO));
        return new EvmRpcCapabilityReport(
                report.upstreamId(),
                report.network(),
                report.startedAt(),
                report.finishedAt(),
                observations);
    }

    private static EvmRpcCapabilityReport report(
            String upstreamId,
            EvmRpcProbePurpose... purposes) {
        return report(upstreamId, "0x" + "1".repeat(64), purposes);
    }

    private static EvmRpcCapabilityReport report(
            String upstreamId,
            String safeBlockHash,
            EvmRpcProbePurpose... purposes) {
        return report(upstreamId, 100, safeBlockHash, purposes);
    }

    private static EvmRpcCapabilityReport report(
            String upstreamId,
            long safeBlock,
            String safeBlockHash,
            EvmRpcProbePurpose... purposes) {
        Instant measuredAt = Instant.parse("2026-08-09T00:00:00Z");
        return new EvmRpcCapabilityReport(
                upstreamId,
                STREAM_ID.network(),
                measuredAt,
                measuredAt,
                Arrays.stream(purposes)
                        .map(purpose -> observation(purpose, measuredAt, safeBlock, safeBlockHash))
                        .toList());
    }

    private static EvmRpcProbeObservation observation(
            EvmRpcProbePurpose purpose,
            Instant measuredAt,
            long safeBlock,
            String safeBlockHash) {
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
                        ? OptionalLong.of(safeBlock)
                        : OptionalLong.empty(),
                OptionalLong.empty(),
                purpose == EvmRpcProbePurpose.FINALITY ? safeBlockHash : null,
                OptionalInt.empty(),
                OptionalInt.empty(),
                measuredAt,
                Duration.ZERO);
    }

    private static final class InMemoryCheckpointStore implements EvmLogCheckpointStore {
        @Override
        public Optional<EvmLogCheckpoint> load(EvmLogStreamId streamId) {
            return Optional.empty();
        }

        @Override
        public void save(EvmLogCheckpoint checkpoint) {}
    }

    private abstract static class TestStream implements EvmLogStream {
        private Consumer<Throwable> errorHandler = ignored -> {};
        protected EvmLogStreamStatus status = EvmLogStreamStatus.IDLE;
        protected boolean closed;
        protected EvmLogStreamMetrics metrics = EvmLogStreamMetrics.empty();

        @Override
        public void start(EvmLogFilter filter, Consumer<EvmLog> handler) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void onError(Consumer<Throwable> handler) {
            errorHandler = handler;
        }

        @Override
        public boolean isConnected() {
            return status == EvmLogStreamStatus.LIVE;
        }

        @Override
        public EvmLogStreamStatus status() {
            return status;
        }

        @Override
        public EvmLogStreamMetrics recoveryMetrics() {
            return metrics;
        }

        @Override
        public void close() {
            closed = true;
            status = EvmLogStreamStatus.CLOSED;
        }

        protected void fail(Throwable failure) {
            status = EvmLogStreamStatus.FAILED;
            errorHandler.accept(failure);
        }
    }

    private static final class FailingStream extends TestStream {
        private final RuntimeException failure;

        private FailingStream(RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public void startRecoverable(EvmLogFilter filter, EvmLogBatchHandler handler) {
            throw failure;
        }
    }

    private static final class DeliveringStream extends TestStream {
        private final long blockNumber;

        private DeliveringStream() {
            this(101);
        }

        private DeliveringStream(long blockNumber) {
            this.blockNumber = blockNumber;
        }

        private DeliveringStream(long blockNumber, EvmLogStreamMetrics metrics) {
            this.blockNumber = blockNumber;
            this.metrics = metrics;
        }

        @Override
        public void startRecoverable(EvmLogFilter filter, EvmLogBatchHandler handler) throws Exception {
            EvmLogAcknowledgement acknowledgement = handler.handle(new EvmLogBatch(
                    STREAM_ID,
                    blockNumber,
                    blockNumber,
                    "0x" + "1".repeat(64),
                    List.of()));
            assertThat(acknowledgement).isEqualTo(EvmLogAcknowledgement.ACKNOWLEDGED);
            status = EvmLogStreamStatus.LIVE;
        }
    }
}
