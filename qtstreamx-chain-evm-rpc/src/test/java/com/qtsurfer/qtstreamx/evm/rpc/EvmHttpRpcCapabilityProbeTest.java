package com.qtsurfer.qtstreamx.evm.rpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class EvmHttpRpcCapabilityProbeTest {
    private static final String CONTRACT = "0x1111111111111111111111111111111111111111";
    private static final String TOPIC =
            "0x2222222222222222222222222222222222222222222222222222222222222222";
    private static final String BLOCK_HASH =
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    private static final Clock CLOCK =
            Clock.fixed(Instant.parse("2026-08-09T10:00:00Z"), ZoneOffset.UTC);

    @Test
    void recordsSuccessfulDirectOperationsWithoutConflatingHistory() {
        FakeProbeClient client = new FakeProbeClient();

        EvmRpcCapabilityReport report = probe(client).probe(
                plan(), EvmRpcProbeBudget.safeDefaults(), EvmRpcProbeScope.FULL);

        assertThat(report.observations()).hasSize(10);
        assertThat(report.observations())
                .allMatch(observation -> observation.status() == EvmRpcProbeStatus.SUPPORTED);
        assertThat(report.supports(EvmRpcProbePurpose.NETWORK)).isTrue();
        assertThat(report.supports(EvmRpcProbePurpose.FINALITY)).isTrue();
        assertThat(report.supports(EvmRpcProbePurpose.RECOVERY_LOGS)).isTrue();
        assertThat(report.supports(EvmRpcProbePurpose.DISCOVERY_LOGS)).isTrue();
        assertThat(report.supports(EvmRpcProbePurpose.LIVE_STATE)).isTrue();
        assertThat(report.supports(EvmRpcProbePurpose.HISTORICAL_STATE)).isTrue();
        assertThat(report.earliestProvenLogBlock()).hasValue(10);
        assertThat(report.earliestProvenStateBlock()).hasValue(10);
        assertThat(report.maximumProvenLogRange()).hasValue(10);
        assertThat(client.requests()).isEqualTo(10);
    }

    @Test
    void retainsOldLogEvidenceWhenHistoricalStateFails() {
        FakeProbeClient client = new FakeProbeClient() {
            @Override
            public byte[] call(String address, byte[] data, EvmBlockTag blockTag) {
                countRequest();
                if (blockTag.equals(EvmBlockTag.number(10))) {
                    throw new EvmRpcException(-32000);
                }
                return new byte[] {1};
            }

            @Override
            public byte[] code(String address, EvmBlockTag blockTag) {
                countRequest();
                if (blockTag.equals(EvmBlockTag.number(10))) {
                    throw new EvmRpcException(-32000);
                }
                return new byte[] {1};
            }
        };

        EvmRpcCapabilityReport report = probe(client).probe(
                plan(), EvmRpcProbeBudget.safeDefaults(), EvmRpcProbeScope.FULL);

        assertThat(report.earliestProvenLogBlock()).hasValue(10);
        assertThat(report.earliestProvenStateBlock()).isEmpty();
        assertThat(report.supports(EvmRpcProbePurpose.DISCOVERY_LOGS)).isTrue();
        assertThat(report.supports(EvmRpcProbePurpose.HISTORICAL_STATE)).isFalse();
    }

    @Test
    void stopsBeforeIssuingRequestsBeyondTheBudget() {
        FakeProbeClient client = new FakeProbeClient();
        EvmRpcProbeBudget budget = new EvmRpcProbeBudget(
                2,
                Duration.ofSeconds(45),
                10_000,
                10_000);

        EvmRpcCapabilityReport report = probe(client).probe(
                plan(), budget, EvmRpcProbeScope.FULL);

        assertThat(client.requests()).isEqualTo(2);
        assertThat(report.observations())
                .filteredOn(observation -> observation.status() == EvmRpcProbeStatus.BUDGET_EXHAUSTED)
                .hasSize(8);
    }

    @Test
    void appliesTheReturnedLogCeilingAcrossBothLogObservations() {
        AtomicInteger logRequests = new AtomicInteger();
        FakeProbeClient client = new FakeProbeClient() {
            @Override
            public List<EvmRpcLog> getLogs(
                    EvmLogFilter filter,
                    long fromBlock,
                    long toBlock,
                    int maxResults) {
                countRequest();
                logRequests.incrementAndGet();
                return List.of(new EvmRpcLog(
                        CONTRACT,
                        List.of(TOPIC),
                        "0x",
                        fromBlock,
                        BLOCK_HASH,
                        "0xtx",
                        0,
                        0,
                        false));
            }
        };

        EvmRpcCapabilityReport report = probe(client).probe(
                plan(),
                new EvmRpcProbeBudget(12, Duration.ofSeconds(45), 10_000, 1),
                EvmRpcProbeScope.FULL);

        assertThat(logRequests).hasValue(1);
        assertThat(report.observations())
                .filteredOn(observation -> observation.purpose() == EvmRpcProbePurpose.DISCOVERY_LOGS)
                .singleElement()
                .extracting(EvmRpcProbeObservation::status)
                .isEqualTo(EvmRpcProbeStatus.RESULT_LIMIT);
    }

    @Test
    void classifiesWrongNetworkAndRejectedRangeWithoutProviderText() {
        FakeProbeClient client = new FakeProbeClient() {
            @Override
            public long chainId() {
                countRequest();
                return 4663;
            }

            @Override
            public List<EvmRpcLog> getLogs(
                    EvmLogFilter filter,
                    long fromBlock,
                    long toBlock,
                    int maxResults) {
                countRequest();
                if (fromBlock == 10) {
                    throw new EvmRpcException(-32005);
                }
                return List.of();
            }

            @Override
            public byte[] code(String address, EvmBlockTag blockTag) {
                countRequest();
                throw new IllegalStateException(
                        "provider body from https://user:api-secret@example.test/rpc");
            }
        };

        EvmRpcCapabilityReport report = probe(client).probe(
                plan(), EvmRpcProbeBudget.safeDefaults(), EvmRpcProbeScope.FULL);

        assertThat(report.observations())
                .anySatisfy(observation -> {
                    assertThat(observation.operation()).isEqualTo(EvmRpcProbeOperation.CHAIN_ID);
                    assertThat(observation.status()).isEqualTo(EvmRpcProbeStatus.WRONG_NETWORK);
                })
                .anySatisfy(observation -> {
                    assertThat(observation.purpose()).isEqualTo(EvmRpcProbePurpose.DISCOVERY_LOGS);
                    assertThat(observation.status()).isEqualTo(EvmRpcProbeStatus.UNKNOWN);
                    assertThat(observation.rpcErrorCode()).hasValue(-32005);
                });
        assertThat(report.toString())
                .doesNotContain("api-secret")
                .doesNotContain("example.test")
                .doesNotContain("provider body");
    }

    @Test
    void rejectsPlansThatExceedTheBudgetBeforeContact() {
        FakeProbeClient client = new FakeProbeClient();
        EvmRpcProbePlan oversized = new EvmRpcProbePlan(
                plan().logFilter(),
                100,
                200,
                10,
                19,
                CONTRACT,
                new byte[] {1},
                10);
        EvmRpcProbeBudget budget = new EvmRpcProbeBudget(
                12,
                Duration.ofSeconds(45),
                10,
                10_000);

        assertThatIllegalArgumentException().isThrownBy(() -> probe(client).probe(
                oversized, budget, EvmRpcProbeScope.FULL));
        assertThat(client.requests()).isZero();
    }

    @Test
    void startupScopeNeverContactsHistoricalOperations() {
        FakeProbeClient client = new FakeProbeClient();

        EvmRpcCapabilityReport report = probe(client).probe(
                plan(), EvmRpcProbeBudget.safeDefaults(), EvmRpcProbeScope.STARTUP);

        assertThat(client.requests()).isEqualTo(6);
        assertThat(report.observations()).hasSize(6);
        assertThat(report.observations())
                .noneMatch(observation -> observation.purpose() == EvmRpcProbePurpose.DISCOVERY_LOGS)
                .noneMatch(observation -> observation.purpose() == EvmRpcProbePurpose.HISTORICAL_STATE);
        assertThat(report.supports(EvmRpcProbePurpose.LIVE_STATE)).isTrue();
    }

    @Test
    void routeScopeFitsEightRequestsWithoutProbingUnusedOperations() {
        FakeProbeClient client = new FakeProbeClient();
        EvmRpcProbeBudget routeBudget = new EvmRpcProbeBudget(
                8, Duration.ofSeconds(30), 10_000, 10_000);

        EvmRpcCapabilityReport report = probe(client).probe(
                plan(), routeBudget, EvmRpcProbeScope.ROUTE);

        assertThat(client.requests()).isEqualTo(8);
        assertThat(report.observations()).hasSize(8);
        assertThat(report.observations())
                .noneMatch(observation -> observation.operation()
                        == EvmRpcProbeOperation.FINALIZED_BLOCK)
                .filteredOn(observation -> observation.purpose()
                        == EvmRpcProbePurpose.LIVE_STATE)
                .singleElement()
                .extracting(EvmRpcProbeObservation::operation)
                .isEqualTo(EvmRpcProbeOperation.CALL);
        assertThat(report.supports(EvmRpcProbePurpose.RECOVERY_LOGS)).isTrue();
        assertThat(report.supports(EvmRpcProbePurpose.DISCOVERY_LOGS)).isTrue();
        assertThat(report.supports(EvmRpcProbePurpose.HISTORICAL_STATE)).isTrue();
    }

    @Test
    void disablesTransportRetriesSoTheWireRequestCeilingIsReal() {
        AtomicInteger wireRequests = new AtomicInteger();
        JsonRpcHttpTransport transport = (request, timeout) -> {
            wireRequests.incrementAndGet();
            throw new IOException("provider detail api-secret");
        };
        EvmRpcReaderConfig config = config();
        EvmHttpRpcCapabilityProbe probe =
                new EvmHttpRpcCapabilityProbe(config, "ethereum-primary", transport, CLOCK);

        EvmRpcCapabilityReport report = probe.probe(
                plan(),
                new EvmRpcProbeBudget(1, Duration.ofSeconds(45), 10_000, 10_000),
                EvmRpcProbeScope.FULL);

        assertThat(wireRequests).hasValue(1);
        assertThat(report.observations().getFirst().status())
                .isEqualTo(EvmRpcProbeStatus.TRANSPORT_FAILURE);
        assertThat(report.toString()).doesNotContain("api-secret", "provider detail");
    }

    @Test
    void rejectsASequenceWhoseRequestDeadlinesExceedTheWallClockBudget() {
        FakeProbeClient client = new FakeProbeClient();

        assertThatIllegalArgumentException().isThrownBy(() -> probe(client).probe(
                plan(),
                new EvmRpcProbeBudget(2, Duration.ofSeconds(5), 10_000, 10_000),
                EvmRpcProbeScope.FULL));
        assertThat(client.requests()).isZero();
    }

    private static EvmHttpRpcCapabilityProbe probe(EvmRpcProbeHttpClient client) {
        return new EvmHttpRpcCapabilityProbe(config(), "ethereum-primary", client, CLOCK);
    }

    private static EvmRpcReaderConfig config() {
        return new EvmRpcReaderConfig(
                "eip155:1",
                "https://user:api-secret@example.test/rpc",
                2_000,
                Duration.ofSeconds(5),
                1);
    }

    private static EvmRpcProbePlan plan() {
        return new EvmRpcProbePlan(
                new EvmLogFilter(java.util.Set.of(CONTRACT), java.util.Set.of(TOPIC)),
                100,
                109,
                10,
                19,
                CONTRACT,
                new byte[] {1, 2, 3, 4},
                10);
    }

    private static class FakeProbeClient implements EvmRpcProbeHttpClient {
        private final AtomicInteger requests = new AtomicInteger();

        @Override
        public long chainId() {
            countRequest();
            return 1;
        }

        @Override
        public long latestBlockNumber() {
            countRequest();
            return 120;
        }

        @Override
        public EvmBlock getBlock(EvmBlockTag blockTag) {
            countRequest();
            long number = blockTag.equals(EvmBlockTag.safe()) ? 118 : 110;
            return new EvmBlock(number, BLOCK_HASH, 1_000_000);
        }

        @Override
        public List<EvmRpcLog> getLogs(
                EvmLogFilter filter,
                long fromBlock,
                long toBlock,
                int maxResults) {
            countRequest();
            return List.of();
        }

        @Override
        public byte[] call(String address, byte[] data, EvmBlockTag blockTag) {
            countRequest();
            return new byte[] {1};
        }

        @Override
        public byte[] code(String address, EvmBlockTag blockTag) {
            countRequest();
            return new byte[] {1};
        }

        protected final void countRequest() {
            requests.incrementAndGet();
        }

        private int requests() {
            return requests.get();
        }
    }
}
