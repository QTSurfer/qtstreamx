package com.qtsurfer.qtstreamx.dex.discovery.uniswap;

import static com.qtsurfer.qtstreamx.dex.discovery.uniswap.DiscoveryTestData.PAIR_CREATED_TOPIC;
import static com.qtsurfer.qtstreamx.dex.discovery.uniswap.DiscoveryTestData.V2_SWAP_TOPIC;
import static com.qtsurfer.qtstreamx.dex.discovery.uniswap.DiscoveryTestData.exactOrientation;
import static com.qtsurfer.qtstreamx.dex.discovery.uniswap.DiscoveryTestData.swapEvent;
import static com.qtsurfer.qtstreamx.dex.discovery.uniswap.DiscoveryTestData.v2Event;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.RecordingEvmRpcReader.TokenMetadata;
import com.qtsurfer.qtstreamx.dex.uniswap.v2.UniswapV2Pair;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogFilter;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcLog;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class UniswapV2MarketDiscoveryTest {

    private static final String NETWORK = "eip155:1";
    private static final String FACTORY = "0x00000000000000000000000000000000000000f0";
    private static final String TOKEN0 = "0x0000000000000000000000000000000000000001";
    private static final String TOKEN1 = "0x0000000000000000000000000000000000000002";
    private static final String PAIR = "0x00000000000000000000000000000000000000aa";

    @Test
    void discoversPairFromCanonicalFactoryEvent() {
        RecordingEvmRpcReader reader = new RecordingEvmRpcReader(List.of(
                v2Event(FACTORY, TOKEN0, TOKEN1, PAIR, 100, "0xtx", 0)), Map.of(
                        TOKEN0, new TokenMetadata("WETH", 18),
                        TOKEN1, new TokenMetadata("USDC", 6)));
        MarketDiscovery<UniswapV2Pair> discovery = new UniswapV2MarketDiscovery(
                new UniswapFactoryScan(NETWORK, FACTORY, 100),
                reader,
                exactOrientation(
                        NETWORK, TOKEN0, TOKEN1, new Instrument("WETH", "USDC")));

        Set<UniswapV2Pair> pairs =
                discovery.refresh(120).toCompletableFuture().join();

        assertThat(pairs).singleElement().satisfies(pair -> {
            assertThat(pair.network()).isEqualTo(NETWORK);
            assertThat(pair.address()).isEqualTo(PAIR);
            assertThat(pair.token0().symbol()).isEqualTo("WETH");
            assertThat(pair.token0().decimals()).isEqualTo(18);
            assertThat(pair.token1().symbol()).isEqualTo("USDC");
            assertThat(pair.token1().decimals()).isEqualTo(6);
            assertThat(pair.instrument()).isEqualTo(new Instrument("WETH", "USDC"));
        });
        assertThat(reader.filters).containsExactly(new EvmLogFilter(
                Set.of(FACTORY), Set.of(PAIR_CREATED_TOPIC)));
        assertThat(reader.ranges).containsExactly("100-120");
        assertThat(discovery.nextBlock()).isEqualTo(121);
    }

    @Test
    void resumesAtCursorAndCachesMetadataAcrossDuplicateEvents() {
        String secondPair = "0x00000000000000000000000000000000000000ab";
        RecordingEvmRpcReader reader = new RecordingEvmRpcReader(
                (fromBlock, toBlock) -> fromBlock == 100
                        ? List.of(
                                v2Event(FACTORY, TOKEN0, TOKEN1, PAIR, 100, "0xtx1", 0),
                                v2Event(FACTORY, TOKEN0, TOKEN1, PAIR, 100, "0xtx1", 0))
                        : List.of(v2Event(
                                FACTORY, TOKEN0, TOKEN1, secondPair, 105, "0xtx2", 0)),
                Map.of(
                        TOKEN0, new TokenMetadata("WETH", 18),
                        TOKEN1, new TokenMetadata("USDC", 6)));
        MarketDiscovery<UniswapV2Pair> discovery = discovery(reader, UniswapDiscoveryListener.ignoring());

        Set<UniswapV2Pair> first = discovery.refresh(100).toCompletableFuture().join();
        Set<UniswapV2Pair> second = discovery.refresh(110).toCompletableFuture().join();

        assertThat(first).extracting(UniswapV2Pair::address).containsExactly(PAIR);
        assertThat(second).extracting(UniswapV2Pair::address).containsExactly(PAIR, secondPair);
        assertThat(reader.ranges).containsExactly("100-100", "101-110");
        assertThat(reader.calls).hasSize(4);
        assertThatThrownBy(second::clear).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void isolatesMalformedEventAndMetadataFailureFromValidPair() {
        String unknownToken = "0x0000000000000000000000000000000000000003";
        String unknownPair = "0x00000000000000000000000000000000000000ac";
        EvmRpcLog truncated = new EvmRpcLog(
                        FACTORY,
                        List.of(PAIR_CREATED_TOPIC,
                                DiscoveryTestData.addressWord(TOKEN0),
                                DiscoveryTestData.addressWord(TOKEN1)),
                        DiscoveryTestData.addressWord(PAIR),
                        100,
                        "0xblock",
                        "0xmalformed",
                        0,
                        0,
                        false);
        RecordingEvmRpcReader reader = new RecordingEvmRpcReader(List.of(
                truncated,
                v2Event(FACTORY, TOKEN0, unknownToken, unknownPair, 101, "0xmetadata", 0),
                v2Event(FACTORY, TOKEN0, TOKEN1, PAIR, 102, "0xvalid", 0)), Map.of(
                        TOKEN0, new TokenMetadata("WETH", 18),
                        TOKEN1, new TokenMetadata("USDC", 6)));
        List<UniswapDiscoveryFailure> failures = new ArrayList<>();

        Set<UniswapV2Pair> pairs = discovery(reader, failures::add)
                .refresh(110).toCompletableFuture().join();

        assertThat(pairs).extracting(UniswapV2Pair::address).containsExactly(PAIR);
        assertThat(failures).extracting(UniswapDiscoveryFailure::kind).containsExactly(
                UniswapDiscoveryFailure.Kind.MALFORMED_EVENT,
                UniswapDiscoveryFailure.Kind.TOKEN_METADATA);
    }

    @Test
    void preservesSnapshotAndCursorWhenARefreshFails() {
        AtomicInteger scans = new AtomicInteger();
        RecordingEvmRpcReader reader = new RecordingEvmRpcReader(
                (fromBlock, toBlock) -> {
                    if (scans.getAndIncrement() == 0) {
                        return List.of(v2Event(
                                FACTORY, TOKEN0, TOKEN1, PAIR, 100, "0xtx", 0));
                    }
                    throw new IllegalStateException("provider detail");
                },
                Map.of(
                        TOKEN0, new TokenMetadata("WETH", 18),
                        TOKEN1, new TokenMetadata("USDC", 6)));
        MarketDiscovery<UniswapV2Pair> discovery = discovery(
                reader, UniswapDiscoveryListener.ignoring());
        Set<UniswapV2Pair> completed = discovery.refresh(100).toCompletableFuture().join();

        assertThatThrownBy(() -> discovery.refresh(110).toCompletableFuture().join())
                .hasCauseInstanceOf(UniswapDiscoveryException.class)
                .hasMessageNotContaining("provider detail");

        assertThat(discovery.snapshot()).isSameAs(completed);
        assertThat(discovery.nextBlock()).isEqualTo(101);
    }

    @Test
    void rejectsAnOversizedScanBeforeCallingTheProvider() {
        RecordingEvmRpcReader reader = new RecordingEvmRpcReader(List.of(), Map.of());
        UniswapDiscoveryPolicy policy = new UniswapDiscoveryPolicy(
                exactOrientation(
                        NETWORK, TOKEN0, TOKEN1, new Instrument("WETH", "USDC")),
                new UniswapDiscoveryLimits(2, 10, 10, 10),
                OptionalLong.empty());
        MarketDiscovery<UniswapV2Pair> discovery = new UniswapV2MarketDiscovery(
                new UniswapFactoryScan(NETWORK, FACTORY, 100), reader, policy);

        assertThatThrownBy(() -> discovery.refresh(102).toCompletableFuture().join())
                .hasCauseInstanceOf(UniswapDiscoveryException.class)
                .cause()
                .extracting("kind")
                .isEqualTo(UniswapDiscoveryException.Kind.SCAN_RANGE_LIMIT);
        assertThat(reader.ranges).isEmpty();
        assertThat(discovery.nextBlock()).isEqualTo(100);
    }

    @Test
    void rejectsMetadataWorkThatExceedsTheRefreshBudgetBeforeCallingTokens() {
        RecordingEvmRpcReader reader = new RecordingEvmRpcReader(List.of(
                v2Event(FACTORY, TOKEN0, TOKEN1, PAIR, 100, "0xtx", 0)), Map.of(
                        TOKEN0, new TokenMetadata("WETH", 18),
                        TOKEN1, new TokenMetadata("USDC", 6)));
        UniswapDiscoveryPolicy policy = new UniswapDiscoveryPolicy(
                exactOrientation(
                        NETWORK, TOKEN0, TOKEN1, new Instrument("WETH", "USDC")),
                new UniswapDiscoveryLimits(100, 3, 10, 10),
                OptionalLong.empty());
        MarketDiscovery<UniswapV2Pair> discovery = new UniswapV2MarketDiscovery(
                new UniswapFactoryScan(NETWORK, FACTORY, 100), reader, policy);

        assertThatThrownBy(() -> discovery.refresh(100).toCompletableFuture().join())
                .hasCauseInstanceOf(UniswapDiscoveryException.class)
                .cause()
                .extracting("kind")
                .isEqualTo(UniswapDiscoveryException.Kind.METADATA_CALL_LIMIT);
        assertThat(reader.calls).isEmpty();
        assertThat(discovery.snapshot()).isEmpty();
        assertThat(discovery.nextBlock()).isEqualTo(100);
    }

    @Test
    void retainsInactiveCandidatesAndPublishesThemAfterRecentSwapActivity() {
        String secondPair = "0x00000000000000000000000000000000000000ab";
        AtomicInteger factoryScans = new AtomicInteger();
        RecordingEvmRpcReader reader = new RecordingEvmRpcReader(
                (filter, fromBlock, toBlock) -> {
                    if (filter.addresses().contains(FACTORY)) {
                        return factoryScans.getAndIncrement() == 0
                                ? List.of(
                                        v2Event(FACTORY, TOKEN0, TOKEN1, PAIR, 100, "0xtx1", 0),
                                        v2Event(FACTORY, TOKEN0, TOKEN1, secondPair, 101, "0xtx2", 0))
                                : List.of();
                    }
                    String market = filter.addresses().iterator().next();
                    if (market.equals(PAIR) || toBlock >= 121) {
                        return List.of(swapEvent(market, V2_SWAP_TOPIC, toBlock));
                    }
                    return List.of();
                },
                Map.of(
                        TOKEN0, new TokenMetadata("WETH", 18),
                        TOKEN1, new TokenMetadata("USDC", 6)));
        List<UniswapDiscoveryFailure> failures = new ArrayList<>();
        UniswapDiscoveryPolicy policy = new UniswapDiscoveryPolicy(
                exactOrientation(
                        NETWORK, TOKEN0, TOKEN1, new Instrument("WETH", "USDC")),
                new UniswapDiscoveryLimits(100, 4, 2, 2),
                OptionalLong.of(20));
        MarketDiscovery<UniswapV2Pair> discovery = new UniswapV2MarketDiscovery(
                new UniswapFactoryScan(NETWORK, FACTORY, 100), reader, policy, failures::add);

        Set<UniswapV2Pair> first = discovery.refresh(120).toCompletableFuture().join();
        Set<UniswapV2Pair> second = discovery.refresh(121).toCompletableFuture().join();

        assertThat(first).extracting(UniswapV2Pair::address).containsExactly(PAIR);
        assertThat(second).extracting(UniswapV2Pair::address).containsExactly(PAIR, secondPair);
        assertThat(failures).extracting(UniswapDiscoveryFailure::kind)
                .containsExactly(UniswapDiscoveryFailure.Kind.INACTIVE_MARKET);
        assertThat(reader.ranges).containsExactly(
                "100-120", "101-120", "101-120", "121-121", "102-121", "102-121");
    }

    @Test
    void preservesStateWhenFactoryCandidatesExceedTheConfiguredLimit() {
        String secondPair = "0x00000000000000000000000000000000000000ab";
        RecordingEvmRpcReader reader = new RecordingEvmRpcReader(List.of(
                v2Event(FACTORY, TOKEN0, TOKEN1, PAIR, 100, "0xtx1", 0),
                v2Event(FACTORY, TOKEN0, TOKEN1, secondPair, 101, "0xtx2", 0)), Map.of(
                        TOKEN0, new TokenMetadata("WETH", 18),
                        TOKEN1, new TokenMetadata("USDC", 6)));
        UniswapDiscoveryPolicy policy = new UniswapDiscoveryPolicy(
                exactOrientation(
                        NETWORK, TOKEN0, TOKEN1, new Instrument("WETH", "USDC")),
                new UniswapDiscoveryLimits(100, 4, 1, 1),
                OptionalLong.empty());
        MarketDiscovery<UniswapV2Pair> discovery = new UniswapV2MarketDiscovery(
                new UniswapFactoryScan(NETWORK, FACTORY, 100), reader, policy);

        assertThatThrownBy(() -> discovery.refresh(101).toCompletableFuture().join())
                .hasCauseInstanceOf(UniswapDiscoveryException.class)
                .cause()
                .extracting("kind")
                .isEqualTo(UniswapDiscoveryException.Kind.DISCOVERED_MARKET_LIMIT);
        assertThat(discovery.snapshot()).isEmpty();
        assertThat(discovery.nextBlock()).isEqualTo(100);
    }

    @Test
    void preservesStateWhenSelectedMarketsExceedTheOutputLimit() {
        String secondPair = "0x00000000000000000000000000000000000000ab";
        RecordingEvmRpcReader reader = new RecordingEvmRpcReader(List.of(
                v2Event(FACTORY, TOKEN0, TOKEN1, PAIR, 100, "0xtx1", 0),
                v2Event(FACTORY, TOKEN0, TOKEN1, secondPair, 101, "0xtx2", 0)), Map.of(
                        TOKEN0, new TokenMetadata("WETH", 18),
                        TOKEN1, new TokenMetadata("USDC", 6)));
        UniswapDiscoveryPolicy policy = new UniswapDiscoveryPolicy(
                exactOrientation(
                        NETWORK, TOKEN0, TOKEN1, new Instrument("WETH", "USDC")),
                new UniswapDiscoveryLimits(100, 4, 2, 1),
                OptionalLong.empty());
        MarketDiscovery<UniswapV2Pair> discovery = new UniswapV2MarketDiscovery(
                new UniswapFactoryScan(NETWORK, FACTORY, 100), reader, policy);

        assertThatThrownBy(() -> discovery.refresh(101).toCompletableFuture().join())
                .hasCauseInstanceOf(UniswapDiscoveryException.class)
                .cause()
                .extracting("kind")
                .isEqualTo(UniswapDiscoveryException.Kind.OUTPUT_MARKET_LIMIT);
        assertThat(discovery.snapshot()).isEmpty();
        assertThat(discovery.nextBlock()).isEqualTo(100);
    }

    @Test
    void preservesThePreviousSnapshotWhenAnActivityRefreshFails() {
        AtomicInteger factoryScans = new AtomicInteger();
        AtomicInteger activityScans = new AtomicInteger();
        RecordingEvmRpcReader reader = new RecordingEvmRpcReader(
                (filter, fromBlock, toBlock) -> {
                    if (filter.addresses().contains(FACTORY)) {
                        return factoryScans.getAndIncrement() == 0
                                ? List.of(v2Event(
                                        FACTORY, TOKEN0, TOKEN1, PAIR, 100, "0xtx", 0))
                                : List.of();
                    }
                    if (activityScans.getAndIncrement() == 0) {
                        return List.of(swapEvent(PAIR, V2_SWAP_TOPIC, 100));
                    }
                    throw new IllegalStateException("provider body must stay private");
                },
                Map.of(
                        TOKEN0, new TokenMetadata("WETH", 18),
                        TOKEN1, new TokenMetadata("USDC", 6)));
        UniswapDiscoveryPolicy policy = new UniswapDiscoveryPolicy(
                exactOrientation(
                        NETWORK, TOKEN0, TOKEN1, new Instrument("WETH", "USDC")),
                new UniswapDiscoveryLimits(100, 4, 1, 1),
                OptionalLong.of(20));
        MarketDiscovery<UniswapV2Pair> discovery = new UniswapV2MarketDiscovery(
                new UniswapFactoryScan(NETWORK, FACTORY, 100), reader, policy);
        Set<UniswapV2Pair> completed = discovery.refresh(100).toCompletableFuture().join();

        assertThatThrownBy(() -> discovery.refresh(101).toCompletableFuture().join())
                .hasCauseInstanceOf(UniswapDiscoveryException.class)
                .hasMessageNotContaining("provider body must stay private");
        assertThat(discovery.snapshot()).isSameAs(completed);
        assertThat(discovery.nextBlock()).isEqualTo(101);
    }

    private static MarketDiscovery<UniswapV2Pair> discovery(
            RecordingEvmRpcReader reader,
            UniswapDiscoveryListener listener) {
        return new UniswapV2MarketDiscovery(
                new UniswapFactoryScan(NETWORK, FACTORY, 100),
                reader,
                exactOrientation(
                        NETWORK, TOKEN0, TOKEN1, new Instrument("WETH", "USDC")),
                listener);
    }

}
