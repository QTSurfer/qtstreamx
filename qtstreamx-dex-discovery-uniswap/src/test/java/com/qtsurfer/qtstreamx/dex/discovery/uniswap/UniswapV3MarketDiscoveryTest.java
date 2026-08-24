package com.qtsurfer.qtstreamx.dex.discovery.uniswap;

import static com.qtsurfer.qtstreamx.dex.discovery.uniswap.DiscoveryTestData.POOL_CREATED_TOPIC;
import static com.qtsurfer.qtstreamx.dex.discovery.uniswap.DiscoveryTestData.V3_SWAP_TOPIC;
import static com.qtsurfer.qtstreamx.dex.discovery.uniswap.DiscoveryTestData.exactOrientation;
import static com.qtsurfer.qtstreamx.dex.discovery.uniswap.DiscoveryTestData.swapEvent;
import static com.qtsurfer.qtstreamx.dex.discovery.uniswap.DiscoveryTestData.v3Event;
import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.RecordingEvmRpcReader.TokenMetadata;
import com.qtsurfer.qtstreamx.dex.uniswap.v3.UniswapV3Pool;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogFilter;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import org.junit.jupiter.api.Test;

class UniswapV3MarketDiscoveryTest {

    private static final String NETWORK = "eip155:1";
    private static final String FACTORY = "0x00000000000000000000000000000000000000f3";
    private static final String TOKEN0 = "0x0000000000000000000000000000000000000001";
    private static final String TOKEN1 = "0x0000000000000000000000000000000000000002";
    private static final String POOL = "0x00000000000000000000000000000000000000bb";

    @Test
    void discoversPoolWithFeeAndAsymmetricTokenDecimals() {
        RecordingEvmRpcReader reader = new RecordingEvmRpcReader(List.of(
                v3Event(FACTORY, TOKEN0, TOKEN1, 500, POOL, 200, "0xtx", 0)), Map.of(
                        TOKEN0, new TokenMetadata("WETH", 18),
                        TOKEN1, new TokenMetadata("USDC", 6)));
        MarketDiscovery<UniswapV3Pool> discovery = new UniswapV3MarketDiscovery(
                new UniswapFactoryScan(NETWORK, FACTORY, 200),
                reader,
                exactOrientation(
                        NETWORK, TOKEN0, TOKEN1, new Instrument("WETH", "USDC")));

        Set<UniswapV3Pool> pools = discovery.refresh(220).toCompletableFuture().join();

        assertThat(pools).singleElement().satisfies(pool -> {
            assertThat(pool.network()).isEqualTo(NETWORK);
            assertThat(pool.address()).isEqualTo(POOL);
            assertThat(pool.feeTier()).isEqualTo(500);
            assertThat(pool.token0().decimals()).isEqualTo(18);
            assertThat(pool.token1().decimals()).isEqualTo(6);
            assertThat(pool.instrument()).isEqualTo(new Instrument("WETH", "USDC"));
        });
        assertThat(reader.filters).containsExactly(new EvmLogFilter(
                Set.of(FACTORY), Set.of(POOL_CREATED_TOPIC)));
        assertThat(reader.ranges).containsExactly("200-220");
        assertThat(discovery.nextBlock()).isEqualTo(221);
    }

    @Test
    void retainsDistinctSameInstrumentPoolsAcrossSnapshotsAndCachesTokens() {
        String secondPool = "0x00000000000000000000000000000000000000bc";
        String thirdPool = "0x00000000000000000000000000000000000000bd";
        RecordingEvmRpcReader reader = new RecordingEvmRpcReader(
                (fromBlock, toBlock) -> fromBlock == 200
                        ? List.of(
                                v3Event(
                                        FACTORY, TOKEN0, TOKEN1, 500, POOL, 200, "0xtx1", 0),
                                v3Event(
                                        FACTORY, TOKEN0, TOKEN1, 3_000, secondPool, 200, "0xtx2", 1))
                        : List.of(v3Event(
                                FACTORY, TOKEN0, TOKEN1, 10_000, thirdPool, 210, "0xtx3", 0)),
                Map.of(
                        TOKEN0, new TokenMetadata("WETH", 18),
                        TOKEN1, new TokenMetadata("USDC", 6)));
        MarketDiscovery<UniswapV3Pool> discovery = new UniswapV3MarketDiscovery(
                new UniswapFactoryScan(NETWORK, FACTORY, 200),
                reader,
                exactOrientation(
                        NETWORK, TOKEN0, TOKEN1, new Instrument("WETH", "USDC")));

        discovery.refresh(200).toCompletableFuture().join();
        Set<UniswapV3Pool> pools = discovery.refresh(220).toCompletableFuture().join();

        assertThat(pools).extracting(UniswapV3Pool::address)
                .containsExactly(POOL, secondPool, thirdPool);
        assertThat(pools).extracting(UniswapV3Pool::feeTier)
                .containsExactly(500, 3_000, 10_000);
        assertThat(pools).extracting(UniswapV3Pool::instrument)
                .containsOnly(new Instrument("WETH", "USDC"));
        assertThat(reader.ranges).containsExactly("200-200", "201-220");
        assertThat(reader.calls).hasSize(4);
    }

    @Test
    void usesTheCanonicalV3SwapTopicForOptionalActivitySelection() {
        RecordingEvmRpcReader reader = new RecordingEvmRpcReader(
                (filter, fromBlock, toBlock) -> filter.addresses().contains(FACTORY)
                        ? List.of(v3Event(
                                FACTORY, TOKEN0, TOKEN1, 500, POOL, 200, "0xtx", 0))
                        : List.of(swapEvent(POOL, V3_SWAP_TOPIC, 220)),
                Map.of(
                        TOKEN0, new TokenMetadata("WETH", 18),
                        TOKEN1, new TokenMetadata("USDC", 6)));
        UniswapDiscoveryPolicy policy = new UniswapDiscoveryPolicy(
                exactOrientation(
                        NETWORK, TOKEN0, TOKEN1, new Instrument("WETH", "USDC")),
                new UniswapDiscoveryLimits(100, 4, 1, 1),
                OptionalLong.of(20));
        MarketDiscovery<UniswapV3Pool> discovery = new UniswapV3MarketDiscovery(
                new UniswapFactoryScan(NETWORK, FACTORY, 200), reader, policy);

        assertThat(discovery.refresh(220).toCompletableFuture().join())
                .extracting(UniswapV3Pool::address)
                .containsExactly(POOL);
        assertThat(reader.filters).containsExactly(
                new EvmLogFilter(Set.of(FACTORY), Set.of(POOL_CREATED_TOPIC)),
                new EvmLogFilter(Set.of(POOL), Set.of(V3_SWAP_TOPIC)));
    }
}
