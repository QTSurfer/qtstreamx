package com.qtsurfer.qtstreamx.dex.discovery.uniswap.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.dex.discovery.uniswap.MarketDiscovery;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.AddressBasedUniswapPairOrientation;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapDiscoveryLimits;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapDiscoveryPolicy;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapFactoryScan;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapNetworkTokenPolicy;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapV2MarketDiscovery;
import com.qtsurfer.qtstreamx.dex.uniswap.v2.UniswapV2Pair;
import com.qtsurfer.qtstreamx.evm.rpc.EvmBlockTag;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogFilter;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcLog;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcReader;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import org.junit.jupiter.api.Test;

class PublicUniswapDiscoveryTest {

    @Test
    void constructsAndReadsSnapshotThroughPublicInterfaces() {
        MarketDiscovery<UniswapV2Pair> discovery = new UniswapV2MarketDiscovery(
                new UniswapFactoryScan(
                        "eip155:1",
                        "0x00000000000000000000000000000000000000f0",
                        100),
                new EmptyReader(),
                (network, token0, token1) -> Optional.empty());

        assertThat(discovery.refresh(99).toCompletableFuture().join()).isEmpty();
        assertThat(discovery.snapshot()).isEmpty();
        assertThat(discovery.nextBlock()).isEqualTo(100);
    }

    @Test
    void constructsBoundedAddressBasedPolicyThroughPublicInterfaces() {
        String quote = "0x0000000000000000000000000000000000000001";
        String base = "0x0000000000000000000000000000000000000002";
        AddressBasedUniswapPairOrientation orientation =
                new AddressBasedUniswapPairOrientation(Map.of(
                        "eip155:1",
                        new UniswapNetworkTokenPolicy(Set.of(quote), Set.of(base))));
        UniswapDiscoveryPolicy policy = new UniswapDiscoveryPolicy(
                orientation,
                new UniswapDiscoveryLimits(100, 20, 10, 5),
                OptionalLong.of(20));
        MarketDiscovery<UniswapV2Pair> discovery = new UniswapV2MarketDiscovery(
                new UniswapFactoryScan(
                        "eip155:1",
                        "0x00000000000000000000000000000000000000f0",
                        100),
                new EmptyReader(),
                policy);

        assertThat(discovery.refresh(99).toCompletableFuture().join()).isEmpty();
    }

    private static final class EmptyReader implements EvmRpcReader {
        @Override
        public long latestBlockNumber() {
            return 99;
        }

        @Override
        public List<EvmRpcLog> logs(EvmLogFilter filter, long fromBlock, long toBlock) {
            throw new AssertionError("No scan expected before the configured cursor");
        }

        @Override
        public byte[] call(String contractAddress, byte[] data, EvmBlockTag blockTag) {
            throw new AssertionError("No token call expected without events");
        }
    }
}
