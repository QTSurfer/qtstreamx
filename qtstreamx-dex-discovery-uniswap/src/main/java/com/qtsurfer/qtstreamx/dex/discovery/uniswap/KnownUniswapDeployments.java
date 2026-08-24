package com.qtsurfer.qtstreamx.dex.discovery.uniswap;

import java.util.List;
import java.util.Set;

/**
 * Small reviewed set of Uniswap factory deployments for examples and canaries.
 *
 * <p>Factory cursors are deployment blocks. Callers still own safe heads,
 * bounded pagination, checkpoints, activity filters, and subscription policy.
 */
public final class KnownUniswapDeployments {

    /** Ethereum mainnet Uniswap v2 factory deployed at block 10,000,835. */
    public static final UniswapDeployment ETHEREUM_MAINNET_V2 =
            new UniswapDeployment(
                    UniswapDeployment.Version.V2,
                    new UniswapFactoryScan(
                            "eip155:1",
                            "0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f",
                            10_000_835),
                    ethereumWethUsdcPolicy());

    /** Ethereum mainnet Uniswap v3 factory deployed at block 12,369,621. */
    public static final UniswapDeployment ETHEREUM_MAINNET_V3 =
            new UniswapDeployment(
                    UniswapDeployment.Version.V3,
                    new UniswapFactoryScan(
                            "eip155:1",
                            "0x1f98431c8ad98523631ae4a59f267346ea31f984",
                            12_369_621),
                    ethereumWethUsdcPolicy());

    /** Robinhood mainnet Uniswap v3 factory deployed at block 8,930. */
    public static final UniswapDeployment ROBINHOOD_MAINNET_V3 =
            new UniswapDeployment(
                    UniswapDeployment.Version.V3,
                    new UniswapFactoryScan(
                            "eip155:4663",
                            "0x1f7d7550b1b028f7571e69a784071f0205fd2efa",
                            8_930),
                    robinhoodWethUsdgPolicy());

    private static final List<UniswapDeployment> ALL = List.of(
            ETHEREUM_MAINNET_V2,
            ETHEREUM_MAINNET_V3,
            ROBINHOOD_MAINNET_V3);

    private KnownUniswapDeployments() {}

    /**
     * Returns every reviewed deployment in deterministic order.
     *
     * @return immutable deployment list
     */
    public static List<UniswapDeployment> all() {
        return ALL;
    }

    private static UniswapNetworkTokenPolicy ethereumWethUsdcPolicy() {
        return new UniswapNetworkTokenPolicy(
                Set.of("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
                Set.of("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"));
    }

    private static UniswapNetworkTokenPolicy robinhoodWethUsdgPolicy() {
        return new UniswapNetworkTokenPolicy(
                Set.of("0x5fc5360d0400a0fd4f2af552add042d716f1d168"),
                Set.of("0x0bd7d308f8e1639fab988df18a8011f41eacad73"));
    }
}
