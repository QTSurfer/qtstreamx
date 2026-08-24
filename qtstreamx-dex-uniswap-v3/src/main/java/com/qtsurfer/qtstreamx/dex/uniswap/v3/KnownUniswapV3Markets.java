package com.qtsurfer.qtstreamx.dex.uniswap.v3;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.dex.core.EvmToken;
import java.util.List;

/**
 * Small reviewed set of explicit Uniswap v3 markets for examples and canaries.
 *
 * <p>These constants are convenience descriptors, not a complete catalogue,
 * activity ranking, or automatic subscription policy.
 */
public final class KnownUniswapV3Markets {

    /** Ethereum mainnet WETH/USDC fee-500 pool created at block 12,376,729. */
    public static final UniswapV3Pool ETHEREUM_MAINNET_WETH_USDC_500 =
            new UniswapV3Pool(
                    "eip155:1",
                    "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
                    new EvmToken(
                            "USDC", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", 6),
                    new EvmToken(
                            "WETH", "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", 18),
                    new Instrument("WETH", "USDC"),
                    500);

    /** Robinhood mainnet WETH/USDG fee-100 pool created at block 1,506,281. */
    public static final UniswapV3Pool ROBINHOOD_MAINNET_WETH_USDG_100 =
            new UniswapV3Pool(
                    "eip155:4663",
                    "0x52e65b17fb6e5ba00ed806f37afcd2daa50271ca",
                    new EvmToken(
                            "WETH", "0x0bd7d308f8e1639fab988df18a8011f41eacad73", 18),
                    new EvmToken(
                            "USDG", "0x5fc5360d0400a0fd4f2af552add042d716f1d168", 6),
                    new Instrument("WETH", "USDG"),
                    100);

    private static final List<UniswapV3Pool> ALL = List.of(
            ETHEREUM_MAINNET_WETH_USDC_500,
            ROBINHOOD_MAINNET_WETH_USDG_100);

    private KnownUniswapV3Markets() {}

    /**
     * Returns every reviewed market in deterministic order.
     *
     * @return immutable known-market list
     */
    public static List<UniswapV3Pool> all() {
        return ALL;
    }
}
