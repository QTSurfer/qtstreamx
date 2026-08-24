package com.qtsurfer.qtstreamx.dex.uniswap.v2;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.dex.core.EvmToken;
import java.util.List;

/**
 * Small reviewed set of explicit Uniswap v2 markets for examples and canaries.
 *
 * <p>These constants are convenience descriptors, not a complete catalogue,
 * activity ranking, or automatic subscription policy.
 */
public final class KnownUniswapV2Markets {

    /** Ethereum mainnet WETH/USDC pair created at block 10,008,355. */
    public static final UniswapV2Pair ETHEREUM_MAINNET_WETH_USDC =
            new UniswapV2Pair(
                    "eip155:1",
                    "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc",
                    new EvmToken(
                            "USDC", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", 6),
                    new EvmToken(
                            "WETH", "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", 18),
                    new Instrument("WETH", "USDC"));

    private static final List<UniswapV2Pair> ALL = List.of(ETHEREUM_MAINNET_WETH_USDC);

    private KnownUniswapV2Markets() {}

    /**
     * Returns every reviewed market in deterministic order.
     *
     * @return immutable known-market list
     */
    public static List<UniswapV2Pair> all() {
        return ALL;
    }
}
