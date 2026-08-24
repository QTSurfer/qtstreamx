package com.qtsurfer.qtstreamx.dex.discovery.uniswap;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.dex.core.EvmToken;
import java.util.Optional;

/** Selects an explicit logical base/quote orientation for an ordered token pair. */
@FunctionalInterface
public interface UniswapPairOrientation {

    /**
     * Performs an optional metadata-free address preselection.
     *
     * <p>The default preserves custom orientation implementations that require
     * resolved token metadata. Address-backed policies should override this to
     * avoid contract calls for untrusted pairs.
     *
     * @param network stable EVM network identity
     * @param token0Address lower-addressed factory token
     * @param token1Address higher-addressed factory token
     * @return whether metadata resolution may proceed
     */
    default boolean acceptsAddresses(
            String network,
            String token0Address,
            String token1Address) {
        return true;
    }

    /**
     * Selects the instrument for an address-identified token pair.
     *
     * @param network stable EVM network identity
     * @param token0 lower-addressed factory token
     * @param token1 higher-addressed factory token
     * @return explicit orientation, or empty when the pair must be excluded
     */
    Optional<Instrument> orient(String network, EvmToken token0, EvmToken token1);
}
