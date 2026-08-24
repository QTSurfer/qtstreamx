package com.qtsurfer.qtstreamx.dex.discovery.uniswap;

import java.util.Map;
import java.util.Objects;

/**
 * Reviewed inputs needed to start discovery from one Uniswap factory deployment.
 *
 * @param version protocol version emitted by the factory
 * @param factoryScan factory identity and deployment-block cursor
 * @param tokenPolicy canonical address policy for example market orientation
 */
public record UniswapDeployment(
        Version version,
        UniswapFactoryScan factoryScan,
        UniswapNetworkTokenPolicy tokenPolicy
) {

    /** Supported factory event families. */
    public enum Version {
        /** Uniswap v2 {@code PairCreated} factories. */
        V2,
        /** Uniswap v3 {@code PoolCreated} factories. */
        V3
    }

    /** Validates immutable deployment components and matching policy ownership. */
    public UniswapDeployment {
        Objects.requireNonNull(version, "version");
        Objects.requireNonNull(factoryScan, "factoryScan");
        Objects.requireNonNull(tokenPolicy, "tokenPolicy");
    }

    /**
     * Returns the CAIP-2 network owned by this deployment.
     *
     * @return stable network identity
     */
    public String network() {
        return factoryScan.network();
    }

    /**
     * Builds an address-based orientation restricted to this network and policy.
     *
     * @return fresh immutable orientation
     */
    public AddressBasedUniswapPairOrientation orientation() {
        return new AddressBasedUniswapPairOrientation(Map.of(network(), tokenPolicy));
    }
}
