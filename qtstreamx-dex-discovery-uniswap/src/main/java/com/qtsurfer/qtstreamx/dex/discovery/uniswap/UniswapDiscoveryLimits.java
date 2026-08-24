package com.qtsurfer.qtstreamx.dex.discovery.uniswap;

/**
 * Hard bounds for one discovery instance.
 *
 * @param maxScanBlocks maximum inclusive block range per refresh
 * @param maxMetadataCalls maximum ERC-20 contract calls per refresh
 * @param maxDiscoveredMarkets maximum retained factory markets
 * @param maxOutputMarkets maximum descriptors in a published snapshot
 */
public record UniswapDiscoveryLimits(
        long maxScanBlocks,
        int maxMetadataCalls,
        int maxDiscoveredMarkets,
        int maxOutputMarkets
) {

    /** Validates that every bound is positive. */
    public UniswapDiscoveryLimits {
        if (maxScanBlocks <= 0) {
            throw new IllegalArgumentException("maxScanBlocks must be positive");
        }
        if (maxMetadataCalls <= 0) {
            throw new IllegalArgumentException("maxMetadataCalls must be positive");
        }
        if (maxDiscoveredMarkets <= 0) {
            throw new IllegalArgumentException("maxDiscoveredMarkets must be positive");
        }
        if (maxOutputMarkets <= 0 || maxOutputMarkets > maxDiscoveredMarkets) {
            throw new IllegalArgumentException(
                    "maxOutputMarkets must be positive and not exceed maxDiscoveredMarkets");
        }
    }

    /**
     * Returns conservative bounds used by orientation-only compatibility constructors.
     *
     * @return limits for 100,000 blocks, 4,000 metadata calls, and 2,000 markets
     */
    public static UniswapDiscoveryLimits safeDefaults() {
        return new UniswapDiscoveryLimits(100_000, 4_000, 2_000, 2_000);
    }
}
