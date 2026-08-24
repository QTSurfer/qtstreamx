package com.qtsurfer.qtstreamx.dex.discovery.uniswap;

import java.util.Objects;
import java.util.OptionalLong;

/**
 * Immutable selection and resource policy shared by V2 and V3 discovery.
 *
 * @param orientation address-backed base/quote orientation
 * @param limits hard discovery bounds
 * @param activityLookbackBlocks recent block window requiring a Swap, or empty to disable
 */
public record UniswapDiscoveryPolicy(
        UniswapPairOrientation orientation,
        UniswapDiscoveryLimits limits,
        OptionalLong activityLookbackBlocks
) {

    /** Validates the complete policy. */
    public UniswapDiscoveryPolicy {
        Objects.requireNonNull(orientation, "orientation");
        Objects.requireNonNull(limits, "limits");
        Objects.requireNonNull(activityLookbackBlocks, "activityLookbackBlocks");
        if (activityLookbackBlocks.isPresent() && activityLookbackBlocks.orElseThrow() <= 0) {
            throw new IllegalArgumentException("activityLookbackBlocks must be positive");
        }
        if (activityLookbackBlocks.isPresent()
                && activityLookbackBlocks.orElseThrow() > limits.maxScanBlocks()) {
            throw new IllegalArgumentException(
                    "activityLookbackBlocks must not exceed maxScanBlocks");
        }
    }

    static UniswapDiscoveryPolicy compatibility(UniswapPairOrientation orientation) {
        return new UniswapDiscoveryPolicy(
                orientation, UniswapDiscoveryLimits.safeDefaults(), OptionalLong.empty());
    }
}
