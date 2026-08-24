package com.qtsurfer.qtstreamx.canary;

import java.util.Map;

/**
 * Safe one-shot discovery counts attached to DEX canary artifacts.
 *
 * @param discovered selected plus rejected factory events
 * @param selected descriptors passed to the market stream
 * @param rejected excluded factory events
 * @param reasons rejection counts keyed by stable failure kind
 */
record DiscoveryCaptureReport(
        int discovered,
        int selected,
        int rejected,
        Map<String, Integer> reasons
) {

    DiscoveryCaptureReport {
        reasons = Map.copyOf(reasons);
        if (discovered < 0 || selected < 0 || rejected < 0) {
            throw new IllegalArgumentException("discovery counts must be non-negative");
        }
        if (discovered != selected + rejected) {
            throw new IllegalArgumentException("discovered must equal selected plus rejected");
        }
    }
}
