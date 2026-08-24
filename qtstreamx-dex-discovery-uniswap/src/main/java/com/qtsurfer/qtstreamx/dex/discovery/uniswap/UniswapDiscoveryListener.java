package com.qtsurfer.qtstreamx.dex.discovery.uniswap;

/** Receives safe diagnostics for factory events excluded from discovery. */
@FunctionalInterface
public interface UniswapDiscoveryListener {

    /**
     * Handles one excluded event.
     *
     * @param failure safe failure diagnostic
     */
    void onFailure(UniswapDiscoveryFailure failure);

    /**
     * Returns a listener that intentionally ignores diagnostics.
     *
     * @return no-op listener
     */
    static UniswapDiscoveryListener ignoring() {
        return ignored -> {};
    }
}
