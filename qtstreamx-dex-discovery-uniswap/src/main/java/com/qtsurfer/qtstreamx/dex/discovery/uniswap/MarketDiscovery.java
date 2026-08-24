package com.qtsurfer.qtstreamx.dex.discovery.uniswap;

import java.util.Set;
import java.util.concurrent.CompletionStage;

/**
 * Discovers an immutable snapshot of typed markets through an explicit safe block.
 *
 * @param <T> concrete market descriptor type
 */
public interface MarketDiscovery<T> {

    /**
     * Refreshes the snapshot through the supplied inclusive safe head.
     *
     * @param safeHead last block eligible for discovery
     * @return stage yielding the complete immutable snapshot
     */
    CompletionStage<Set<T>> refresh(long safeHead);

    /**
     * Returns the most recently completed immutable snapshot.
     *
     * @return complete market snapshot
     */
    Set<T> snapshot();

    /**
     * Returns the first block not included in the current snapshot.
     *
     * @return next inclusive scan block
     */
    long nextBlock();
}
