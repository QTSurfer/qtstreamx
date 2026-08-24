package com.qtsurfer.qtstreamx.evm.rpc;

import java.util.Objects;

/** Finds the first canonical block at or after one Unix timestamp by bounded binary search. */
public final class EvmBlockTimestampResolver {
    private final EvmRpcReader reader;

    /** Creates a resolver over one bounded RPC reader. */
    public EvmBlockTimestampResolver(EvmRpcReader reader) {
        this.reader = Objects.requireNonNull(reader, "reader");
    }

    /** Returns the first block with a timestamp greater than or equal to {@code epochSecond}. */
    public long firstBlockAtOrAfter(long epochSecond) {
        if (epochSecond < 0) throw new IllegalArgumentException("epochSecond must be non-negative");
        long high = reader.latestBlockNumber();
        if (reader.block(high).timestamp() < epochSecond) return high;
        long low = 0;
        while (low < high) {
            long middle = low + (high - low) / 2;
            if (reader.block(middle).timestamp() < epochSecond) low = middle + 1;
            else high = middle;
        }
        return low;
    }
}
