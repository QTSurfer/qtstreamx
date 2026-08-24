package com.qtsurfer.qtstreamx.evm.rpc;

import java.util.Objects;

/** Identifies the explicit EVM block state used by a read-only contract call. */
public final class EvmBlockTag {
    private static final EvmBlockTag LATEST = new EvmBlockTag("latest");
    private static final EvmBlockTag SAFE = new EvmBlockTag("safe");
    private static final EvmBlockTag FINALIZED = new EvmBlockTag("finalized");

    private final String rpcValue;

    private EvmBlockTag(String rpcValue) {
        this.rpcValue = rpcValue;
    }

    /**
     * Selects one exact block number.
     *
     * @param blockNumber non-negative block number
     * @return numeric block tag
     * @throws IllegalArgumentException if the block number is negative
     */
    public static EvmBlockTag number(long blockNumber) {
        if (blockNumber < 0) {
            throw new IllegalArgumentException("blockNumber must be non-negative");
        }
        return new EvmBlockTag("0x" + Long.toHexString(blockNumber));
    }

    /**
     * Selects the provider's latest canonical head.
     *
     * @return latest block tag
     */
    public static EvmBlockTag latest() {
        return LATEST;
    }

    /**
     * Selects the provider's safe head.
     *
     * @return safe block tag
     */
    public static EvmBlockTag safe() {
        return SAFE;
    }

    /**
     * Selects the provider's finalized head.
     *
     * @return finalized block tag
     */
    public static EvmBlockTag finalized() {
        return FINALIZED;
    }

    String rpcValue() {
        return rpcValue;
    }

    @Override
    public boolean equals(Object other) {
        return this == other
                || other instanceof EvmBlockTag that && rpcValue.equals(that.rpcValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rpcValue);
    }

    /** Returns the safe block-tag value without endpoint or request data. */
    @Override
    public String toString() {
        return rpcValue;
    }
}
