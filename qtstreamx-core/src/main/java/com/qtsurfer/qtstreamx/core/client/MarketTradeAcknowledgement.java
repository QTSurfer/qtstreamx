package com.qtsurfer.qtstreamx.core.client;

/** Explicit downstream decision for a normalized market-trade batch. */
public enum MarketTradeAcknowledgement {
    /** The downstream effect completed and the source cursor may advance. */
    ACKNOWLEDGED,

    /** The downstream effect did not complete and the source cursor must not advance. */
    REJECTED
}
