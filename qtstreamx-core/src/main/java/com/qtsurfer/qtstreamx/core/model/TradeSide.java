package com.qtsurfer.qtstreamx.core.model;

/** Direction of a normalized trade from the base asset's perspective. */
public enum TradeSide {
    /** The taker acquired base and paid quote. */
    BUY,

    /** The taker sold base and acquired quote. */
    SELL
}
