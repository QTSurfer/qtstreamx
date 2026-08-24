package com.qtsurfer.qtstreamx.core.client;

/** A normalized market-trade stream with explicit durable batch acknowledgement. */
public interface RecoverableMarketTradeStream extends MarketTradeStream {

    /**
     * Starts the stream with a source cursor that advances only after acknowledgement.
     *
     * @param handler idempotent normalized-batch handler
     * @throws Exception when source recovery or startup fails
     * @throws IllegalStateException when the stream was already started
     */
    void startRecoverable(MarketTradeBatchHandler handler) throws Exception;
}
