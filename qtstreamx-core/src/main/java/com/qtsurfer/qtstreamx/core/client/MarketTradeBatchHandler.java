package com.qtsurfer.qtstreamx.core.client;

/** Applies one idempotent downstream effect before a source cursor may advance. */
@FunctionalInterface
public interface MarketTradeBatchHandler {

    /**
     * Handles one normalized trade batch, which may be empty.
     *
     * @param batch ordered normalized trades
     * @return explicit acknowledgement decision
     * @throws Exception when the downstream effect fails
     */
    MarketTradeAcknowledgement handle(MarketTradeBatch batch) throws Exception;
}
