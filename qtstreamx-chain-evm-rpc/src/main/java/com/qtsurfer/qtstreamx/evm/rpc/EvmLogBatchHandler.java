package com.qtsurfer.qtstreamx.evm.rpc;

/** Applies one idempotent downstream effect before a durable cursor may advance. */
@FunctionalInterface
public interface EvmLogBatchHandler {

    /**
     * Handles a confirmed batch, which may contain no matching logs.
     *
     * @param batch ordered confirmed batch and proposed cursor
     * @return explicit acknowledgement decision
     * @throws Exception when the downstream effect fails
     */
    EvmLogAcknowledgement handle(EvmLogBatch batch) throws Exception;
}
