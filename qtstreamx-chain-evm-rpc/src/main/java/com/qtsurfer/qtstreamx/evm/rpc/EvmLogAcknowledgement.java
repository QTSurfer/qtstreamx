package com.qtsurfer.qtstreamx.evm.rpc;

/** Explicit downstream decision for a confirmed EVM log batch. */
public enum EvmLogAcknowledgement {
    /** The downstream effect completed and the batch cursor may be persisted. */
    ACKNOWLEDGED,

    /** The downstream effect did not complete and the cursor must not advance. */
    REJECTED
}
