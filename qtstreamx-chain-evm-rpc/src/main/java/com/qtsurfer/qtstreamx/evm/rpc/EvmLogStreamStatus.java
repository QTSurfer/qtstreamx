package com.qtsurfer.qtstreamx.evm.rpc;

/** Observable lifecycle state of an EVM log stream. */
public enum EvmLogStreamStatus {
    /** The stream has not started. */
    IDLE,
    /** The stream is validating and replaying its durable cursor. */
    RECOVERING,
    /** Catch-up completed and the live WebSocket is connected. */
    LIVE,
    /** A bounded historical range was fully replayed; the stream will not go live. */
    COMPLETED,
    /** The missing safe interval exceeded the configured recovery ceiling. */
    GAP_EXHAUSTED,
    /** An unrecoverable checkpoint or acknowledgement transition failed. */
    FAILED,
    /** The stream was closed. */
    CLOSED
}
