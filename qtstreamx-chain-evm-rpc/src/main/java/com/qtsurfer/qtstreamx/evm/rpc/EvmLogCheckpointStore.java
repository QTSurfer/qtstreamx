package com.qtsurfer.qtstreamx.evm.rpc;

import java.util.Optional;

/** Provider-neutral persistence seam for acknowledged EVM log checkpoints. */
public interface EvmLogCheckpointStore {

    /**
     * Loads the latest acknowledged checkpoint for a stream.
     *
     * @param streamId stable stream identity
     * @return stored checkpoint, or empty when the stream has never advanced
     * @throws Exception when the backing store cannot be read
     */
    Optional<EvmLogCheckpoint> load(EvmLogStreamId streamId) throws Exception;

    /**
     * Persists the latest acknowledged checkpoint.
     *
     * @param checkpoint checkpoint to save
     * @throws Exception when the backing store cannot be updated
     */
    void save(EvmLogCheckpoint checkpoint) throws Exception;
}
