package com.qtsurfer.qtstreamx.evm.rpc;


/** Creates one durable log stream for a selected runtime provider bundle. */
@FunctionalInterface
public interface EvmLogStreamFactory {
    EvmLogStream create(
            EvmProviderBundle bundle,
            EvmLogCheckpointStore checkpointStore,
            EvmRecoveryPolicy recoveryPolicy);
}
