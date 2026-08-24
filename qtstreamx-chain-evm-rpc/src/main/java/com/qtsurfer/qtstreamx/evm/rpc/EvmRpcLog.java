package com.qtsurfer.qtstreamx.evm.rpc;

import java.util.List;
import java.util.Objects;

/**
 * Raw EVM log returned by {@code eth_getLogs} before confirmation or timestamp enrichment.
 *
 * @param address emitting contract address
 * @param topics ordered event topics
 * @param data non-indexed event data
 * @param blockNumber block number reported by the provider
 * @param blockHash block hash reported by the provider
 * @param transactionHash transaction hash
 * @param transactionIndex transaction position within the block
 * @param logIndex log position within the block
 * @param removed whether the provider marks the log as removed
 */
public record EvmRpcLog(
        String address,
        List<String> topics,
        String data,
        long blockNumber,
        String blockHash,
        String transactionHash,
        int transactionIndex,
        int logIndex,
        boolean removed
) {
    /** Creates an immutable raw log. */
    public EvmRpcLog {
        Objects.requireNonNull(address, "address");
        topics = List.copyOf(topics);
        Objects.requireNonNull(data, "data");
        Objects.requireNonNull(blockHash, "blockHash");
        Objects.requireNonNull(transactionHash, "transactionHash");
        if (blockNumber < 0 || transactionIndex < 0 || logIndex < 0) {
            throw new IllegalArgumentException("log positions must be non-negative");
        }
    }
}
