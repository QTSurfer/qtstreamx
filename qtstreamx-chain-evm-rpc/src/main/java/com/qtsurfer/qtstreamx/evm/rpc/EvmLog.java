package com.qtsurfer.qtstreamx.evm.rpc;

import java.util.List;
import java.util.Objects;

/**
 * Confirmed EVM log with its canonical block timestamp.
 *
 * @param network stable network identifier independent from the RPC endpoint
 * @param address emitting contract address
 * @param topics ordered event topics
 * @param data non-indexed event data
 * @param blockNumber canonical block number
 * @param blockHash canonical block hash
 * @param transactionHash transaction hash
 * @param transactionIndex transaction position within the block
 * @param logIndex log position within the block
 * @param timestamp block timestamp in epoch microseconds
 */
public record EvmLog(
        String network,
        String address,
        List<String> topics,
        String data,
        long blockNumber,
        String blockHash,
        String transactionHash,
        int transactionIndex,
        int logIndex,
        long timestamp
) {
    /** Creates an immutable confirmed log. */
    public EvmLog {
        Objects.requireNonNull(network, "network");
        Objects.requireNonNull(address, "address");
        topics = List.copyOf(topics);
        Objects.requireNonNull(data, "data");
        Objects.requireNonNull(blockHash, "blockHash");
        Objects.requireNonNull(transactionHash, "transactionHash");
    }
}
