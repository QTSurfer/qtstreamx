package com.qtsurfer.qtstreamx.evm.rpc;

import java.util.List;

/** Performs bounded, read-only JSON-RPC operations against one EVM network. */
public interface EvmRpcReader {

    /**
     * Reads the provider's current canonical head number.
     *
     * @return non-negative block number
     * @throws EvmRpcException when the provider returns a JSON-RPC protocol error
     * @throws IllegalStateException when transport retries are exhausted
     */
    long latestBlockNumber();

    /** Reads one canonical block by number, including its Unix timestamp in seconds. */
    default EvmBlock block(long blockNumber) {
        throw new UnsupportedOperationException("Block reads are not supported");
    }

    /**
     * Reads raw logs over one inclusive block interval.
     *
     * <p>The implementation pages and, when required, bisects provider queries. Returned logs are
     * ordered by block, transaction, and log index. Confirmation and cursor policy belong to the
     * caller.
     *
     * @param filter emitting contracts and accepted first topics
     * @param fromBlock first block, inclusive
     * @param toBlock last block, inclusive
     * @return immutable logs in chain order
     * @throws IllegalArgumentException if the interval is negative or reversed
     * @throws EvmRpcException when a single-block query returns a JSON-RPC protocol error
     * @throws IllegalStateException when transport retries are exhausted
     */
    List<EvmRpcLog> logs(EvmLogFilter filter, long fromBlock, long toBlock);

    /**
     * Executes a read-only contract call against an explicit block state.
     *
     * @param contractAddress 20-byte contract address
     * @param data opaque ABI call data
     * @param blockTag exact or named block state
     * @return a new byte array containing the opaque return data
     * @throws IllegalArgumentException if the contract address is malformed
     * @throws EvmRpcException when the call reverts or the provider returns another protocol error
     * @throws IllegalStateException when transport retries are exhausted or the result is malformed
     */
    byte[] call(String contractAddress, byte[] data, EvmBlockTag blockTag);

    /**
     * Reads contract bytecode at an explicit block state.
     *
     * <p>The default preserves compatibility with custom readers written before bytecode lookup was
     * added. Readers used for contract inspection must override this method.
     *
     * @param contractAddress 20-byte contract address
     * @param blockTag exact or named block state
     * @return a new byte array containing contract bytecode, or an empty array for no code
     * @throws UnsupportedOperationException if the reader does not support bytecode reads
     * @throws IllegalArgumentException if the contract address is malformed
     * @throws EvmRpcException when the provider returns a JSON-RPC protocol error
     * @throws IllegalStateException when transport retries are exhausted or the result is malformed
     */
    default byte[] code(String contractAddress, EvmBlockTag blockTag) {
        throw new UnsupportedOperationException("Contract bytecode reads are not supported");
    }
}
