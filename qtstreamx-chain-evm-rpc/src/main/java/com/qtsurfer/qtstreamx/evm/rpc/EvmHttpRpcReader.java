package com.qtsurfer.qtstreamx.evm.rpc;

import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;

/** HTTP adapter for bounded, read-only EVM JSON-RPC operations. */
public final class EvmHttpRpcReader implements EvmRpcReader {
    private static final Comparator<EvmRpcLog> CHAIN_ORDER = Comparator
            .comparingLong(EvmRpcLog::blockNumber)
            .thenComparingInt(EvmRpcLog::transactionIndex)
            .thenComparingInt(EvmRpcLog::logIndex);
    private static final Pattern EVM_ADDRESS = Pattern.compile("0x[0-9a-fA-F]{40}");

    private final EvmRpcReaderConfig config;
    private final JsonRpcHttpClient client;

    /**
     * Creates a reader backed by the JDK HTTP client.
     *
     * @param config network, endpoint, bounds, timeout, and retry policy
     */
    public EvmHttpRpcReader(EvmRpcReaderConfig config) {
        this(
                config,
                new JdkJsonRpcHttpTransport(URI.create(config.httpUrl())),
                Thread::sleep);
    }

    EvmHttpRpcReader(
            EvmRpcReaderConfig config,
            JsonRpcHttpTransport transport,
            RetryDelay retryDelay) {
        this.config = Objects.requireNonNull(config, "config");
        client = new JsonRpcHttpClient(config, transport, retryDelay);
    }

    @Override
    public long latestBlockNumber() {
        return client.latestBlockNumber();
    }

    @Override
    public EvmBlock block(long blockNumber) {
        if (blockNumber < 0) throw new IllegalArgumentException("blockNumber must be non-negative");
        return client.getBlock(blockNumber);
    }

    @Override
    public List<EvmRpcLog> logs(EvmLogFilter filter, long fromBlock, long toBlock) {
        Objects.requireNonNull(filter, "filter");
        if (fromBlock < 0 || toBlock < fromBlock) {
            throw new IllegalArgumentException(
                    "block interval must be non-negative and ordered");
        }
        List<EvmRpcLog> logs = new ArrayList<>();
        long pageStart = fromBlock;
        while (pageStart <= toBlock) {
            long rangeEnd = pageStart > Long.MAX_VALUE - config.maxBlockRange() + 1L
                    ? Long.MAX_VALUE
                    : pageStart + config.maxBlockRange() - 1L;
            long pageEnd = Math.min(toBlock, rangeEnd);
            readRange(filter, pageStart, pageEnd, logs);
            if (pageEnd == Long.MAX_VALUE) {
                break;
            }
            pageStart = pageEnd + 1L;
        }
        logs.sort(CHAIN_ORDER);
        return List.copyOf(logs);
    }

    @Override
    public byte[] call(String contractAddress, byte[] data, EvmBlockTag blockTag) {
        Objects.requireNonNull(contractAddress, "contractAddress");
        Objects.requireNonNull(data, "data");
        Objects.requireNonNull(blockTag, "blockTag");
        return client.call(
                normalizeAddress(contractAddress),
                data.clone(),
                blockTag);
    }

    @Override
    public byte[] code(String contractAddress, EvmBlockTag blockTag) {
        Objects.requireNonNull(contractAddress, "contractAddress");
        Objects.requireNonNull(blockTag, "blockTag");
        return client.code(normalizeAddress(contractAddress), blockTag);
    }

    private static String normalizeAddress(String contractAddress) {
        if (!EVM_ADDRESS.matcher(contractAddress).matches()) {
            throw new IllegalArgumentException("contractAddress must be a 20-byte hex value");
        }
        return contractAddress.toLowerCase(Locale.ROOT);
    }

    private void readRange(
            EvmLogFilter filter,
            long fromBlock,
            long toBlock,
            List<EvmRpcLog> destination) {
        try {
            destination.addAll(client.getLogs(filter, fromBlock, toBlock));
        } catch (EvmRpcException exception) {
            if (fromBlock == toBlock) {
                throw exception;
            }
            long midpoint = fromBlock + (toBlock - fromBlock) / 2L;
            readRange(filter, fromBlock, midpoint, destination);
            readRange(filter, midpoint + 1L, toBlock, destination);
        }
    }
}
