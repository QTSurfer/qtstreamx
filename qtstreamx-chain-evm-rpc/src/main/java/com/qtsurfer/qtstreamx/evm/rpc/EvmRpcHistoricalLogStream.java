package com.qtsurfer.qtstreamx.evm.rpc;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Bounded, deterministic replay of confirmed logs over one closed block range.
 *
 * <p>Unlike {@link EvmRpcLogStream}, this stream never opens a WebSocket, never checkpoints, and
 * never applies an {@link EvmRecoveryPolicy#maxReplayBlocks()}-style ceiling. That guard exists to
 * stop a live tailing stream from silently skipping a backlog it fell behind on; a deliberate,
 * explicitly bounded historical range is not a backlog, so the ceiling simply does not apply here
 * rather than being raised. The only safety check performed is that the requested range must
 * already be confirmed — see {@link EvmHistoricalRangeNotFinalizedException}.
 *
 * <p>Replaying the same {@code [fromBlock, toBlock]} against unchanged chain state always emits
 * the same logs in the same order: {@link EvmRpcReader#logs} already returns chain-ordered
 * results, and canonicalization here only ever drops non-canonical entries, never reorders them.
 *
 * <p>Because there is no checkpoint, a failed or interrupted replay restarts from {@code fromBlock}
 * rather than resuming.
 */
public final class EvmRpcHistoricalLogStream implements EvmLogStream {
    private final EvmRpcReader reader;
    private final String network;
    private final long fromBlock;
    private final long toBlock;
    private final int confirmationDepth;
    private volatile Consumer<Throwable> errorHandler = ignored -> {};
    private volatile EvmLogStreamStatus status = EvmLogStreamStatus.IDLE;

    /**
     * Creates a bounded historical reader backed by the JDK HTTP client.
     *
     * @param config network, endpoint, and bounds for read-only RPC access
     * @param fromBlock first block eligible for replay, inclusive
     * @param toBlock last block eligible for replay, inclusive
     * @param confirmationDepth required canonical descendant blocks before a block is safe to read
     */
    public EvmRpcHistoricalLogStream(
            EvmRpcReaderConfig config,
            long fromBlock,
            long toBlock,
            int confirmationDepth) {
        this(
                new EvmHttpRpcReader(Objects.requireNonNull(config, "config")),
                config.network(),
                fromBlock,
                toBlock,
                confirmationDepth);
    }

    EvmRpcHistoricalLogStream(
            EvmRpcReader reader,
            String network,
            long fromBlock,
            long toBlock,
            int confirmationDepth) {
        this.reader = Objects.requireNonNull(reader, "reader");
        this.network = Objects.requireNonNull(network, "network");
        if (fromBlock < 0 || toBlock < fromBlock) {
            throw new IllegalArgumentException("block range must be non-negative and ordered");
        }
        if (confirmationDepth < 0) {
            throw new IllegalArgumentException("confirmationDepth must be non-negative");
        }
        this.fromBlock = fromBlock;
        this.toBlock = toBlock;
        this.confirmationDepth = confirmationDepth;
    }

    /**
     * Replays every confirmed log in {@code [fromBlock, toBlock]} matching the filter, then
     * completes.
     *
     * @param filter contracts and event topics to observe
     * @param handler confirmed-log consumer, invoked once per canonical log in chain order
     * @throws EvmHistoricalRangeNotFinalizedException if {@code toBlock} is not yet confirmed safe
     * @throws IllegalStateException if this stream was already started
     */
    @Override
    public void start(EvmLogFilter filter, Consumer<EvmLog> handler) {
        Objects.requireNonNull(filter, "filter");
        Objects.requireNonNull(handler, "handler");
        if (status != EvmLogStreamStatus.IDLE) {
            throw new IllegalStateException("historical stream can only be started once");
        }
        status = EvmLogStreamStatus.RECOVERING;
        try {
            long safeBlock = reader.latestBlockNumber() - confirmationDepth;
            if (toBlock > safeBlock) {
                throw new EvmHistoricalRangeNotFinalizedException(network, toBlock, safeBlock);
            }
            List<EvmRpcLog> rawLogs = reader.logs(filter, fromBlock, toBlock);
            Map<Long, EvmBlock> blocks = new LinkedHashMap<>();
            for (EvmRpcLog log : rawLogs) {
                if (log.removed()) {
                    continue;
                }
                EvmBlock block = blocks.computeIfAbsent(log.blockNumber(), reader::block);
                if (!log.blockHash().equals(block.hash())) {
                    continue;
                }
                handler.accept(confirmed(log, block.timestamp()));
            }
            status = EvmLogStreamStatus.COMPLETED;
        } catch (RuntimeException exception) {
            status = EvmLogStreamStatus.FAILED;
            errorHandler.accept(exception);
            throw exception;
        }
    }

    @Override
    public void onError(Consumer<Throwable> handler) {
        errorHandler = Objects.requireNonNull(handler, "handler");
    }

    @Override
    public boolean isConnected() {
        return false;
    }

    @Override
    public EvmLogStreamStatus status() {
        return status;
    }

    @Override
    public void close() {
        if (status != EvmLogStreamStatus.COMPLETED && status != EvmLogStreamStatus.FAILED) {
            status = EvmLogStreamStatus.CLOSED;
        }
    }

    private EvmLog confirmed(EvmRpcLog log, long timestamp) {
        return new EvmLog(
                network,
                log.address(),
                log.topics(),
                log.data(),
                log.blockNumber(),
                log.blockHash(),
                log.transactionHash(),
                log.transactionIndex(),
                log.logIndex(),
                timestamp);
    }
}
