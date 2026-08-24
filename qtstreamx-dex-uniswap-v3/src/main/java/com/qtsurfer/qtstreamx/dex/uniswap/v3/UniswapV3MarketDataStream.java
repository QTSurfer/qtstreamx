package com.qtsurfer.qtstreamx.dex.uniswap.v3;

import com.qtsurfer.qtstreamx.core.client.MarketTradeAcknowledgement;
import com.qtsurfer.qtstreamx.core.client.MarketTradeBatch;
import com.qtsurfer.qtstreamx.core.client.MarketTradeBatchHandler;
import com.qtsurfer.qtstreamx.core.client.RecoverableMarketTradeStream;
import com.qtsurfer.qtstreamx.core.model.MarketTrade;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLog;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogAcknowledgement;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogBatch;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogFilter;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/** Converts confirmed logs for explicitly configured Uniswap v3 pools into trades. */
public final class UniswapV3MarketDataStream implements RecoverableMarketTradeStream {
    private final EvmLogStream source;
    private final Map<PoolKey, UniswapV3Pool> pools;
    private final AtomicBoolean started = new AtomicBoolean();
    private volatile Consumer<Throwable> errorHandler = ignored -> {};

    /**
     * Creates a market-data stream over an existing confirmed EVM log source.
     *
     * @param source confirmed EVM log source
     * @param configuredPools explicit pools consumed by this stream
     */
    public UniswapV3MarketDataStream(
            EvmLogStream source,
            Collection<UniswapV3Pool> configuredPools) {
        this.source = Objects.requireNonNull(source, "source");
        Objects.requireNonNull(configuredPools, "configuredPools");
        if (configuredPools.isEmpty()) {
            throw new IllegalArgumentException("configuredPools must not be empty");
        }
        pools = new LinkedHashMap<>();
        for (UniswapV3Pool pool : configuredPools) {
            UniswapV3Pool previous = pools.put(PoolKey.from(pool), pool);
            if (previous != null) {
                throw new IllegalArgumentException("duplicate configured pool " + pool.address());
            }
        }
        source.onError(error -> errorHandler.accept(error));
    }

    /**
     * Registers the non-terminal malformed-event and source error consumer.
     *
     * @param handler error consumer
     */
    @Override
    public void onError(Consumer<Throwable> handler) {
        errorHandler = Objects.requireNonNull(handler, "handler");
    }

    /**
     * Starts consuming all configured pools and emitting normalized trades.
     *
     * @param handler normalized-trade consumer
     * @throws Exception when the underlying source cannot start
     */
    @Override
    public void start(Consumer<MarketTrade> handler) throws Exception {
        Objects.requireNonNull(handler, "handler");
        begin();
        source.start(filter(), log -> handle(log, handler));
    }

    /**
     * Starts durable batch recovery and acknowledges a raw cursor only after normalization and
     * downstream acceptance complete.
     *
     * @param handler idempotent normalized-batch handler
     * @throws Exception when source recovery or startup fails
     */
    @Override
    public void startRecoverable(MarketTradeBatchHandler handler) throws Exception {
        Objects.requireNonNull(handler, "handler");
        begin();
        source.startRecoverable(filter(), batch -> handleRecoverable(batch, handler));
    }

    /**
     * Returns whether the underlying confirmed log source is connected.
     *
     * @return {@code true} while the underlying source is connected
     */
    @Override
    public boolean isConnected() {
        return source.isConnected();
    }

    @Override
    public void close() throws Exception {
        source.close();
    }

    private void handle(EvmLog log, Consumer<MarketTrade> handler) {
        UniswapV3Pool pool = pools.get(PoolKey.from(log));
        if (pool == null) {
            errorHandler.accept(new IllegalArgumentException(
                    "event does not belong to a configured pool"));
            return;
        }
        MarketTrade trade;
        try {
            trade = UniswapV3SwapDecoder.decode(pool, log);
        } catch (RuntimeException exception) {
            errorHandler.accept(exception);
            return;
        }
        handler.accept(trade);
    }

    private EvmLogAcknowledgement handleRecoverable(
            EvmLogBatch batch,
            MarketTradeBatchHandler handler) throws Exception {
        ArrayList<MarketTrade> trades = new ArrayList<>(batch.logs().size());
        try {
            for (EvmLog log : batch.logs()) {
                trades.add(decode(log));
            }
        } catch (RuntimeException exception) {
            errorHandler.accept(exception);
            throw exception;
        }
        MarketTradeAcknowledgement acknowledgement =
                Objects.requireNonNull(handler.handle(new MarketTradeBatch(trades)), "acknowledgement");
        return acknowledgement == MarketTradeAcknowledgement.ACKNOWLEDGED
                ? EvmLogAcknowledgement.ACKNOWLEDGED
                : EvmLogAcknowledgement.REJECTED;
    }

    private MarketTrade decode(EvmLog log) {
        UniswapV3Pool pool = pools.get(PoolKey.from(log));
        if (pool == null) {
            throw new IllegalArgumentException("event does not belong to a configured pool");
        }
        return UniswapV3SwapDecoder.decode(pool, log);
    }

    private EvmLogFilter filter() {
        Set<String> addresses = pools.values().stream()
                .map(UniswapV3Pool::address)
                .collect(Collectors.toUnmodifiableSet());
        return new EvmLogFilter(addresses, Set.of(UniswapV3SwapDecoder.SWAP_TOPIC));
    }

    private void begin() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("stream has already started");
        }
    }

    private record PoolKey(String network, String address) {
        private static PoolKey from(UniswapV3Pool pool) {
            return new PoolKey(pool.network(), pool.address());
        }

        private static PoolKey from(EvmLog log) {
            return new PoolKey(log.network(), log.address().toLowerCase(Locale.ROOT));
        }
    }
}
