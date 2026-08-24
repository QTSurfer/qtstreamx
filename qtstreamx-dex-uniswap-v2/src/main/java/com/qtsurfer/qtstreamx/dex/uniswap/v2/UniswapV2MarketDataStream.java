package com.qtsurfer.qtstreamx.dex.uniswap.v2;

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

/** Converts confirmed logs for explicitly configured Uniswap v2 pairs into trades. */
public final class UniswapV2MarketDataStream implements RecoverableMarketTradeStream {
    private final EvmLogStream source;
    private final Map<PairKey, UniswapV2Pair> pairs;
    private final AtomicBoolean started = new AtomicBoolean();
    private volatile Consumer<Throwable> errorHandler = ignored -> {};

    /**
     * Creates a market-data stream over an existing confirmed EVM log source.
     *
     * @param source confirmed EVM log source
     * @param configuredPairs explicit pairs consumed by this stream
     */
    public UniswapV2MarketDataStream(
            EvmLogStream source,
            Collection<UniswapV2Pair> configuredPairs) {
        this.source = Objects.requireNonNull(source, "source");
        Objects.requireNonNull(configuredPairs, "configuredPairs");
        if (configuredPairs.isEmpty()) {
            throw new IllegalArgumentException("configuredPairs must not be empty");
        }
        pairs = new LinkedHashMap<>();
        for (UniswapV2Pair pair : configuredPairs) {
            UniswapV2Pair previous = pairs.put(PairKey.from(pair), pair);
            if (previous != null) {
                throw new IllegalArgumentException("duplicate configured pair " + pair.address());
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
     * Starts consuming all configured pairs and emitting normalized trades.
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
        UniswapV2Pair pair = pairs.get(PairKey.from(log));
        if (pair == null) {
            errorHandler.accept(new IllegalArgumentException(
                    "event does not belong to a configured pair"));
            return;
        }
        MarketTrade trade;
        try {
            trade = UniswapV2SwapDecoder.decode(pair, log);
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
        UniswapV2Pair pair = pairs.get(PairKey.from(log));
        if (pair == null) {
            throw new IllegalArgumentException("event does not belong to a configured pair");
        }
        return UniswapV2SwapDecoder.decode(pair, log);
    }

    private EvmLogFilter filter() {
        Set<String> addresses = pairs.values().stream()
                .map(UniswapV2Pair::address)
                .collect(Collectors.toUnmodifiableSet());
        return new EvmLogFilter(addresses, Set.of(UniswapV2SwapDecoder.SWAP_TOPIC));
    }

    private void begin() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("stream has already started");
        }
    }

    private record PairKey(String network, String address) {
        private static PairKey from(UniswapV2Pair pair) {
            return new PairKey(pair.network(), pair.address());
        }

        private static PairKey from(EvmLog log) {
            return new PairKey(log.network(), log.address().toLowerCase(Locale.ROOT));
        }
    }
}
