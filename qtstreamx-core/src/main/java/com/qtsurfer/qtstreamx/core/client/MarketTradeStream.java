package com.qtsurfer.qtstreamx.core.client;

import com.qtsurfer.qtstreamx.core.model.MarketTrade;
import java.util.function.Consumer;

/**
 * Streams normalized trades from one or more explicitly configured markets.
 *
 * <p>Implementations own source-specific subscription, normalization, and
 * recovery behavior. A stream instance has one start lifecycle. Malformed
 * source events are reported to the error consumer without changing the
 * normalized trade interface.
 */
public interface MarketTradeStream extends AutoCloseable {

    /**
     * Registers the source and malformed-event error consumer.
     *
     * @param handler error consumer
     */
    void onError(Consumer<Throwable> handler);

    /**
     * Starts the stream and emits normalized trades.
     *
     * @param handler normalized-trade consumer
     * @throws Exception when the underlying source cannot start
     * @throws IllegalStateException when the stream was already started
     */
    void start(Consumer<MarketTrade> handler) throws Exception;

    /**
     * Returns whether the underlying live source is connected.
     *
     * @return {@code true} while the source is connected
     */
    boolean isConnected();

    /**
     * Closes the underlying source.
     *
     * @throws Exception when source shutdown fails
     */
    @Override
    void close() throws Exception;
}
