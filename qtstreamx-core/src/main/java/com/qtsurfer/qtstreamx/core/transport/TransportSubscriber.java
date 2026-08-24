package com.qtsurfer.qtstreamx.core.transport;

import java.util.function.Consumer;

/**
 * Subscribes to messages from a transport layer.
 *
 * <p>Used by backends/engines to consume normalized data. The Rx bridge
 * ({@code qtstreamx-transport-nats-rx}) wraps this into {@code Observable<T>}.
 */
public interface TransportSubscriber extends AutoCloseable {

    /**
     * Subscribe to messages on the given subject pattern.
     *
     * @param subject subject or wildcard (e.g. "ticker.binance.>")
     * @param handler callback receiving raw encoded bytes
     */
    void subscribe(String subject, Consumer<byte[]> handler);
}
