package com.qtsurfer.qtstreamx.core.transport;

/**
 * Publishes encoded messages to a transport layer.
 *
 * <p>Implementations: NATS, Kafka, Redis Streams, or any pub/sub system.
 * Used by satellites to push normalized data.
 */
public interface TransportPublisher extends AutoCloseable {

    /**
     * Publish a message to the given subject.
     *
     * @param subject routing subject (e.g. "ticker.binance.BTC-USDT")
     * @param data    encoded payload
     */
    void publish(String subject, byte[] data);
}
