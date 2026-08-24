package com.qtsurfer.qtstreamx.core.ws;

import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Minimal WebSocket client abstraction.
 *
 * <p>Implementations: JDK 21 {@code java.net.http.WebSocket}, Tyrus, or any
 * JSR-356 compatible client.
 *
 * <p>This interface is intentionally thin — it covers only what exchange
 * stream clients need, without leaking transport-specific details.
 */
public interface WebSocketClient extends AutoCloseable {

    /**
     * Connects to the given WebSocket URL and blocks until connected.
     *
     * @param url runtime WebSocket endpoint
     * @throws Exception when the connection cannot be established
     */
    void connect(String url) throws Exception;

    /**
     * Connects within a caller-supplied deadline.
     *
     * <p>Implementations should override this method when their native transport supports a
     * cancellable handshake. The default isolates the legacy blocking method on a virtual thread,
     * cancels it on timeout, and never includes the URL in the timeout exception.
     *
     * @param url runtime WebSocket endpoint
     * @param timeout positive handshake timeout
     * @throws HttpTimeoutException when the handshake exceeds the timeout
     * @throws Exception when the underlying connection fails
     */
    default void connect(String url, Duration timeout) throws Exception {
        Objects.requireNonNull(url, "url");
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        Future<?> connection = executor.submit(() -> {
            connect(url);
            return null;
        });
        try {
            connection.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (TimeoutException exception) {
            connection.cancel(true);
            throw new HttpTimeoutException("WebSocket connection timed out");
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            connection.cancel(true);
            throw exception;
        } catch (ExecutionException exception) {
            Throwable cause = exception.getCause();
            if (cause instanceof Exception checked) {
                throw checked;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException("WebSocket connection failed");
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Sends a text message.
     *
     * @param message text payload
     */
    void send(String message);

    /**
     * Registers a handler for incoming text messages.
     *
     * @param handler message callback
     */
    void onMessage(Consumer<String> handler);

    /**
     * Registers a handler for connection close code and reason.
     *
     * @param handler close callback
     */
    void onClose(BiConsumer<Integer, String> handler);

    /**
     * Registers a handler for transport errors.
     *
     * @param handler error callback
     */
    void onError(Consumer<Throwable> handler);

    /**
     * Returns whether the connection is open.
     *
     * @return {@code true} when the connection is open
     */
    boolean isOpen();
}
