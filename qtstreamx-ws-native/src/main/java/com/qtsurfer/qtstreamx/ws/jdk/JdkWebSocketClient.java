package com.qtsurfer.qtstreamx.ws.jdk;

import com.qtsurfer.qtstreamx.core.ws.WebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * WebSocket client implementation using JDK 21+ {@code java.net.http.WebSocket}.
 * Zero external dependencies — uses only the standard library.
 */
public class JdkWebSocketClient implements WebSocketClient {

    private static final Logger log = LoggerFactory.getLogger(JdkWebSocketClient.class);

    private final HttpClient httpClient;
    private java.net.http.WebSocket webSocket;
    private Consumer<String> messageHandler;
    private BiConsumer<Integer, String> closeHandler;
    private Consumer<Throwable> errorHandler;

    /** Creates a client backed by a dedicated JDK HTTP client. */
    public JdkWebSocketClient() {
        this.httpClient = HttpClient.newHttpClient();
    }

    @Override
    public void connect(String url) throws Exception {
        webSocket = httpClient.newWebSocketBuilder()
                .buildAsync(endpoint(url), new Listener())
                .join();
        log.debug("WebSocket connected");
    }

    /** {@inheritDoc} */
    @Override
    public void connect(String url, Duration timeout) throws Exception {
        CompletableFuture<java.net.http.WebSocket> connection = httpClient.newWebSocketBuilder()
                .buildAsync(endpoint(url), new Listener());
        try {
            webSocket = connection.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
            log.debug("WebSocket connected");
        } catch (TimeoutException exception) {
            connection.cancel(true);
            throw new HttpTimeoutException("WebSocket connection timed out");
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            connection.cancel(true);
            throw exception;
        } catch (ExecutionException exception) {
            throw new IllegalStateException("WebSocket connection failed");
        }
    }

    @Override
    public void send(String message) {
        if (webSocket != null) {
            webSocket.sendText(message, true);
        }
    }

    @Override
    public void onMessage(Consumer<String> handler) {
        this.messageHandler = handler;
    }

    @Override
    public void onClose(BiConsumer<Integer, String> handler) {
        this.closeHandler = handler;
    }

    @Override
    public void onError(Consumer<Throwable> handler) {
        this.errorHandler = handler;
    }

    @Override
    public boolean isOpen() {
        return webSocket != null && !webSocket.isOutputClosed();
    }

    @Override
    public void close() {
        if (webSocket != null) {
            webSocket.sendClose(java.net.http.WebSocket.NORMAL_CLOSURE, "closing")
                    .exceptionally(t -> { log.debug("WebSocket close failed"); return null; });
        }
        // Release the HttpClient's native resources (SelectorManager + worker
        // threads). Without this, every reconnect leaks an HttpClient whose
        // threads keep running, eventually exhausting native thread stacks in
        // a constrained cgroup ("OutOfMemoryError: Unable to create native
        // thread") and zombifying the process. shutdownNow() is non-blocking
        // and safe to call on the WS-reader/reconnect path; close() would block
        // until in-flight operations finish, which can hang on an erroring client.
        if (httpClient != null) {
            try {
                httpClient.shutdownNow();
            } catch (Exception e) {
                log.debug("HttpClient shutdown failed");
            }
        }
    }

    private static URI endpoint(String url) {
        try {
            return URI.create(url);
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("WebSocket URL is invalid");
        }
    }

    private class Listener implements java.net.http.WebSocket.Listener {
        private final StringBuilder buffer = new StringBuilder();

        @Override
        public CompletionStage<?> onText(java.net.http.WebSocket ws, CharSequence data, boolean last) {
            buffer.append(data);
            if (last) {
                String message = buffer.toString();
                buffer.setLength(0);
                if (messageHandler != null) {
                    messageHandler.accept(message);
                }
            }
            ws.request(1);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<?> onClose(java.net.http.WebSocket ws, int statusCode, String reason) {
            log.debug("WebSocket closed with status {}", statusCode);
            if (closeHandler != null) {
                closeHandler.accept(statusCode, reason);
            }
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void onError(java.net.http.WebSocket ws, Throwable error) {
            log.warn("WebSocket transport error ({})", error.getClass().getSimpleName());
            if (errorHandler != null) {
                errorHandler.accept(error);
            }
        }

        @Override
        public CompletionStage<?> onPing(java.net.http.WebSocket ws, ByteBuffer message) {
            ws.sendPong(message);
            ws.request(1);
            return CompletableFuture.completedFuture(null);
        }
    }
}
