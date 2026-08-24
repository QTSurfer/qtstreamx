package com.qtsurfer.qtstreamx.ws.jdk;

import com.qtsurfer.qtstreamx.core.ws.WebSocketClient;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpTimeoutException;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDK-based WebSocket client that extends {@link JdkWebSocketClient} behaviour with:
 *
 * <ul>
 *   <li><b>Gzip binary frames</b> — HTX, Bitget and Gate.io send compressed payloads as
 *       WebSocket binary frames. Binary data is accumulated across fragmented frames and
 *       inflated with {@link GZIPInputStream} before being delivered as UTF-8 text to the
 *       downstream handler.
 *   <li><b>Auto-pong</b> for exchanges that send JSON pings inside the data channel rather
 *       than WebSocket control frames:
 *       <ul>
 *         <li>HTX: server sends {@code {"ping":<ts>}} → replied with {@code {"pong":<ts>}}.
 *         <li>OKX: server sends plain-text {@code "ping"} → replied with {@code "pong"}.
 *       </ul>
 *       Bybit/Bitget/Binance need client-initiated pings; those are still a transport
 *       concern handled above this class.
 * </ul>
 *
 * The decompressed text (or any text frame) is delivered to the message handler <em>after</em>
 * the auto-pong is sent. Downstream stream clients still see the ping payload (they already
 * filter it); keeping it in the stream makes ping/pong visible for debugging.
 */
public class GzipAwareJdkWebSocketClient implements WebSocketClient {

    private static final Logger log = LoggerFactory.getLogger(GzipAwareJdkWebSocketClient.class);

    private final HttpClient httpClient;
    private WebSocket webSocket;
    private Consumer<String> messageHandler;
    private BiConsumer<Integer, String> closeHandler;
    private Consumer<Throwable> errorHandler;

    /** Creates a gzip-aware client backed by a dedicated JDK HTTP client. */
    public GzipAwareJdkWebSocketClient() {
        this.httpClient = HttpClient.newHttpClient();
    }

    @Override
    public void connect(String url) {
        webSocket = httpClient.newWebSocketBuilder()
                .buildAsync(endpoint(url), new Listener())
                .join();
        log.debug("WebSocket connected");
    }

    /** {@inheritDoc} */
    @Override
    public void connect(String url, Duration timeout) throws Exception {
        CompletableFuture<WebSocket> connection = httpClient.newWebSocketBuilder()
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
        if (webSocket != null) webSocket.sendText(message, true);
    }

    @Override
    public void onMessage(Consumer<String> handler) { this.messageHandler = handler; }

    @Override
    public void onClose(BiConsumer<Integer, String> handler) { this.closeHandler = handler; }

    @Override
    public void onError(Consumer<Throwable> handler) { this.errorHandler = handler; }

    @Override
    public boolean isOpen() { return webSocket != null && !webSocket.isOutputClosed(); }

    @Override
    public void close() {
        if (webSocket != null) {
            webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "closing")
                    .exceptionally(t -> { log.debug("WebSocket close failed"); return null; });
        }
        // Release the HttpClient's native resources (SelectorManager + worker
        // threads); see JdkWebSocketClient.close() for the leak rationale.
        // shutdownNow() is non-blocking and safe on the reconnect path.
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

    static String inflate(byte[] compressed) throws java.io.IOException {
        try (GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(compressed))) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    /** Build pong for a well-known ping pattern, or null to skip. Package-private for tests. */
    static String maybePong(String message) {
        if (message == null || message.length() > 64) return null;
        if ("ping".equals(message)) return "pong";
        // HTX-style {"ping":1234567890}
        if (message.startsWith("{\"ping\":")) {
            int colon = message.indexOf(':');
            int end = message.indexOf('}', colon);
            if (end < 0) return null;
            String ts = message.substring(colon + 1, end).trim();
            return "{\"pong\":" + ts + "}";
        }
        return null;
    }

    private class Listener implements WebSocket.Listener {
        private final StringBuilder textBuffer = new StringBuilder();
        private final ByteArrayOutputStream binaryBuffer = new ByteArrayOutputStream();

        @Override
        public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
            textBuffer.append(data);
            if (last) {
                String message = textBuffer.toString();
                textBuffer.setLength(0);
                deliver(ws, message);
            }
            ws.request(1);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<?> onBinary(WebSocket ws, ByteBuffer data, boolean last) {
            byte[] chunk = new byte[data.remaining()];
            data.get(chunk);
            binaryBuffer.write(chunk, 0, chunk.length);
            if (last) {
                byte[] all = binaryBuffer.toByteArray();
                binaryBuffer.reset();
                String message;
                try {
                    message = inflate(all);
                } catch (java.io.IOException e) {
                    // Not gzip — deliver as UTF-8 raw
                    message = new String(all, StandardCharsets.UTF_8);
                }
                deliver(ws, message);
            }
            ws.request(1);
            return CompletableFuture.completedFuture(null);
        }

        private void deliver(WebSocket ws, String message) {
            String pong = maybePong(message);
            if (pong != null) {
                try {
                    ws.sendText(pong, true);
                } catch (RuntimeException e) {
                    log.debug("Auto-pong send failed");
                }
            }
            if (messageHandler != null) messageHandler.accept(message);
        }

        @Override
        public CompletionStage<?> onClose(WebSocket ws, int statusCode, String reason) {
            if (closeHandler != null) closeHandler.accept(statusCode, reason);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void onError(WebSocket ws, Throwable error) {
            if (errorHandler != null) errorHandler.accept(error);
        }

        @Override
        public CompletionStage<?> onPing(WebSocket ws, ByteBuffer message) {
            ws.sendPong(message);
            ws.request(1);
            return CompletableFuture.completedFuture(null);
        }
    }
}
