package com.qtsurfer.qtstreamx.ws.javaws;

import com.qtsurfer.qtstreamx.core.ws.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * WebSocket client implementation using TooTallNate's Java-WebSocket library.
 *
 * <p>Battle-tested in crypto streaming (used internally by XChange).
 * Zero external dependencies beyond the library itself.
 */
public class JavaWsClient implements WebSocketClient {

    private static final Logger log = LoggerFactory.getLogger(JavaWsClient.class);
    private static final int CONNECT_TIMEOUT_SECONDS = 10;

    private Consumer<String> messageHandler;
    private BiConsumer<Integer, String> closeHandler;
    private Consumer<Throwable> errorHandler;
    private InnerClient client;

    @Override
    public void connect(String url) throws Exception {
        var latch = new CountDownLatch(1);
        client = new InnerClient(URI.create(url), latch);
        client.connect();
        if (!latch.await(CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            client.close();
            throw new Exception("WebSocket connect timeout after " + CONNECT_TIMEOUT_SECONDS + "s: " + url);
        }
        log.debug("Connected to {}", url);
    }

    @Override
    public void send(String message) {
        if (client != null && client.isOpen()) {
            client.send(message);
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
        return client != null && client.isOpen();
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
    }

    private class InnerClient extends org.java_websocket.client.WebSocketClient {
        private final CountDownLatch connectLatch;

        InnerClient(URI uri, CountDownLatch connectLatch) {
            super(uri);
            this.connectLatch = connectLatch;
        }

        @Override
        public void onOpen(ServerHandshake handshake) {
            connectLatch.countDown();
        }

        @Override
        public void onMessage(String message) {
            if (messageHandler != null) {
                messageHandler.accept(message);
            }
        }

        @Override
        public void onClose(int code, String reason, boolean remote) {
            log.debug("WebSocket closed: {} {} (remote={})", code, reason, remote);
            if (closeHandler != null) {
                closeHandler.accept(code, reason);
            }
        }

        @Override
        public void onError(Exception ex) {
            log.warn("WebSocket error: {}", ex.getMessage());
            connectLatch.countDown();
            if (errorHandler != null) {
                errorHandler.accept(ex);
            }
        }
    }
}
