package com.qtsurfer.qtstreamx.canary;

import com.qtsurfer.qtstreamx.core.ws.WebSocketClient;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Decorator that ties any {@link WebSocketClient} to a {@link FrameRecorder} so every decoded
 * incoming frame and every outbound frame is captured to disk alongside normal dispatch.
 *
 * Used by {@code CaptureMain} to tee WS activity into a JSONL log without changing the exchange
 * stream clients.
 */
final class RecordingWebSocketClient implements WebSocketClient {

    private final WebSocketClient delegate;
    private final FrameRecorder recorder;
    private final String endpointTag;
    private String connectedUrl;

    RecordingWebSocketClient(WebSocketClient delegate, FrameRecorder recorder, String endpointTag) {
        this.delegate = delegate;
        this.recorder = recorder;
        this.endpointTag = endpointTag;
    }

    @Override
    public void connect(String url) throws Exception {
        this.connectedUrl = url;
        recorder.recordLifecycle(endpointTag, "connect", url);
        delegate.connect(url);
    }

    @Override
    public void send(String message) {
        recorder.recordOutbound(endpointTag, message);
        delegate.send(message);
    }

    @Override
    public void onMessage(Consumer<String> handler) {
        delegate.onMessage(message -> {
            recorder.recordInbound(endpointTag, message);
            handler.accept(message);
        });
    }

    @Override
    public void onClose(BiConsumer<Integer, String> handler) {
        delegate.onClose((code, reason) -> {
            recorder.recordLifecycle(endpointTag, "close", code + " " + reason);
            handler.accept(code, reason);
        });
    }

    @Override
    public void onError(Consumer<Throwable> handler) {
        delegate.onError(err -> {
            recorder.recordLifecycle(endpointTag, "error", err.toString());
            handler.accept(err);
        });
    }

    @Override
    public boolean isOpen() { return delegate.isOpen(); }

    @Override
    public void close() throws Exception {
        recorder.recordLifecycle(endpointTag, "close-local", connectedUrl == null ? "" : connectedUrl);
        delegate.close();
    }
}
