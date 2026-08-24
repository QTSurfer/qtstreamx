package com.qtsurfer.qtstreamx.ws.jdk;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.Test;

class GzipAwareJdkWebSocketClientTest {

    @Test
    void inflatesGzippedPayload() throws Exception {
        String payload = "{\"ch\":\"market.btcusdt.detail\",\"ts\":1,\"tick\":{}}";
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream out = new GZIPOutputStream(baos)) {
            out.write(payload.getBytes(StandardCharsets.UTF_8));
        }
        String inflated = GzipAwareJdkWebSocketClient.inflate(baos.toByteArray());
        assertThat(inflated).isEqualTo(payload);
    }

    @Test
    void htxStylePingProducesPong() {
        assertThat(GzipAwareJdkWebSocketClient.maybePong("{\"ping\":1713715200000}"))
                .isEqualTo("{\"pong\":1713715200000}");
    }

    @Test
    void plainTextPingProducesPong() {
        assertThat(GzipAwareJdkWebSocketClient.maybePong("ping")).isEqualTo("pong");
    }

    @Test
    void dataPayloadDoesNotTriggerPong() {
        String msg = "{\"ch\":\"market.btcusdt.detail\",\"ts\":1,\"tick\":{\"close\":65000}}";
        assertThat(GzipAwareJdkWebSocketClient.maybePong(msg)).isNull();
    }

    @Test
    void oversizedPingIgnored() {
        // >64 bytes won't be treated as ping — reduces false-positives on data messages.
        String oversized = "{\"ping\":" + "1".repeat(80) + "}";
        assertThat(GzipAwareJdkWebSocketClient.maybePong(oversized)).isNull();
    }

    @Test
    void nullAndEmptyAreSafe() {
        assertThat(GzipAwareJdkWebSocketClient.maybePong(null)).isNull();
        assertThat(GzipAwareJdkWebSocketClient.maybePong("")).isNull();
    }
}
