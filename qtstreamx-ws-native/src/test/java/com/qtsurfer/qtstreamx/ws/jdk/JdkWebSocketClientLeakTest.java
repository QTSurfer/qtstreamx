package com.qtsurfer.qtstreamx.ws.jdk;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.http.HttpClient;
import org.junit.jupiter.api.Test;

/**
 * Regression test for the HttpClient native-thread leak. Before the fix,
 * {@code close()} closed only the WebSocket and never the underlying
 * {@link HttpClient}, so each reconnect orphaned an HttpClient whose
 * SelectorManager + worker threads kept running, eventually exhausting native
 * thread stacks ("OutOfMemoryError: Unable to create native thread") under
 * sustained reconnect churn in a memory-constrained process.
 *
 * <p>These tests assert that {@code close()} shuts the HttpClient down so its
 * threads do not accumulate across repeated create→connect→close cycles.
 */
class JdkWebSocketClientLeakTest {

    /** Pull the private HttpClient out so we can assert on its lifecycle directly. */
    private static HttpClient httpClientOf(Object wsClient) throws Exception {
        Field f = wsClient.getClass().getDeclaredField("httpClient");
        f.setAccessible(true);
        return (HttpClient) f.get(wsClient);
    }

    private static int selectorManagerThreadCount() {
        int n = 0;
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            String name = t.getName();
            if (name != null && name.startsWith("HttpClient-") && name.contains("SelectorManager")) {
                n++;
            }
        }
        return n;
    }

    @Test
    void closeTerminatesUnderlyingHttpClient() throws Exception {
        JdkWebSocketClient client = new JdkWebSocketClient();
        HttpClient http = httpClientOf(client);
        // Force the HttpClient to spin up its selector/worker threads by issuing
        // a connect attempt to an unroutable address (fails fast, but the
        // SelectorManager is created lazily on first use).
        try {
            client.connect("ws://127.0.0.1:1");
        } catch (Exception expected) {
            // connection refused — we only needed the client to initialise.
        }

        client.close();

        // close() must terminate the HttpClient. shutdownNow() is async, so
        // poll briefly for isTerminated() rather than asserting immediately.
        boolean terminated = false;
        for (int i = 0; i < 50 && !terminated; i++) {
            terminated = http.isTerminated();
            if (!terminated) Thread.sleep(20);
        }
        assertThat(terminated)
                .as("HttpClient must be terminated after close()")
                .isTrue();
    }

    @Test
    void repeatedCreateCloseDoesNotLeakSelectorThreads() throws Exception {
        int before = selectorManagerThreadCount();

        for (int i = 0; i < 30; i++) {
            JdkWebSocketClient client = new JdkWebSocketClient();
            try {
                client.connect("ws://127.0.0.1:1");
            } catch (Exception expected) {
                // ignore — connect failure still leaves an HttpClient to close.
            }
            client.close();
        }

        // Give the selector managers a moment to wind down after shutdownNow().
        for (int i = 0; i < 50; i++) {
            if (selectorManagerThreadCount() <= before) break;
            Thread.sleep(40);
        }

        int after = selectorManagerThreadCount();
        // Allow a tiny slack for a thread that is mid-teardown, but the leak (pre-fix)
        // would have left ~30 orphaned SelectorManager threads here.
        assertThat(after - before)
                .as("SelectorManager threads must not accumulate across close() cycles")
                .isLessThanOrEqualTo(2);
    }

    @Test
    void gzipVariantCloseTerminatesUnderlyingHttpClient() throws Exception {
        GzipAwareJdkWebSocketClient client = new GzipAwareJdkWebSocketClient();
        HttpClient http = httpClientOf(client);
        try {
            client.connect("ws://127.0.0.1:1");
        } catch (Exception expected) {
            // ignore.
        }

        client.close();

        boolean terminated = false;
        for (int i = 0; i < 50 && !terminated; i++) {
            terminated = http.isTerminated();
            if (!terminated) Thread.sleep(20);
        }
        assertThat(terminated)
                .as("GzipAware HttpClient must be terminated after close()")
                .isTrue();
    }

    @Test
    void closeIsIdempotentAndNullSafe() {
        // A client that never connected (httpClient created, webSocket null) must
        // close cleanly, and a second close() must not throw.
        JdkWebSocketClient client = new JdkWebSocketClient();
        client.close();
        client.close();
    }
}
