package com.qtsurfer.qtstreamx.core.client;

import com.qtsurfer.qtstreamx.core.ws.WebSocketClient;
import java.time.Duration;
import java.util.function.Supplier;

/**
 * Configuration for a {@link StreamClient}.
 *
 * @param wsClientFactory factory for WebSocket client instances
 * @param reconnectDelay  initial delay before reconnect attempts
 * @param maxReconnects   max consecutive reconnect attempts (0 = unlimited)
 * @param pingInterval    WebSocket ping interval (exchange-specific default if null)
 */
public record StreamClientConfig(
        Supplier<WebSocketClient> wsClientFactory,
        Duration reconnectDelay,
        int maxReconnects,
        Duration pingInterval
) {
    public StreamClientConfig {
        if (reconnectDelay == null) reconnectDelay = Duration.ofSeconds(1);
        if (maxReconnects < 0) maxReconnects = 0;
        if (pingInterval == null) pingInterval = Duration.ofSeconds(20);
    }

    public static StreamClientConfig withDefaults(Supplier<WebSocketClient> wsClientFactory) {
        return new StreamClientConfig(wsClientFactory, Duration.ofSeconds(1), 0, Duration.ofSeconds(20));
    }
}
