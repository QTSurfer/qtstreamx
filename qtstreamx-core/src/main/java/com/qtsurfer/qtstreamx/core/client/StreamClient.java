package com.qtsurfer.qtstreamx.core.client;

import com.qtsurfer.qtstreamx.core.model.FundingRate;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.model.Kline;
import com.qtsurfer.qtstreamx.core.model.Ticker;
import java.util.function.Consumer;

/**
 * Exchange-specific streaming client.
 *
 * <p>Each exchange module provides an implementation that connects via WebSocket,
 * parses exchange-native messages, and delivers normalized model objects.
 *
 * <p>Implementations must handle reconnection internally. Callbacks are invoked
 * on the WebSocket reader thread — handlers should be non-blocking.
 */
public interface StreamClient extends AutoCloseable {

    /** Connect to the exchange WebSocket. */
    void connect() throws Exception;

    /** Subscribe to normalized ticker updates for an instrument. */
    void subscribeTicker(Instrument instrument, Consumer<Ticker> handler);

    /** Subscribe to kline/candle updates. */
    void subscribeKline(Instrument instrument, String interval, Consumer<Kline> handler);

    /** Subscribe to funding rate updates (derivatives only). */
    void subscribeFundingRate(Instrument instrument, Consumer<FundingRate> handler);

    /** True if the WebSocket connection is open. */
    boolean isConnected();

    /**
     * Register a callback invoked when the connection drops (close or error).
     * Implementations must ensure the callback fires at most once per {@link
     * #connect()} lifecycle, even when both onClose and onError arrive for the
     * same dropout. A Link wraps this with {@code reconnectWithBackoff} so
     * dropouts automatically trigger reconnects — wiring the callback is the
     * only way for an exchange-independent supervisor to notice dropouts.
     */
    default void onDisconnect(Runnable handler) {
        // no-op by default; implementations should override to trigger handler
        // from their onClose/onError paths.
    }
}
