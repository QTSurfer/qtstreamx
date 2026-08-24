package com.qtsurfer.qtstreamx.link;

import java.time.Duration;

/**
 * Observer for WebSocket link lifecycle events.
 *
 * <p>Publisher-side wiring uses this to maintain drop / reconnect counters and outage histograms
 * exposed as Prometheus metrics. Default methods are no-ops so callers only implement what they
 * care about.
 *
 * <p>Implementations must be thread-safe — events are emitted from the link's scheduler thread or
 * from the WS reader callback path.
 */
public interface LinkLifecycleListener {

  LinkLifecycleListener NOOP = new LinkLifecycleListener() {};

  /**
   * Emitted whenever a link's WebSocket is (re)connected and resubscribed.
   *
   * @param linkId stable identifier, e.g. {@code binance#3}
   * @param instrumentCount instruments currently subscribed on this link
   * @param outage time the link was down before this connect. {@link Duration#ZERO} for the very
   *     first connect after construction; non-zero for every subsequent reconnect.
   */
  default void onConnected(String linkId, int instrumentCount, Duration outage) {}

  /** Emitted as soon as the WS layer signals disconnect, before backoff starts. */
  default void onDisconnected(String linkId) {}

  /**
   * Emitted when one reconnect attempt fails. {@link #onConnected} fires on the eventual success;
   * if the link is closed before then, no terminal event is emitted.
   */
  default void onReconnectFailed(String linkId, int attempt, Throwable cause) {}

  /**
   * Emitted, repeatedly, while a link is connected but delivering no market data — the failure the
   * connect/disconnect events above cannot express, because nothing disconnects. Fired once per
   * watchdog pass per silent link, so implementations should be idempotent and cheap; treat it as a
   * level, not an edge.
   *
   * @param linkId stable identifier, e.g. {@code bybit-linear#3}
   * @param silence how long since the last message on this link; falls back to the link's uptime
   *     when it has never received one
   * @param instrumentCount instruments subscribed on this link — the size of the capture hole
   */
  default void onSilent(String linkId, Duration silence, int instrumentCount) {}
}
