package com.qtsurfer.qtstreamx.link;

import java.time.Duration;

/**
 * WebSocket limits published by an exchange. Used by the {@link LinkPartitioner}
 * to size each {@link Link}'s subscription load and by {@link Link} itself to
 * schedule preemptive reconnects before the exchange kills the connection.
 *
 * <p>Values for reference (spot):
 *
 * <ul>
 *   <li>Binance: 300 conn / 5 min per IP, 1024 streams / conn, 5 msg/s, 24 h lifetime
 *   <li>Binance futures: same 300 conn / 5 min, 200 streams / conn, 10 msg/s, 24 h
 *   <li>Bybit: 500 subs / conn, ping every 20 s
 *   <li>OKX: no hard stream cap, ping every 30 s
 * </ul>
 *
 * @param maxConnectionsPer5MinPerIp upper bound on new connections from one IP in a 5-minute
 *     window; exceeding triggers an IP ban on Binance
 * @param maxStreamsPerConnection hard cap on streams a single WebSocket can carry
 * @param maxIncomingMsgPerSec upper bound on client-to-server control messages per second (SUBSCRIBE,
 *     UNSUBSCRIBE, pongs)
 * @param connectionLifetime server-side forced close interval; links schedule a preemptive reconnect
 *     at {@code connectionLifetime - renewalSafetyMargin}
 * @param renewalSafetyMargin how far ahead of {@code connectionLifetime} to reconnect
 */
public record WsLimits(
    int maxConnectionsPer5MinPerIp,
    int maxStreamsPerConnection,
    int maxIncomingMsgPerSec,
    Duration connectionLifetime,
    Duration renewalSafetyMargin) {

  public WsLimits {
    if (maxConnectionsPer5MinPerIp <= 0) {
      throw new IllegalArgumentException("maxConnectionsPer5MinPerIp must be > 0");
    }
    if (maxStreamsPerConnection <= 0) {
      throw new IllegalArgumentException("maxStreamsPerConnection must be > 0");
    }
    if (maxIncomingMsgPerSec <= 0) {
      throw new IllegalArgumentException("maxIncomingMsgPerSec must be > 0");
    }
    if (connectionLifetime == null || connectionLifetime.isNegative() || connectionLifetime.isZero()) {
      throw new IllegalArgumentException("connectionLifetime must be positive");
    }
    if (renewalSafetyMargin == null) {
      renewalSafetyMargin = Duration.ofMinutes(10);
    }
    if (renewalSafetyMargin.compareTo(connectionLifetime) >= 0) {
      throw new IllegalArgumentException("renewalSafetyMargin must be < connectionLifetime");
    }
  }

  /** Presets for well-known exchanges. */
  public static WsLimits binanceSpot() {
    return new WsLimits(300, 1024, 5, Duration.ofHours(24), Duration.ofMinutes(10));
  }

  public static WsLimits binanceFutures() {
    return new WsLimits(300, 200, 10, Duration.ofHours(24), Duration.ofMinutes(10));
  }

  public static WsLimits bybit() {
    // Bybit has no documented 24h cap; set a generous lifetime for symmetry.
    return new WsLimits(100, 500, 10, Duration.ofHours(12), Duration.ofMinutes(5));
  }

  public static WsLimits okx() {
    return new WsLimits(100, 256, 10, Duration.ofHours(12), Duration.ofMinutes(5));
  }

  /** Effective window for preemptive reconnect. */
  public Duration renewBefore() {
    return connectionLifetime.minus(renewalSafetyMargin);
  }
}
