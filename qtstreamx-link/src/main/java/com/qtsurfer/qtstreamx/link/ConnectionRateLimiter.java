package com.qtsurfer.qtstreamx.link;

import java.util.Locale;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import javax.net.ssl.SSLHandshakeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-IP, per-exchange connection-rate gate for WebSocket opens.
 *
 * <p>A node has exactly ONE outbound IP, so every actual socket open to a given exchange — initial
 * subscribe, reconnect, AND preemptive renewal — must pass through a single shared instance of this
 * limiter. It exists to stop a node from self-inflicting an exchange per-IP connection-rate ban: a
 * reconnect storm (many links dropping together and retrying in lockstep) is QUEUED and trickled out
 * at a rate the exchange tolerates rather than fired all at once.
 *
 * <h2>Token bucket</h2>
 *
 * <p>A classic token bucket sized from {@link WsLimits#maxConnectionsPer5MinPerIp()} over a 5-minute
 * window. Tokens refill continuously at {@code maxConn * SAFETY_FRACTION / 5min}; a small {@code
 * burst} capacity absorbs a handful of legitimate concurrent reconnects without delay, and anything
 * beyond that is DELAYED — never dropped. Losing a connection would violate the 100%-capture prime
 * directive, so {@link #acquire} only ever waits; it cannot reject. A permit is consumed per open
 * <em>attempt</em>, including a failed TLS handshake — the exchange counts those too.
 *
 * <h2>Adaptive backpressure</h2>
 *
 * <p>When connection attempts start failing with a throttling signal (TLS handshake terminations,
 * explicit 429/418/-1003 — see {@link #isBackpressureSignal}), the exchange is actively rate-limiting
 * this IP. {@link #onFailure()} raises a bounded penalty level; once it crosses {@link
 * #PENALTY_THRESHOLD} an extra floor delay is added to every {@link #acquire}, ramping the node's
 * connection attempts DOWN harder than the steady-state token rate. {@link #onSuccess()} decays the
 * penalty multiplicatively, so once the exchange stops terminating handshakes the node ramps back UP
 * to full rate on its own. The penalty is bounded ({@link #PENALTY_MAX}) and self-healing (decays to
 * exactly zero), so it can never wedge the node permanently.
 *
 * <p>All mutable state is guarded by {@code this}; the class is safe under concurrent reconnects.
 */
public final class ConnectionRateLimiter {

  private static final Logger log = LoggerFactory.getLogger(ConnectionRateLimiter.class);

  /** Rolling window the exchange's connection cap is expressed over. */
  static final long WINDOW_MS = 5 * 60 * 1000L;

  /**
   * Fraction of the exchange's published per-window cap we actually aim for. A token bucket can emit
   * up to {@code burst + refillRate * window} in a window, so refilling at {@code maxConn / window}
   * with a non-zero burst could momentarily exceed the cap; staying at a fraction leaves headroom so
   * the node never reaches the exact limit the exchange bans at.
   */
  static final double SAFETY_FRACTION = 0.9;

  /** Divisor used to derive a small default burst from the per-window cap. */
  static final int BURST_DIVISOR = 60;

  /** Poll slice while waiting, so a closing link aborts promptly instead of hanging. */
  static final long ABORT_POLL_MS = 200L;

  // ---- adaptive backpressure tuning -----------------------------------------
  /** Penalty added per backpressure signal. */
  static final double PENALTY_FAILURE_STEP = 1.0;
  /** Hard cap on the penalty level — bounds the maximum extra delay. */
  static final double PENALTY_MAX = 8.0;
  /** Penalty below which no extra delay is applied (steady-state token rate only). */
  static final double PENALTY_THRESHOLD = 3.0;
  /** Multiplicative decay applied to the penalty on every successful connect. */
  static final double PENALTY_DECAY = 0.5;
  /** Penalty snaps to exactly zero below this, guaranteeing full self-heal. */
  static final double PENALTY_HEAL_FLOOR = 0.25;
  /** Milliseconds of extra floor delay per unit of penalty over the threshold. */
  static final long PENALTY_STEP_MS = 2000L;

  private final String exchangeKey;
  private final double capacity;
  private final double refillPerMs;
  private final LongSupplier clockMs;
  private final boolean unlimited;

  // Guarded by this.
  private double tokens;
  private long lastRefillMs;
  private double penalty;

  ConnectionRateLimiter(
      String exchangeKey, double burstCapacity, double refillPerMs, LongSupplier clockMs) {
    this(exchangeKey, burstCapacity, refillPerMs, clockMs, false);
  }

  private ConnectionRateLimiter(
      String exchangeKey,
      double burstCapacity,
      double refillPerMs,
      LongSupplier clockMs,
      boolean unlimited) {
    this.exchangeKey = Objects.requireNonNull(exchangeKey, "exchangeKey");
    this.clockMs = Objects.requireNonNull(clockMs, "clockMs");
    if (!unlimited) {
      if (burstCapacity < 1.0) {
        throw new IllegalArgumentException("burstCapacity must be >= 1");
      }
      if (refillPerMs <= 0.0) {
        throw new IllegalArgumentException("refillPerMs must be > 0");
      }
    }
    this.capacity = burstCapacity;
    this.refillPerMs = refillPerMs;
    this.unlimited = unlimited;
    this.tokens = burstCapacity;
    this.lastRefillMs = clockMs.getAsLong();
  }

  /** A no-op limiter that never delays — used as the default where no gate is wired. */
  public static ConnectionRateLimiter unlimited() {
    return new ConnectionRateLimiter("unlimited", 1.0, 1.0, System::currentTimeMillis, true);
  }

  /**
   * Build a limiter sized from an exchange's published per-IP connection cap, using the wall clock.
   * Refill is the cap (scaled by {@link #SAFETY_FRACTION}) spread over the 5-minute window; the
   * burst is a small fraction of the cap so a node cold-starting many links trickles them out at the
   * safe rate instead of opening all at once.
   */
  public static ConnectionRateLimiter forExchange(String exchangeKey, WsLimits limits) {
    return forExchange(exchangeKey, limits, System::currentTimeMillis);
  }

  static ConnectionRateLimiter forExchange(String exchangeKey, WsLimits limits, LongSupplier clockMs) {
    int maxConn = limits.maxConnectionsPer5MinPerIp();
    double refillPerMs = (maxConn * SAFETY_FRACTION) / WINDOW_MS;
    double burst = Math.max(1.0, (double) (maxConn / BURST_DIVISOR));
    log.info(
        "ConnectionRateLimiter[{}] burst={} refill={}/s (cap {}/5min, safety {})",
        exchangeKey,
        (long) burst,
        String.format(Locale.ROOT, "%.3f", refillPerMs * 1000.0),
        maxConn,
        SAFETY_FRACTION);
    return new ConnectionRateLimiter(exchangeKey, burst, refillPerMs, clockMs);
  }

  public String exchangeKey() {
    return exchangeKey;
  }

  /**
   * Block until a permit is available, then return. Consumes exactly one permit per call. Waits in
   * short slices so that a link that is shutting down (its {@code abort} supplier flips to {@code
   * true}) bails out promptly with an {@link InterruptedException} instead of hanging. NEVER drops:
   * the only non-return is abort/interrupt during shutdown.
   *
   * @param abort polled while waiting; when it returns {@code true} the wait is abandoned. May be
   *     {@code null}.
   */
  public void acquire(BooleanSupplier abort) throws InterruptedException {
    if (unlimited) return;
    long waitMs = reserveDelayMillis();
    long remaining = waitMs;
    while (remaining > 0) {
      if (abort != null && abort.getAsBoolean()) {
        throw new InterruptedException("acquire aborted for " + exchangeKey + " (link closing)");
      }
      long slice = Math.min(remaining, ABORT_POLL_MS);
      Thread.sleep(slice);
      remaining -= slice;
    }
  }

  /**
   * Reserve one permit and return how many milliseconds the caller must wait before using it. Zero
   * means a permit was immediately available. The permit is consumed atomically here (the bucket may
   * go transiently negative to reserve a future slot), so concurrent callers serialize correctly and
   * each gets a distinct, monotonically later slot. Package-visible for deterministic unit tests.
   */
  synchronized long reserveDelayMillis() {
    if (unlimited) return 0L;
    refill(clockMs.getAsLong());
    long tokenWaitMs;
    if (tokens >= 1.0) {
      tokenWaitMs = 0L;
    } else {
      double deficit = 1.0 - tokens; // > 0
      tokenWaitMs = (long) Math.ceil(deficit / refillPerMs);
    }
    tokens -= 1.0; // reserve (may go negative; refill clamps only on the positive side)
    return tokenWaitMs + adaptiveFloorMillisLocked();
  }

  /** Signal that a connection attempt failed with an exchange throttling signal. */
  public synchronized void onFailure() {
    if (unlimited) return;
    penalty = Math.min(PENALTY_MAX, penalty + PENALTY_FAILURE_STEP);
  }

  /** Signal a successful connect — decays the adaptive penalty back towards zero. */
  public synchronized void onSuccess() {
    if (unlimited || penalty == 0.0) return;
    penalty *= PENALTY_DECAY;
    if (penalty < PENALTY_HEAL_FLOOR) {
      penalty = 0.0;
    }
  }

  /**
   * True if a throwable looks like the exchange actively throttling this IP (TLS handshake
   * termination at the edge — no HTTP status — or an explicit rate-limit code). Such failures drive
   * the adaptive backpressure; ordinary network errors do not.
   */
  public static boolean isBackpressureSignal(Throwable e) {
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (t instanceof SSLHandshakeException) {
        return true;
      }
      String m = t.getMessage();
      if (m != null) {
        String lm = m.toLowerCase(Locale.ROOT);
        if (lm.contains("terminated the handshake")
            || lm.contains("too many")
            || lm.contains("429")
            || lm.contains("418")
            || lm.contains("-1003")) {
          return true;
        }
      }
      if (t == t.getCause()) break; // guard against self-referential cause chains
    }
    return false;
  }

  // ---- internals ------------------------------------------------------------

  private void refill(long now) {
    long elapsed = now - lastRefillMs;
    if (elapsed <= 0) return;
    lastRefillMs = now;
    tokens = Math.min(capacity, tokens + elapsed * refillPerMs);
  }

  private long adaptiveFloorMillisLocked() {
    if (penalty < PENALTY_THRESHOLD) return 0L;
    double over = penalty - PENALTY_THRESHOLD + 1.0; // >= 1 at the threshold
    return (long) (over * PENALTY_STEP_MS);
  }

  // ---- test hooks -----------------------------------------------------------

  synchronized double penaltyLevel() {
    return penalty;
  }

  synchronized long adaptiveFloorMillis() {
    return adaptiveFloorMillisLocked();
  }

  synchronized double availableTokens() {
    refill(clockMs.getAsLong());
    return tokens;
  }
}
