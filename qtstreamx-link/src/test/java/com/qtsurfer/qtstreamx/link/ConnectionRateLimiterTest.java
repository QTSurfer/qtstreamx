package com.qtsurfer.qtstreamx.link;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.SSLHandshakeException;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ConnectionRateLimiter}: the token bucket trickles a burst at the safe rate
 * without ever dropping a permit, is thread-safe under concurrent reconnects, and the adaptive
 * backpressure ramps up on a wave of throttling failures then self-heals on success.
 *
 * <p>Time is driven by a virtual clock (an {@link AtomicLong}) so the bucket assertions are exact
 * and not timing-flaky. One small real-time test exercises the blocking {@link
 * ConnectionRateLimiter#acquire} path with generous tolerance.
 */
class ConnectionRateLimiterTest {

  // ---- token bucket: burst trickles, nothing is dropped ---------------------

  @Test
  void burst_beyond_capacity_is_delayed_never_dropped() {
    AtomicLong now = new AtomicLong(0);
    // capacity 5 tokens, refill 1 token / 1000ms.
    var limiter = new ConnectionRateLimiter("test", 5.0, 1.0 / 1000.0, now::get);

    int m = 20; // burst far larger than capacity
    long[] waits = new long[m];
    for (int i = 0; i < m; i++) {
      waits[i] = limiter.reserveDelayMillis();
    }

    // The first `capacity` permits are immediate; everything after is DELAYED (never rejected).
    int immediate = 0;
    for (long w : waits) {
      if (w == 0) immediate++;
    }
    assertThat(immediate).isEqualTo(5);

    // Delays are strictly increasing for the queued excess and bounded by the refill rate:
    // permit k (k >= capacity) waits ~ (k - capacity + 1) * 1000ms.
    for (int i = 5; i < m; i++) {
      long expected = (long) (i - 5 + 1) * 1000L;
      assertThat(waits[i]).isEqualTo(expected);
      assertThat(waits[i]).isGreaterThan(waits[i - 1]);
    }

    // All M permits were issued (none dropped) and the bucket reserved exactly M.
    // After advancing the clock past the whole drain, the bucket refills back to capacity.
    now.addAndGet(10 * 60 * 1000L);
    assertThat(limiter.availableTokens()).isEqualTo(5.0);
  }

  @Test
  void steady_state_rate_matches_exchange_cap() {
    // Binance spot: 300 conn / 5min. forExchange refills at 300*SAFETY/300000 = 0.0009 tokens/ms
    // ⇒ the first permit beyond the burst waits ~1/0.0009 ≈ 1112ms. Frozen clock for determinism.
    AtomicLong now = new AtomicLong(0);
    var limiter = ConnectionRateLimiter.forExchange("binance-spot", WsLimits.binanceSpot(), now::get);

    // Drain the small burst (5 for cap 300) until the first queued permit appears.
    long firstQueuedWait = -1;
    for (int i = 0; i < 50 && firstQueuedWait <= 0; i++) {
      firstQueuedWait = limiter.reserveDelayMillis();
    }
    // ~1.1s spacing per permit once the burst is exhausted: a single node cannot exceed the cap.
    assertThat(firstQueuedWait).isBetween(1000L, 1300L);
  }

  // ---- thread-safety: concurrent reservations lose nothing ------------------

  @Test
  void concurrent_reservations_are_thread_safe() throws Exception {
    AtomicLong now = new AtomicLong(0);
    double capacity = 10.0;
    var limiter = new ConnectionRateLimiter("test", capacity, 1.0 / 1000.0, now::get);

    int threads = 16;
    int perThread = 25;
    int total = threads * perThread; // 400 concurrent reservations, clock frozen
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CyclicBarrier start = new CyclicBarrier(threads);
    CountDownLatch done = new CountDownLatch(threads);
    AtomicInteger immediate = new AtomicInteger();
    ConcurrentLinkedQueue<Long> all = new ConcurrentLinkedQueue<>();

    for (int t = 0; t < threads; t++) {
      pool.submit(
          () -> {
            try {
              start.await();
              for (int i = 0; i < perThread; i++) {
                long w = limiter.reserveDelayMillis();
                all.add(w);
                if (w == 0) immediate.incrementAndGet();
              }
            } catch (Exception e) {
              throw new RuntimeException(e);
            } finally {
              done.countDown();
            }
          });
    }
    assertThat(done.await(20, TimeUnit.SECONDS)).isTrue();
    pool.shutdownNow();

    // Exactly `total` reservations recorded — no lost updates.
    assertThat(all).hasSize(total);
    // No more than `capacity` permits were granted immediately (the per-window burst budget).
    assertThat(immediate.get()).isEqualTo((int) capacity);
    // The bucket reserved exactly `total` tokens against the frozen clock (capacity - total),
    // proving no decrement was lost to a race.
    assertThat(limiter.availableTokens()).isEqualTo(capacity - total);
  }

  // ---- adaptive backpressure: ramp up on failures, self-heal on success -----

  @Test
  void adaptive_backpressure_ramps_up_then_self_heals() {
    AtomicLong now = new AtomicLong(0);
    var limiter = new ConnectionRateLimiter("test", 100.0, 1.0, now::get); // huge burst: isolate adaptive

    // Baseline: tokens available, no penalty ⇒ zero extra floor delay.
    assertThat(limiter.adaptiveFloorMillis()).isZero();
    assertThat(limiter.reserveDelayMillis()).isZero();

    // A wave of handshake-termination failures pushes the penalty up monotonically.
    long prevFloor = 0;
    for (int i = 0; i < 12; i++) {
      limiter.onFailure();
      long floor = limiter.adaptiveFloorMillis();
      assertThat(floor).isGreaterThanOrEqualTo(prevFloor);
      prevFloor = floor;
    }
    // Crossed the threshold ⇒ a real, bounded extra delay is now applied to every acquire,
    // even though burst tokens remain (ramp DOWN harder than the steady rate).
    long throttledFloor = limiter.adaptiveFloorMillis();
    assertThat(throttledFloor).isGreaterThan(0L);
    assertThat(limiter.reserveDelayMillis()).isGreaterThanOrEqualTo(throttledFloor);
    // Penalty is bounded — extra delay cannot grow without limit.
    assertThat(limiter.penaltyLevel()).isLessThanOrEqualTo(ConnectionRateLimiter.PENALTY_MAX);
    long maxFloor =
        (long)
            ((ConnectionRateLimiter.PENALTY_MAX - ConnectionRateLimiter.PENALTY_THRESHOLD + 1.0)
                * ConnectionRateLimiter.PENALTY_STEP_MS);
    assertThat(throttledFloor).isLessThanOrEqualTo(maxFloor);

    // Successful connects decay the penalty back to EXACTLY zero — fully self-healing.
    for (int i = 0; i < 20; i++) {
      limiter.onSuccess();
    }
    assertThat(limiter.penaltyLevel()).isZero();
    assertThat(limiter.adaptiveFloorMillis()).isZero();
  }

  // ---- backpressure signal classification -----------------------------------

  @Test
  void recognises_handshake_termination_and_rate_codes_as_backpressure() {
    assertThat(ConnectionRateLimiter.isBackpressureSignal(new SSLHandshakeException("Remote host terminated the handshake")))
        .isTrue();
    assertThat(ConnectionRateLimiter.isBackpressureSignal(
            new RuntimeException(new SSLHandshakeException("Remote host terminated the handshake"))))
        .isTrue();
    assertThat(ConnectionRateLimiter.isBackpressureSignal(new RuntimeException("HTTP 429 Too Many Requests")))
        .isTrue();
    assertThat(ConnectionRateLimiter.isBackpressureSignal(new RuntimeException("error -1003 too many requests")))
        .isTrue();
    // Ordinary errors must NOT trigger backpressure.
    assertThat(ConnectionRateLimiter.isBackpressureSignal(new RuntimeException("connection reset")))
        .isFalse();
    assertThat(ConnectionRateLimiter.isBackpressureSignal(new java.net.ConnectException("refused")))
        .isFalse();
  }

  // ---- blocking acquire: real-time, generous tolerance ----------------------

  @Test
  void blocking_acquire_trickles_a_burst_and_drops_nothing() throws Exception {
    // capacity 1, refill 20 tokens/s (50ms each). 6 acquires ⇒ ~5*50 = 250ms minimum.
    var limiter = new ConnectionRateLimiter("test", 1.0, 20.0 / 1000.0, System::currentTimeMillis);
    int m = 6;
    AtomicInteger completed = new AtomicInteger();
    ExecutorService pool = Executors.newFixedThreadPool(m);
    CyclicBarrier barrier = new CyclicBarrier(m);
    long startNs = System.nanoTime();
    for (int i = 0; i < m; i++) {
      pool.submit(
          () -> {
            try {
              barrier.await();
              limiter.acquire(() -> false);
              completed.incrementAndGet();
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          });
    }
    pool.shutdown();
    assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    long elapsedMs = (System.nanoTime() - startNs) / 1_000_000L;

    // All M eventually acquired — NONE dropped.
    assertThat(completed.get()).isEqualTo(m);
    // The excess was genuinely delayed (trickled), not all granted at once.
    assertThat(elapsedMs).isGreaterThanOrEqualTo(150L);
  }

  @Test
  void acquire_aborts_promptly_when_link_closing() throws Exception {
    // Long per-permit wait; capacity 1 so the 2nd acquire must wait ~10s — but abort flips fast.
    var limiter = new ConnectionRateLimiter("test", 1.0, 1.0 / 10_000.0, System::currentTimeMillis);
    limiter.reserveDelayMillis(); // drain the single burst token
    AtomicInteger interrupted = new AtomicInteger();
    Thread th =
        new Thread(
            () -> {
              try {
                limiter.acquire(() -> true); // abort signal already true ⇒ must bail fast
              } catch (InterruptedException e) {
                interrupted.incrementAndGet();
              }
            });
    long startNs = System.nanoTime();
    th.start();
    th.join(2000);
    long elapsedMs = (System.nanoTime() - startNs) / 1_000_000L;
    assertThat(th.isAlive()).isFalse();
    assertThat(interrupted.get()).isEqualTo(1);
    assertThat(elapsedMs).isLessThan(1000L);
  }
}
