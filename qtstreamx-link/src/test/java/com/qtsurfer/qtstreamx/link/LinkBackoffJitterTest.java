package com.qtsurfer.qtstreamx.link;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link Link#backoffMillis(int, double)} — the jittered exponential backoff that
 * de-synchronises many links failing together (the lockstep that breached Binance's per-IP
 * connection-rate limit on sat2).
 */
class LinkBackoffJitterTest {

  @Test
  void delays_for_one_attempt_are_spread_not_constant() {
    Set<Long> seen = new HashSet<>();
    for (int i = 0; i < 1000; i++) {
      double r = i / 1000.0;
      seen.add(Link.backoffMillis(3, r));
    }
    // Equal jitter over a 4s base must produce a wide spread of distinct delays, never one value.
    assertThat(seen).hasSizeGreaterThan(100);
  }

  @Test
  void delay_is_bounded_by_base_window_for_every_jitter() {
    // attempt 3 → base = 1000 << 3 = 8000ms → delay in [4000, 8000].
    for (int i = 0; i <= 1000; i++) {
      double r = Math.min(0.999999, i / 1000.0);
      long d = Link.backoffMillis(3, r);
      assertThat(d).isBetween(4000L, 8000L);
    }
  }

  @Test
  void delay_never_exceeds_60s_cap_even_for_huge_attempts() {
    for (int attempt = 0; attempt < 40; attempt++) {
      assertThat(Link.backoffMillis(attempt, 0.999999)).isLessThanOrEqualTo(60_000L);
      assertThat(Link.backoffMillis(attempt, 0.0)).isGreaterThanOrEqualTo(500L);
    }
  }

  @Test
  void base_grows_with_attempt_until_capped() {
    // Min delay (rand=0 → base/2) must be non-decreasing and grow over the early attempts.
    long prev = -1;
    for (int attempt = 0; attempt <= 6; attempt++) {
      long min = Link.backoffMillis(attempt, 0.0);
      assertThat(min).isGreaterThan(prev);
      prev = min;
    }
    // attempt 6 → base 64000 capped to 60000 → min 30000; attempt 7+ stays capped.
    assertThat(Link.backoffMillis(6, 0.0)).isEqualTo(30_000L);
    assertThat(Link.backoffMillis(10, 0.0)).isEqualTo(30_000L);
  }
}
