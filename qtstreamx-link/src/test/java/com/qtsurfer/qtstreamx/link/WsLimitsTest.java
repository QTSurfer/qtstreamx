package com.qtsurfer.qtstreamx.link;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class WsLimitsTest {

  @Test
  void renewBefore_equals_lifetime_minus_safety_margin() {
    WsLimits w = WsLimits.binanceSpot();
    assertThat(w.renewBefore()).isEqualTo(Duration.ofHours(24).minus(Duration.ofMinutes(10)));
  }

  @Test
  void presets_have_expected_shape() {
    assertThat(WsLimits.binanceSpot().maxStreamsPerConnection()).isEqualTo(1024);
    assertThat(WsLimits.binanceFutures().maxStreamsPerConnection()).isEqualTo(200);
    assertThat(WsLimits.bybit().maxStreamsPerConnection()).isEqualTo(500);
    assertThat(WsLimits.okx().maxStreamsPerConnection()).isEqualTo(256);
  }

  @Test
  void rejects_non_positive_maxStreams() {
    assertThatThrownBy(
            () ->
                new WsLimits(
                    300, 0, 5, Duration.ofHours(24), Duration.ofMinutes(10)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejects_safety_margin_geq_lifetime() {
    assertThatThrownBy(
            () -> new WsLimits(300, 1024, 5, Duration.ofMinutes(5), Duration.ofMinutes(10)))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
