package com.qtsurfer.qtstreamx.link;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class LinkPartitionerTest {

  private static List<Instrument> instruments(int count) {
    return IntStream.range(0, count)
        .mapToObj(i -> new Instrument("A" + i, "USDT"))
        .toList();
  }

  @Test
  void capacity_respects_streams_per_link_and_reserves_one_slot() {
    // wsLimits 1024 streams/conn → cap 1023 (1 slot reserved for control traffic)
    LinkPartitioner p =
        new LinkPartitioner(WsLimits.binanceSpot(), Integer.MAX_VALUE, /*streamsPerInstrument*/ 1);
    assertThat(p.instrumentsPerLink()).isEqualTo(1023);
  }

  @Test
  void capacity_respects_operator_target_below_cap() {
    LinkPartitioner p = new LinkPartitioner(WsLimits.binanceSpot(), 100, 1);
    assertThat(p.instrumentsPerLink()).isEqualTo(100);
  }

  @Test
  void capacity_halves_for_ticker_plus_kline_streams() {
    LinkPartitioner p =
        new LinkPartitioner(WsLimits.binanceSpot(), Integer.MAX_VALUE, /*streamsPerInstrument*/ 2);
    // 1023 streams / 2 streams-per-instrument = 511 instruments
    assertThat(p.instrumentsPerLink()).isEqualTo(511);
  }

  @Test
  void partition_splits_into_expected_number_of_buckets() {
    LinkPartitioner p = new LinkPartitioner(WsLimits.binanceSpot(), 100, 1);
    List<Set<Instrument>> buckets = p.partition(instruments(1400));
    assertThat(buckets).hasSize(14);
    assertThat(buckets).allMatch(b -> b.size() <= 100);
    // last bucket contains remainder
    int total = buckets.stream().mapToInt(Set::size).sum();
    assertThat(total).isEqualTo(1400);
  }

  @Test
  void partition_is_deterministic_across_calls() {
    LinkPartitioner p = new LinkPartitioner(WsLimits.binanceSpot(), 50, 1);
    List<Set<Instrument>> a = p.partition(instruments(250));
    List<Set<Instrument>> b = p.partition(instruments(250));
    assertThat(a).isEqualTo(b);
  }

  @Test
  void assign_finds_link_with_capacity() {
    LinkPartitioner p = new LinkPartitioner(WsLimits.binanceSpot(), 10, 1);
    // Link doubles as a simple stub — we don't need real connect for this unit
    List<Link> links = new ArrayList<>();
    // Two stubbed links: first full (10), second has 3 items
    links.add(stubLink(10));
    links.add(stubLink(3));
    int idx = p.assign(links);
    assertThat(idx).isEqualTo(1);
  }

  @Test
  void assign_returns_minus_one_when_all_full() {
    LinkPartitioner p = new LinkPartitioner(WsLimits.binanceSpot(), 10, 1);
    List<Link> links = new ArrayList<>();
    links.add(stubLink(10));
    links.add(stubLink(10));
    assertThat(p.assign(links)).isEqualTo(-1);
  }

  @Test
  void rejects_zero_streams_per_instrument() {
    assertThatThrownBy(() -> new LinkPartitioner(WsLimits.binanceSpot(), 100, 0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // --------------------------------------------------------------------------
  // Seeded shuffle: decorrelate link grouping across redundant publisher instances
  // --------------------------------------------------------------------------

  @Test
  void null_seed_keeps_deterministic_input_order() {
    LinkPartitioner det = new LinkPartitioner(WsLimits.binanceSpot(), 50, 1, /*shuffleSeed*/ null);
    List<Set<Instrument>> buckets = det.partition(instruments(250));
    // First bucket = the first 50 instruments in input order (A0..A49) — legacy behaviour intact.
    Set<Instrument> first = buckets.get(0);
    assertThat(first).contains(new Instrument("A0", "USDT"), new Instrument("A49", "USDT"));
    assertThat(first).doesNotContain(new Instrument("A50", "USDT"));
  }

  @Test
  void seeded_partition_is_stable_across_calls() {
    // Same instance (same seed) → identical buckets every call, so reconnects stay idempotent.
    LinkPartitioner p = new LinkPartitioner(WsLimits.binanceSpot(), 50, 1, 12345L);
    assertThat(p.partition(instruments(250))).isEqualTo(p.partition(instruments(250)));
  }

  @Test
  void different_seeds_produce_different_grouping() {
    List<Instrument> ins = instruments(250);
    List<Set<Instrument>> a = new LinkPartitioner(WsLimits.binanceSpot(), 50, 1, 1L).partition(ins);
    List<Set<Instrument>> b = new LinkPartitioner(WsLimits.binanceSpot(), 50, 1, 2L).partition(ins);
    // Same universe + same sharding, but different instruments share a WS link → decorrelated.
    assertThat(a).isNotEqualTo(b);
  }

  @Test
  void seeded_grouping_differs_from_deterministic() {
    List<Instrument> ins = instruments(250);
    List<Set<Instrument>> det =
        new LinkPartitioner(WsLimits.binanceSpot(), 50, 1, null).partition(ins);
    List<Set<Instrument>> shuffled =
        new LinkPartitioner(WsLimits.binanceSpot(), 50, 1, 999L).partition(ins);
    assertThat(shuffled).isNotEqualTo(det);
  }

  @Test
  void shuffle_loses_no_instruments_and_preserves_sharding() {
    List<Instrument> ins = instruments(250);
    List<Set<Instrument>> buckets =
        new LinkPartitioner(WsLimits.binanceSpot(), 50, 1, 42L).partition(ins);
    // Same bucket count + sizes as deterministic (50/link → 5 full buckets) ...
    assertThat(buckets).hasSize(5);
    assertThat(buckets).allMatch(b -> b.size() == 50);
    // ... and the union is exactly the input universe — nothing dropped or duplicated (no loss).
    Set<Instrument> union = new java.util.HashSet<>();
    buckets.forEach(union::addAll);
    assertThat(union).hasSize(250).containsAll(ins);
  }

  // --------------------------------------------------------------------------
  // Helpers
  // --------------------------------------------------------------------------

  /** Minimal Link stub using a no-op configuration — only {@link Link#subscribedCount} matters. */
  private static Link stubLink(int subscribed) {
    Link.Configuration cfg =
        new Link.Configuration(
            "stub",
            WsLimits.binanceSpot(),
            Set.of(Link.Subscription.KLINE),
            "1s",
            () -> {
              throw new UnsupportedOperationException();
            },
            t -> {},
            k -> {},
            f -> {},
            java.util.concurrent.Executors.newSingleThreadScheduledExecutor());
    Link l = new Link(cfg);
    // Seed by re-using the public addInstrument path with a dummy set.
    for (int i = 0; i < subscribed; i++) {
      try {
        l.addInstrument(new Instrument("X" + i, "USDT"));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return l;
  }
}
