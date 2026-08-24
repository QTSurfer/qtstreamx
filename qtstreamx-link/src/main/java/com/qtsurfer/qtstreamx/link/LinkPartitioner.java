package com.qtsurfer.qtstreamx.link;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Splits an instrument set across one or more {@link Link}s respecting the
 * exchange's {@link WsLimits}.
 *
 * <p>Baseline algorithm: pack instruments into buckets of at most
 * {@code streamsPerLink = min(targetStreamsPerLink, wsLimits.maxStreamsPerConnection())}
 * streams. Each instrument consumes a known number of streams determined by the
 * caller (e.g. Binance spot kline = 1 stream per instrument, Binance spot
 * bookTicker + kline = 2 streams per instrument).
 *
 * <p>Partitioning is stable across calls for a given partitioner instance, so reconnects
 * don't reshuffle subscriptions. By default (null {@code shuffleSeed}) instruments in the
 * same input order land in the same bucket — fully deterministic. When a {@code shuffleSeed}
 * is supplied the instrument set is shuffled with that seed before bucketing: still stable
 * within this instance (same seed → same shuffle every call), but DIFFERENT across instances
 * given different seeds. When several redundant publisher instances each seed from a per-host
 * value, a WS link drop affects a different instrument subset per instance, so a simultaneous
 * gap on the SAME instrument across all instances drops from ~p (correlated, identical
 * grouping) towards ~p^N (independent). Grouping is independent of any content-based message
 * de-duplication downstream.
 */
public final class LinkPartitioner {

  private final int streamsPerLink;
  private final int streamsPerInstrument;
  private final Long shuffleSeed;

  /** Deterministic partitioner (no shuffle) — equivalent to {@code shuffleSeed = null}. */
  public LinkPartitioner(WsLimits wsLimits, int targetStreamsPerLink, int streamsPerInstrument) {
    this(wsLimits, targetStreamsPerLink, streamsPerInstrument, null);
  }

  /**
   * @param wsLimits exchange limits (used as upper bound)
   * @param targetStreamsPerLink operator-tunable target; capped to {@code
   *     wsLimits.maxStreamsPerConnection()}. Passing {@link Integer#MAX_VALUE} uses the full
   *     limit.
   * @param streamsPerInstrument how many streams each instrument requires on this exchange
   *     for the current subscription mix (e.g. 1 for kline only, 2 for ticker + kline)
   * @param shuffleSeed when non-null, the instrument set is shuffled with this seed before
   *     bucketing. Stable per instance (idempotent reconnects), decorrelated across instances.
   *     {@code null} = deterministic input-order bucketing (legacy behaviour).
   */
  public LinkPartitioner(
      WsLimits wsLimits, int targetStreamsPerLink, int streamsPerInstrument, Long shuffleSeed) {
    if (streamsPerInstrument <= 0) {
      throw new IllegalArgumentException("streamsPerInstrument must be > 0");
    }
    int cap = wsLimits.maxStreamsPerConnection();
    // Reserve one slot per link for control traffic (ping/pong, SUBSCRIBE).
    int effectiveCap = Math.max(1, cap - 1);
    this.streamsPerLink = Math.max(1, Math.min(targetStreamsPerLink, effectiveCap));
    this.streamsPerInstrument = streamsPerInstrument;
    this.shuffleSeed = shuffleSeed;
  }

  /** Instruments a single link can accept. */
  public int instrumentsPerLink() {
    return Math.max(1, streamsPerLink / streamsPerInstrument);
  }

  /**
   * Partition an instrument set into buckets. Stable across calls for this partitioner
   * instance (same input + same {@code shuffleSeed} → same buckets), making reconnections
   * idempotent in terms of which link owns which instrument. With a non-null seed the input
   * is shuffled first, so two instances with different seeds bucket the same universe
   * differently.
   */
  public List<Set<Instrument>> partition(Collection<Instrument> instruments) {
    if (instruments.isEmpty()) return Collections.emptyList();
    int per = instrumentsPerLink();
    // Deterministic input order, or a seeded shuffle to decorrelate grouping per instance.
    List<Instrument> ordered = new ArrayList<>(instruments);
    if (shuffleSeed != null) {
      Collections.shuffle(ordered, new Random(shuffleSeed));
    }
    List<Set<Instrument>> buckets = new ArrayList<>();
    Set<Instrument> current = new LinkedHashSet<>(per);
    for (Instrument ins : ordered) {
      if (current.size() >= per) {
        buckets.add(current);
        current = new LinkedHashSet<>(per);
      }
      current.add(ins);
    }
    if (!current.isEmpty()) buckets.add(current);
    return buckets;
  }

  /**
   * Find an existing link with room for a new instrument. Returns {@code -1} if every
   * link is full and the caller must spawn a new one.
   */
  public int assign(List<Link> existing) {
    int per = instrumentsPerLink();
    for (int i = 0; i < existing.size(); i++) {
      if (existing.get(i).subscribedCount() < per) {
        return i;
      }
    }
    return -1;
  }
}
