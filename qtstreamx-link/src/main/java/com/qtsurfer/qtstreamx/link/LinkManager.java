package com.qtsurfer.qtstreamx.link;

import com.qtsurfer.qtstreamx.core.client.StreamClient;
import com.qtsurfer.qtstreamx.core.model.FundingRate;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.model.Kline;
import com.qtsurfer.qtstreamx.core.model.Ticker;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages a pool of {@link Link}s for one exchange + market.
 *
 * <p>Orchestrates:
 *
 * <ul>
 *   <li>Initial discovery via {@link InstrumentsCache} and partitioning via
 *       {@link LinkPartitioner}.
 *   <li>Connection of each {@link Link} with a small inter-link startup jitter to
 *       stay under the exchange's new-connection budget.
 *   <li>Periodic {@link InstrumentsCache#refresh()} that diffs against the current set
 *       and calls {@link Link#addInstrument} / {@link Link#removeInstrument} accordingly.
 *   <li>Fan-out of ticker / kline / funding-rate emissions into a single consumer per type
 *       that the outer publisher can wire to NATS (or any sink).
 * </ul>
 */
public final class LinkManager implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(LinkManager.class);

  /** Immutable configuration. */
  public record Configuration(
      String exchangeKey,
      WsLimits wsLimits,
      Set<Link.Subscription> subscriptions,
      String klineInterval,
      int targetStreamsPerLink,
      int streamsPerInstrument,
      Duration discoveryRefreshInterval,
      Duration interLinkStartupJitter,
      Function<Set<Instrument>, StreamClient> streamClientFactory,
      LinkLifecycleListener lifecycleListener,
      boolean randomizeLinkGrouping,
      Duration linkSilenceThreshold) {

    public Configuration {
      Objects.requireNonNull(exchangeKey);
      Objects.requireNonNull(wsLimits);
      subscriptions = Set.copyOf(subscriptions);
      if (targetStreamsPerLink <= 0) targetStreamsPerLink = wsLimits.maxStreamsPerConnection() - 1;
      if (streamsPerInstrument <= 0) streamsPerInstrument = 1;
      if (discoveryRefreshInterval == null) discoveryRefreshInterval = Duration.ofMinutes(5);
      if (interLinkStartupJitter == null) interLinkStartupJitter = Duration.ofMillis(250);
      if (lifecycleListener == null) lifecycleListener = LinkLifecycleListener.NOOP;
      // Null = the default; ZERO = watchdog disabled. Two minutes is chosen against the slowest
      // subscription a link normally carries alongside tickers, not against funding rates on their
      // own — a link holding ONLY funding rates is legitimately quiet for longer than this, and
      // such a deployment should pass an explicit larger value.
      if (linkSilenceThreshold == null) linkSilenceThreshold = Duration.ofMinutes(2);
    }

    /**
     * Backwards-compatible constructor — the shape that was canonical before
     * {@code linkSilenceThreshold} was added. Defaults it, so existing callers compile unchanged.
     */
    public Configuration(
        String exchangeKey,
        WsLimits wsLimits,
        Set<Link.Subscription> subscriptions,
        String klineInterval,
        int targetStreamsPerLink,
        int streamsPerInstrument,
        Duration discoveryRefreshInterval,
        Duration interLinkStartupJitter,
        Function<Set<Instrument>, StreamClient> streamClientFactory,
        LinkLifecycleListener lifecycleListener,
        boolean randomizeLinkGrouping) {
      this(
          exchangeKey,
          wsLimits,
          subscriptions,
          klineInterval,
          targetStreamsPerLink,
          streamsPerInstrument,
          discoveryRefreshInterval,
          interLinkStartupJitter,
          streamClientFactory,
          lifecycleListener,
          randomizeLinkGrouping,
          /*linkSilenceThreshold*/ null);
    }

    /** Backwards-compatible constructor — listener supplied, link grouping left deterministic. */
    public Configuration(
        String exchangeKey,
        WsLimits wsLimits,
        Set<Link.Subscription> subscriptions,
        String klineInterval,
        int targetStreamsPerLink,
        int streamsPerInstrument,
        Duration discoveryRefreshInterval,
        Duration interLinkStartupJitter,
        Function<Set<Instrument>, StreamClient> streamClientFactory,
        LinkLifecycleListener lifecycleListener) {
      this(
          exchangeKey,
          wsLimits,
          subscriptions,
          klineInterval,
          targetStreamsPerLink,
          streamsPerInstrument,
          discoveryRefreshInterval,
          interLinkStartupJitter,
          streamClientFactory,
          lifecycleListener,
          /*randomizeLinkGrouping*/ false,
          /*linkSilenceThreshold*/ null);
    }

    /** Backwards-compatible constructor — defaults the listener to a no-op + deterministic grouping. */
    public Configuration(
        String exchangeKey,
        WsLimits wsLimits,
        Set<Link.Subscription> subscriptions,
        String klineInterval,
        int targetStreamsPerLink,
        int streamsPerInstrument,
        Duration discoveryRefreshInterval,
        Duration interLinkStartupJitter,
        Function<Set<Instrument>, StreamClient> streamClientFactory) {
      this(
          exchangeKey,
          wsLimits,
          subscriptions,
          klineInterval,
          targetStreamsPerLink,
          streamsPerInstrument,
          discoveryRefreshInterval,
          interLinkStartupJitter,
          streamClientFactory,
          LinkLifecycleListener.NOOP,
          /*randomizeLinkGrouping*/ false,
          /*linkSilenceThreshold*/ null);
    }
  }

  private final Configuration cfg;
  private final InstrumentsCache instrumentsCache;
  private final LinkPartitioner partitioner;
  /**
   * ONE connection-rate gate shared by every {@link Link} this manager builds. A node has a single
   * IP, so all links to this exchange-market must trickle their socket opens through the same gate
   * to stay under the exchange's per-IP connection-rate cap. (Different exchange-markets — e.g.
   * binance-spot vs bybit — each get their own manager and thus their own gate.)
   */
  private final ConnectionRateLimiter connectionRateLimiter;
  private final ScheduledExecutorService scheduler;
  private final List<Link> links = new ArrayList<>();
  private final AtomicInteger linkId = new AtomicInteger();
  private final java.util.concurrent.atomic.AtomicLong messagesReceived =
      new java.util.concurrent.atomic.AtomicLong();
  /** Epoch-µs event timestamp of the most recent message received; 0 until the first. */
  private final java.util.concurrent.atomic.AtomicLong lastEventMicros =
      new java.util.concurrent.atomic.AtomicLong();

  private Consumer<Ticker> tickerSink = t -> {};
  private Consumer<Kline> klineSink = k -> {};
  private Consumer<FundingRate> fundingSink = f -> {};
  private ScheduledFuture<?> refreshTask;
  private ScheduledFuture<?> silenceWatchdogTask;
  private volatile boolean started;

  public LinkManager(Configuration cfg, InstrumentsCache cache) {
    this(cfg, cache, Executors.newScheduledThreadPool(2, r -> {
      Thread t = new Thread(r, "qtstreamx-link-" + cfg.exchangeKey());
      t.setDaemon(true);
      return t;
    }));
  }

  public LinkManager(Configuration cfg, InstrumentsCache cache, ScheduledExecutorService scheduler) {
    this.cfg = cfg;
    this.instrumentsCache = cache;
    this.scheduler = scheduler;
    this.connectionRateLimiter =
        ConnectionRateLimiter.forExchange(cfg.exchangeKey(), cfg.wsLimits());
    Long shuffleSeed = cfg.randomizeLinkGrouping() ? deriveLinkShuffleSeed(cfg.exchangeKey()) : null;
    this.partitioner =
        new LinkPartitioner(
            cfg.wsLimits(), cfg.targetStreamsPerLink(), cfg.streamsPerInstrument(), shuffleSeed);
    if (shuffleSeed == null) {
      log.info("LinkManager[{}] link grouping = deterministic (input order)", cfg.exchangeKey());
    } else {
      log.info(
          "LinkManager[{}] link grouping = randomized (seeded shuffle) seed={} host={} salt={}",
          cfg.exchangeKey(),
          shuffleSeed,
          hostnameForSeed(),
          seedSalt().isEmpty() ? "<unset>" : seedSalt());
    }
  }

  /**
   * Per-instance link-shuffle seed: a stable 64-bit hash of salt + host + exchange, so the grouping
   * is fixed for this instance's lifetime (idempotent reconnects) but differs across instances.
   * Redundant publisher instances thus group the same universe differently, decorrelating which
   * instruments share a WS link.
   *
   * <p><b>The seed is only as unique as {@code HOSTNAME}, and a container's hostname is whatever
   * its deployment says it is.</b> Measured on the ingest fleet 2026-07-28: every node ran its two
   * publishers as {@code pubx-1} and {@code pubx-2}, chosen so the NATS connection name would be
   * readable. Six processes on three nodes across two continents therefore produced exactly
   * <em>two</em> distinct groupings, and a mute link took the same instruments out on every node at
   * once — the decorrelation this seed exists for was silently absent, and adding a third node
   * contributed none of it.
   *
   * <p>{@code QTSTREAMX_LINK_SEED_SALT} exists for that case: a deployment that cannot give each
   * instance a unique hostname passes something that is unique (the host's own name, a node id).
   * Unset, behaviour is exactly as before.
   */
  private static long deriveLinkShuffleSeed(String exchangeKey) {
    return linkShuffleSeed(seedSalt(), hostnameForSeed(), exchangeKey);
  }

  /** The seed function itself — pure, so the uniqueness it is supposed to provide is testable. */
  static long linkShuffleSeed(String salt, String host, String exchangeKey) {
    // The salt is PREPENDED WITH ITS SEPARATOR only when present. Concatenating an empty salt with
    // a separator regardless would leave a leading '|', changing every seed in the fleet on upgrade
    // and reshuffling every link's bucket for no reason — unset must mean exactly as before.
    String prefix = (salt == null || salt.isBlank()) ? "" : salt.trim() + "|";
    long h = 1125899906842597L; // FNV-ish prime seed
    for (char c : (prefix + host + '|' + exchangeKey).toCharArray()) {
      h = 31 * h + c;
    }
    return h;
  }

  /** Optional deployment-supplied uniqueness for the link-shuffle seed; "" when unset. */
  private static String seedSalt() {
    String s = System.getenv("QTSTREAMX_LINK_SEED_SALT");
    return (s == null || s.isBlank()) ? "" : s.trim();
  }

  private static String hostnameForSeed() {
    String h = System.getenv("HOSTNAME");
    if (h == null || h.isBlank()) {
      try {
        h = java.net.InetAddress.getLocalHost().getHostName();
      } catch (Exception e) {
        h = "unknown-host";
      }
    }
    return h;
  }

  /** Wire the ticker consumer. Must be called before {@link #start()}. */
  public LinkManager onTicker(Consumer<Ticker> sink) {
    Objects.requireNonNull(sink);
    this.tickerSink =
        t -> {
          messagesReceived.incrementAndGet();
          lastEventMicros.set(t.timestamp());
          sink.accept(t);
        };
    return this;
  }

  public LinkManager onKline(Consumer<Kline> sink) {
    Objects.requireNonNull(sink);
    this.klineSink =
        k -> {
          messagesReceived.incrementAndGet();
          lastEventMicros.set(k.timestamp());
          sink.accept(k);
        };
    return this;
  }

  public LinkManager onFundingRate(Consumer<FundingRate> sink) {
    Objects.requireNonNull(sink);
    this.fundingSink =
        f -> {
          messagesReceived.incrementAndGet();
          lastEventMicros.set(f.timestamp());
          sink.accept(f);
        };
    return this;
  }

  /**
   * Discover instruments, partition into links, connect with jitter, and schedule
   * periodic refresh.
   */
  public CompletionStage<Void> start() {
    if (started) {
      return CompletableFuture.completedFuture(null);
    }
    started = true;
    return instrumentsCache
        .refresh()
        .thenAccept(
            instruments -> {
              List<Set<Instrument>> buckets = partitioner.partition(instruments);
              log.info(
                  "{}: partitioned {} instruments across {} links",
                  cfg.exchangeKey(),
                  instruments.size(),
                  buckets.size());

              long jitterMs = cfg.interLinkStartupJitter().toMillis();
              for (int i = 0; i < buckets.size(); i++) {
                Set<Instrument> bucket = buckets.get(i);
                Link link = buildLink(bucket);
                synchronized (links) {
                  links.add(link);
                }
                // The bucket map, once, at startup. Which instruments share a link is otherwise
                // knowable only by reading the partitioner's shuffle by hand, and it is the first
                // thing needed when one link misbehaves and its instruments go missing.
                log.info(
                    "{}: link {} owns {} instruments: [{}]",
                    cfg.exchangeKey(),
                    link.id(),
                    bucket.size(),
                    Link.renderInstruments(bucket));
                long delayMs = (long) i * jitterMs;
                scheduler.schedule(
                    () -> connectLink(link, bucket), delayMs, TimeUnit.MILLISECONDS);
              }

              refreshTask =
                  scheduler.scheduleAtFixedRate(
                      this::refreshAndReconcile,
                      cfg.discoveryRefreshInterval().toMillis(),
                      cfg.discoveryRefreshInterval().toMillis(),
                      TimeUnit.MILLISECONDS);

              Duration silenceThreshold = cfg.linkSilenceThreshold();
              if (silenceThreshold != null && !silenceThreshold.isZero()) {
                // Sample at a third of the threshold so a link is named within ~1.3x of going
                // quiet rather than up to 2x.
                long periodMs = Math.max(5_000L, silenceThreshold.toMillis() / 3);
                silenceWatchdogTask =
                    scheduler.scheduleAtFixedRate(
                        this::checkSilentLinks, periodMs, periodMs, TimeUnit.MILLISECONDS);
              }
            })
        .exceptionally(
            err -> {
              log.error("{}: start failed", cfg.exchangeKey(), err);
              return null;
            });
  }

  /**
   * Name every connected-but-mute link, with how long it has been quiet and how many instruments
   * are behind it, and force each one to reconnect. Runs on the manager's scheduler; never throws
   * into it.
   *
   * <p>A silent link never fires {@link StreamClient#onDisconnect}, so nothing else in this class
   * will ever reconnect it — that path only runs on an actual drop, and here the socket never
   * drops. Left alone, {@code binance#3(quiet=837s)} just keeps climbing indefinitely: without an
   * active reconnect, a link that goes mute stays mute until something external notices the
   * throughput dip and intervenes by hand. A targeted per-link reconnect handles the failure
   * without touching the other links in the pool, let alone every other exchange group on the
   * process.
   */
  private void checkSilentLinks() {
    try {
      Duration threshold = cfg.linkSilenceThreshold();
      List<Link> silent = silentLinks(threshold);
      if (silent.isEmpty()) return;
      int instruments = silent.stream().mapToInt(Link::subscribedCount).sum();
      log.warn(
          "{}: {} of {} links CONNECTED BUT SILENT for >= {}s — {} instruments not being captured:"
              + " {}",
          cfg.exchangeKey(),
          silent.size(),
          linkCount(),
          threshold.toSeconds(),
          instruments,
          silent.stream()
              .map(
                  l ->
                      l.id()
                          + "(quiet="
                          + l.sinceLastMessage().map(d -> d.toSeconds() + "s").orElse("never")
                          + ",instruments="
                          + l.subscribedCount()
                          + ")")
              .collect(java.util.stream.Collectors.joining(", ")));
      for (Link l : silent) {
        try {
          cfg.lifecycleListener()
              .onSilent(
                  l.id(),
                  l.sinceLastMessage().orElse(l.upTime().orElse(Duration.ZERO)),
                  l.subscribedCount());
        } catch (RuntimeException e) {
          log.warn("{} listener.onSilent threw: {}", l.id(), e.getMessage());
        }
        // Dispatch through the scheduler rather than calling reconnectWithBackoff inline: it
        // blocks on the connection-rate limiter's acquire(), and this method itself runs as a
        // scheduled task — blocking it here would delay every other link's renewal/disconnect
        // handling behind however long the rate limiter makes THIS reconnect wait. Same 100ms
        // handoff Link.onStreamDisconnect already uses for a real drop; the shared limiter is
        // what actually paces a burst of these (e.g. all 55 links of one group going silent
        // together), same as it paces a mass-disconnect reconnect storm today.
        //
        // No self-retrigger risk: isSilent() requires upTime() >= threshold, and a successful
        // reconnect resets upTime() to ~0 — so the earliest this link can be reported silent
        // again is one full `threshold` after THIS reconnect, comfortably past the next one or
        // two watchdog passes (period is threshold/3). If the reconnect itself fails,
        // reconnectWithBackoff's own retry loop keeps trying independently of this watchdog.
        scheduler.schedule(() -> l.reconnectWithBackoff(0), 100, TimeUnit.MILLISECONDS);
      }
    } catch (RuntimeException e) {
      log.warn("{}: silence watchdog failed: {}", cfg.exchangeKey(), e.getMessage());
    }
  }

  /** Instruments currently subscribed across all links. */
  public Set<Instrument> subscribedInstruments() {
    Set<Instrument> all = new HashSet<>();
    synchronized (links) {
      for (Link l : links) all.addAll(l.instruments());
    }
    return all;
  }

  /** Count of live (connected) links. */
  public int connectedLinkCount() {
    synchronized (links) {
      return (int) links.stream().filter(Link::isConnected).count();
    }
  }

  /**
   * Links that are connected but have delivered nothing for at least the configured silence
   * threshold. This is the gap {@link #connectedLinkCount()} cannot express: an open socket that
   * answers pings and carries no data still counts as connected, and the instruments in its bucket
   * simply stop being captured — with no error anywhere.
   */
  public List<Link> silentLinks() {
    return silentLinks(cfg.linkSilenceThreshold());
  }

  /** As {@link #silentLinks()}, with an explicit threshold. {@link Duration#ZERO} returns none. */
  public List<Link> silentLinks(Duration threshold) {
    if (threshold == null || threshold.isZero() || threshold.isNegative()) return List.of();
    synchronized (links) {
      return links.stream().filter(l -> l.isSilent(threshold)).toList();
    }
  }

  /**
   * Connected links that are also delivering data. This is the number worth alerting on: it counts
   * the presence of what should be happening, rather than the absence of an error.
   */
  public int liveLinkCount() {
    Duration threshold = cfg.linkSilenceThreshold();
    synchronized (links) {
      return (int)
          links.stream()
              .filter(Link::isConnected)
              .filter(l -> threshold == null || threshold.isZero() || !l.isSilent(threshold))
              .count();
    }
  }

  /** Instruments whose link is currently silent — i.e. the capture hole, named. */
  public Set<Instrument> instrumentsOnSilentLinks() {
    Set<Instrument> out = new HashSet<>();
    for (Link l : silentLinks()) out.addAll(l.instruments());
    return out;
  }

  public int linkCount() {
    synchronized (links) {
      return links.size();
    }
  }

  /**
   * Cumulative market-data messages received across all links (ticker+kline+frate). A stalled
   * counter while {@link #connectedLinkCount()} stays positive is the "connected but silent" zombie
   * signal a watchdog uses to trigger {@link #reconnectAll()}.
   */
  public long messagesReceived() {
    return messagesReceived.get();
  }

  /**
   * Epoch-µs event timestamp of the most recent market-data message received across all links (0 if
   * none yet). Wall-clock minus this is the feed's exchange-lag — how stale the freshest datum is.
   *
   * <p>Caveat: KLINE frames carry the candle OPEN time, so a klines manager's lag grows within each
   * interval and resets on bucket roll-over; TICKER/FUNDING_RATE carry true event time.
   */
  public long lastEventMicros() {
    return lastEventMicros.get();
  }

  /** Exchange-market key (e.g. {@code binance-spot}) used as a label in metrics and logs. */
  public String exchangeKey() {
    return cfg.exchangeKey();
  }

  @Override
  public void close() {
    ScheduledFuture<?> w = silenceWatchdogTask;
    if (w != null) w.cancel(false);
    ScheduledFuture<?> r = refreshTask;
    if (r != null) r.cancel(false);
    synchronized (links) {
      for (Link l : links) l.close();
    }
    scheduler.shutdownNow();
  }

  /**
   * Force every link to drop its current WebSocket and reconnect, re-subscribing all instruments.
   *
   * <p>Recovery for the "connected but silent" zombie: a link's WS still reports {@code connected}
   * (so {@link #connectedLinkCount()} stays full and the liveness probe passes) yet no market data
   * flows. {@link Link#reconnectWithBackoff(int)} closes the stale client and reopens a fresh one,
   * which re-establishes the data stream. Reconnections are staggered on the manager scheduler with
   * the same inter-link jitter as startup to avoid a reconnect thundering herd / exchange rate-limit.
   */
  public void reconnectAll() {
    if (!started) return;
    long jitterMs = cfg.interLinkStartupJitter().toMillis();
    List<Link> snapshot;
    synchronized (links) {
      snapshot = new ArrayList<>(links);
    }
    log.warn(
        "{}: reconnectAll — forcing {} links to drop+reconnect (zombie recovery)",
        cfg.exchangeKey(),
        snapshot.size());
    for (int i = 0; i < snapshot.size(); i++) {
      Link l = snapshot.get(i);
      long delayMs = (long) i * jitterMs;
      scheduler.schedule(
          () -> {
            try {
              l.reconnectWithBackoff(0);
            } catch (RuntimeException e) {
              log.warn("{}: reconnectAll link {} failed: {}", cfg.exchangeKey(), l.id(), e.getMessage());
            }
          },
          delayMs,
          TimeUnit.MILLISECONDS);
    }
  }

  // --------------------------------------------------------------------------
  // Internals
  // --------------------------------------------------------------------------

  private Link buildLink(Set<Instrument> bucket) {
    String id = cfg.exchangeKey() + "#" + linkId.incrementAndGet();
    Link.Configuration linkCfg =
        new Link.Configuration(
            id,
            cfg.wsLimits(),
            cfg.subscriptions(),
            cfg.klineInterval(),
            () -> cfg.streamClientFactory().apply(bucket),
            tickerSink,
            klineSink,
            fundingSink,
            scheduler,
            cfg.lifecycleListener(),
            connectionRateLimiter);
    return new Link(linkCfg);
  }

  private void connectLink(Link link, Set<Instrument> bucket) {
    try {
      link.connect(bucket);
    } catch (Exception e) {
      log.warn("{} initial connect failed: {} — will retry", link.id(), e.getMessage());
      link.reconnectWithBackoff(0);
    }
  }

  private void refreshAndReconcile() {
    instrumentsCache
        .refresh()
        .thenAccept(
            latest -> {
              Set<Instrument> current = subscribedInstruments();
              Set<Instrument> added = new HashSet<>(latest);
              added.removeAll(current);
              Set<Instrument> removed = new HashSet<>(current);
              removed.removeAll(latest);
              if (added.isEmpty() && removed.isEmpty()) return;

              log.info(
                  "{}: refresh diff — {} added, {} removed",
                  cfg.exchangeKey(),
                  added.size(),
                  removed.size());

              for (Instrument ins : added) {
                int idx;
                synchronized (links) {
                  idx = partitioner.assign(links);
                }
                if (idx >= 0) {
                  try {
                    links.get(idx).addInstrument(ins);
                  } catch (Exception e) {
                    log.warn("{}: failed to add {}: {}", cfg.exchangeKey(), ins, e.getMessage());
                  }
                } else {
                  // All links full — spawn a new link to hold the overflow.
                  Set<Instrument> single = Set.of(ins);
                  Link newLink = buildLink(single);
                  synchronized (links) {
                    links.add(newLink);
                  }
                  connectLink(newLink, single);
                }
              }

              for (Instrument ins : removed) {
                synchronized (links) {
                  for (Link l : links) l.removeInstrument(ins);
                }
              }
            })
        .exceptionally(
            err -> {
              log.warn("{}: refresh failed: {}", cfg.exchangeKey(), err.getMessage());
              return null;
            });
  }
}
