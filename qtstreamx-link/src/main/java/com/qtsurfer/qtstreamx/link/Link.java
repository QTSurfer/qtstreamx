package com.qtsurfer.qtstreamx.link;

import com.qtsurfer.qtstreamx.core.client.StreamClient;
import com.qtsurfer.qtstreamx.core.model.FundingRate;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.model.Kline;
import com.qtsurfer.qtstreamx.core.model.Ticker;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One managed WebSocket link to an exchange: a {@link StreamClient} plus lifecycle
 * (initial subscribe, reconnect with exponential backoff, preemptive renewal before
 * the exchange's connection-lifetime cap).
 *
 * <p>Handlers (ticker/kline/funding) are set ONCE via {@link Configuration}; per-instrument
 * subscribe/unsubscribe is done through {@link #addInstrument(Instrument)} / {@link
 * #removeInstrument(Instrument)}. Emissions for all instruments on this link flow into
 * the shared consumer supplied at construction.
 *
 * <p>Not thread-safe for concurrent mutation of the subscription set — {@link LinkManager}
 * owns a single {@link Link} and serialises calls to it. The emit path (from WS reader
 * thread into consumers) is thread-safe (consumers must be).
 */
public final class Link {

  private static final Logger log = LoggerFactory.getLogger(Link.class);

  /** Upper bound on symbols rendered into a single log line. */
  private static final int LOG_INSTRUMENT_CAP = 120;

  /** What a link subscribes to when an instrument is added. */
  public enum Subscription {
    TICKER,
    KLINE,
    FUNDING_RATE
  }

  /** Per-link configuration — immutable after construction. */
  public record Configuration(
      String id,
      WsLimits wsLimits,
      Set<Subscription> subscriptions,
      String klineInterval,
      Supplier<StreamClient> streamClientFactory,
      Consumer<Ticker> tickerSink,
      Consumer<Kline> klineSink,
      Consumer<FundingRate> fundingRateSink,
      ScheduledExecutorService scheduler,
      LinkLifecycleListener lifecycleListener,
      ConnectionRateLimiter connectionRateLimiter) {

    public Configuration {
      subscriptions = Set.copyOf(subscriptions);
      if (subscriptions.contains(Subscription.KLINE) && (klineInterval == null || klineInterval.isBlank())) {
        throw new IllegalArgumentException("klineInterval must be set when subscribing to KLINE");
      }
      if (lifecycleListener == null) {
        lifecycleListener = LinkLifecycleListener.NOOP;
      }
      if (connectionRateLimiter == null) {
        connectionRateLimiter = ConnectionRateLimiter.unlimited();
      }
    }

    /** Backwards-compatible constructor — listener supplied, no connection-rate gate. */
    public Configuration(
        String id,
        WsLimits wsLimits,
        Set<Subscription> subscriptions,
        String klineInterval,
        Supplier<StreamClient> streamClientFactory,
        Consumer<Ticker> tickerSink,
        Consumer<Kline> klineSink,
        Consumer<FundingRate> fundingRateSink,
        ScheduledExecutorService scheduler,
        LinkLifecycleListener lifecycleListener) {
      this(
          id,
          wsLimits,
          subscriptions,
          klineInterval,
          streamClientFactory,
          tickerSink,
          klineSink,
          fundingRateSink,
          scheduler,
          lifecycleListener,
          ConnectionRateLimiter.unlimited());
    }

    /** Backwards-compatible constructor — defaults the listener to a no-op + no rate gate. */
    public Configuration(
        String id,
        WsLimits wsLimits,
        Set<Subscription> subscriptions,
        String klineInterval,
        Supplier<StreamClient> streamClientFactory,
        Consumer<Ticker> tickerSink,
        Consumer<Kline> klineSink,
        Consumer<FundingRate> fundingRateSink,
        ScheduledExecutorService scheduler) {
      this(
          id,
          wsLimits,
          subscriptions,
          klineInterval,
          streamClientFactory,
          tickerSink,
          klineSink,
          fundingRateSink,
          scheduler,
          LinkLifecycleListener.NOOP,
          ConnectionRateLimiter.unlimited());
    }
  }

  private final Configuration cfg;
  private final Set<Instrument> subscribed =
      Collections.synchronizedSet(new LinkedHashSet<>());

  private final AtomicReference<StreamClient> current = new AtomicReference<>();

  /**
   * Every client this link has created and not yet closed. Normally holds exactly the current
   * client (or none while reconnecting); anything beyond that is a client nobody will ever close —
   * a live socket still feeding the sinks, which shows up downstream as duplicated data. Compared
   * by identity, closed at most once.
   */
  private final Set<StreamClient> liveClients =
      Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()));

  /**
   * True from the moment a reconnect starts replacing the client until the replacement is
   * installed (including the backoff waits between failed attempts). Reconnects are triggered from
   * several places — preemptive renewal, a dropped socket, the silence watchdog, a forced
   * reconnect-all — on a scheduler with more than one thread. Two of them running at once each
   * open a client and the second install silently replaces the first, which is never closed.
   */
  private final AtomicBoolean reconnecting = new AtomicBoolean();

  private final AtomicInteger reconnectFailures = new AtomicInteger();
  private final AtomicLong connectedAtEpochMs = new AtomicLong();
  /** Epoch ms at which the link became disconnected; 0 means currently up or never connected. */
  private final AtomicLong disconnectedAtEpochMs = new AtomicLong();
  private final AtomicReference<ScheduledFuture<?>> renewalTask = new AtomicReference<>();
  private volatile boolean closed;

  /**
   * Per-link market-data accounting. {@link LinkManager} already counts messages for the exchange
   * as a whole, which cannot distinguish "this exchange is quiet" from "one of these ten links has
   * gone mute while the other nine carry the traffic". A link that is open, answering pings and
   * delivering nothing reports {@code isConnected() == true} forever, and the instruments in its
   * bucket simply stop existing — silently, and only in that bucket.
   *
   * <p>{@code messages} is a {@link LongAdder} rather than an {@code AtomicLong} because this is
   * the hot path: every ticker, kline and funding frame on this socket passes through it.
   */
  private final LongAdder messages = new LongAdder();

  /** Epoch ms of the last market-data message on this link; 0 = none ever received. */
  private volatile long lastMessageAtEpochMs;

  /**
   * Sinks pre-wrapped once at construction rather than per subscribe call: {@link #subscribeOne}
   * runs once per instrument per (re)connect, and a link can hold hundreds.
   */
  private final Consumer<Ticker> recordingTickerSink;

  private final Consumer<Kline> recordingKlineSink;
  private final Consumer<FundingRate> recordingFundingSink;

  public Link(Configuration cfg) {
    this.cfg = cfg;
    this.recordingTickerSink =
        t -> {
          recordMessage();
          cfg.tickerSink().accept(t);
        };
    this.recordingKlineSink =
        k -> {
          recordMessage();
          cfg.klineSink().accept(k);
        };
    this.recordingFundingSink =
        f -> {
          recordMessage();
          cfg.fundingRateSink().accept(f);
        };
  }

  /**
   * This link's instruments as exchange symbols, for the log. Capped: a bucket is normally tens of
   * instruments, but the cap keeps one pathological configuration from producing a megabyte line.
   */
  public String instrumentsForLog() {
    synchronized (subscribed) {
      return renderInstruments(subscribed);
    }
  }

  /**
   * Same rendering for a set the link does not own yet. {@link LinkManager} logs the partition map
   * at startup, before {@link #connect} has handed each link its bucket — asking the link itself at
   * that point returns nothing, because it genuinely owns nothing yet.
   */
  public static String renderInstruments(Collection<Instrument> instruments) {
    List<String> syms = instruments.stream().map(Instrument::symbol).sorted().toList();
    if (syms.size() <= LOG_INSTRUMENT_CAP) return String.join(",", syms);
    return String.join(",", syms.subList(0, LOG_INSTRUMENT_CAP))
        + ",… (+" + (syms.size() - LOG_INSTRUMENT_CAP) + " more)";
  }

  private void recordMessage() {
    messages.increment();
    lastMessageAtEpochMs = System.currentTimeMillis();
  }

  public String id() {
    return cfg.id();
  }

  public int subscribedCount() {
    return subscribed.size();
  }

  public Set<Instrument> instruments() {
    synchronized (subscribed) {
      return Set.copyOf(subscribed);
    }
  }

  /** True if this link is connected and the streaming client is open. */
  public boolean isConnected() {
    StreamClient sc = current.get();
    return sc != null && sc.isConnected();
  }

  /**
   * Record the full set of instruments this link will own, then connect.
   * Call exactly once. For live additions after connect, use {@link #addInstrument(Instrument)}.
   */
  public void connect(Set<Instrument> initialInstruments) throws Exception {
    if (closed) throw new IllegalStateException("link " + cfg.id() + " is closed");
    synchronized (subscribed) {
      subscribed.clear();
      subscribed.addAll(initialInstruments);
    }
    announce(openAndInstall());
    scheduleRenewal();
  }

  /** Live-subscribe a new instrument on the existing WebSocket. */
  public void addInstrument(Instrument ins) throws Exception {
    if (closed) throw new IllegalStateException("link " + cfg.id() + " is closed");
    synchronized (subscribed) {
      if (!subscribed.add(ins)) return;
    }
    StreamClient sc = current.get();
    if (sc == null || !sc.isConnected()) {
      log.debug("{} adding {} while disconnected — will be picked up on next connect", cfg.id(), ins);
      return;
    }
    subscribeOne(sc, ins);
  }

  /**
   * Remove an instrument from this link's set. The underlying StreamClient typically has
   * no explicit unsubscribe in every implementation; for Binance we rely on the next
   * reconnect to drop the stream from the URL.
   */
  public void removeInstrument(Instrument ins) {
    synchronized (subscribed) {
      subscribed.remove(ins);
    }
  }

  /** Close permanently. No more reconnects. */
  public void close() {
    closed = true;
    cancelRenewal();
    StreamClient sc = current.getAndSet(null);
    if (sc != null) closeClient(sc);
  }

  /**
   * Number of stream clients this link has opened and not closed. One while connected, zero while
   * reconnecting or closed. Anything higher is a leak: a client still holding a socket that no
   * reconnect will ever close.
   */
  public int openClients() {
    return liveClients.size();
  }

  /** Milliseconds since the current WebSocket was (re)connected. */
  public Optional<Duration> upTime() {
    long t = connectedAtEpochMs.get();
    if (t == 0) return Optional.empty();
    return Optional.of(Duration.ofMillis(System.currentTimeMillis() - t));
  }

  public int reconnectFailureCount() {
    return reconnectFailures.get();
  }

  /** Market-data messages received on this link since it was created (survives reconnects). */
  public long messageCount() {
    return messages.sum();
  }

  /** Time since the last market-data message on this link; empty if none was ever received. */
  public Optional<Duration> sinceLastMessage() {
    long t = lastMessageAtEpochMs;
    if (t == 0) return Optional.empty();
    return Optional.of(Duration.ofMillis(Math.max(0, System.currentTimeMillis() - t)));
  }

  /**
   * True when this link is connected but has delivered nothing for at least {@code threshold} —
   * the failure mode {@link #isConnected()} cannot see, because the socket is open and healthy
   * while the subscription behind it produces no data.
   *
   * <p>Deliberately requires the link to have been up for at least {@code threshold} as well. A
   * link that connected two seconds ago has not had time to produce anything, and reporting it as
   * silent would flag the normal case (something starting) instead of the broken one. A link that
   * keeps reconnecting faster than {@code threshold} is never reported here either — that is a
   * different fault, and it already has its own signal in {@link #reconnectFailureCount()} and the
   * disconnect events.
   *
   * <p>Choose {@code threshold} against the slowest subscription the link carries. A link holding
   * only {@link Subscription#FUNDING_RATE} is legitimately quiet for minutes; one that also carries
   * tickers is not.
   */
  public boolean isSilent(Duration threshold) {
    if (!isConnected()) return false;
    Duration up = upTime().orElse(Duration.ZERO);
    if (up.compareTo(threshold) < 0) return false;
    long last = lastMessageAtEpochMs;
    if (last == 0) return true;
    return System.currentTimeMillis() - last >= threshold.toMillis();
  }

  // --------------------------------------------------------------------------
  // Internals
  // --------------------------------------------------------------------------

  /** What {@link #openAndInstall()} hands to {@link #announce}: how many instruments, how long down. */
  private record Opened(int instruments, Duration outage) {}

  /**
   * Open a client, subscribe everything, connect it and make it the link's current client.
   * Returns {@code null} when the link was closed while this was in flight, in which case the new
   * client has already been closed. Never leaves a client behind: any failure closes it before the
   * exception propagates.
   */
  private Opened openAndInstall() throws Exception {
    // Gate EVERY socket open (initial subscribe, reconnect, preemptive renewal) through the
    // per-IP/per-exchange connection-rate limiter so a node never self-inflicts a connection-rate
    // ban. Blocks/trickles under a reconnect storm; aborts promptly if the link is closing. The
    // permit is consumed here, before connect(), so even a failed handshake counts — as the
    // exchange counts it.
    cfg.connectionRateLimiter().acquire(() -> closed);
    StreamClient sc = cfg.streamClientFactory().get();
    liveClients.add(sc);
    boolean installed = false;
    try {
      // Wire disconnect BEFORE connect so any drop — including during the initial
      // handshake — flows into the reconnect backoff path.
      sc.onDisconnect(() -> onStreamDisconnect(sc));
      // Subscribe every instrument BEFORE connect (Binance embeds streams in the URL).
      Set<Instrument> snapshot;
      synchronized (subscribed) {
        snapshot = Set.copyOf(subscribed);
      }
      for (Instrument ins : snapshot) {
        subscribeOne(sc, ins);
      }
      sc.connect();
      StreamClient previous = current.getAndSet(sc);
      installed = true;
      if (previous != null && previous != sc) {
        // Nothing should still be installed here; if something is, it is a client that would
        // otherwise be orphaned with an open socket.
        log.warn("{} replacing a client that was still installed — closing it", cfg.id());
        closeClient(previous);
      }
      if (closed) {
        // close() ran while this connect was in flight and may have looked at `current` before
        // the install above, so it cannot be relied on to see this client.
        current.compareAndSet(sc, null);
        closeClient(sc);
        return null;
      }
      long now = System.currentTimeMillis();
      long downSince = disconnectedAtEpochMs.getAndSet(0);
      Duration outage = downSince == 0 ? Duration.ZERO : Duration.ofMillis(now - downSince);
      connectedAtEpochMs.set(now);
      reconnectFailures.set(0);
      // A clean connect tells the shared limiter the exchange is no longer throttling this IP,
      // so its adaptive backpressure decays back towards the steady-state rate.
      cfg.connectionRateLimiter().onSuccess();
      return new Opened(snapshot.size(), outage);
    } catch (Throwable t) {
      if (!installed) closeClient(sc);
      throw t;
    }
  }

  private void announce(Opened opened) {
    if (opened == null) return;
    if (opened.outage().isZero()) {
      log.info("{} connected with {} instruments", cfg.id(), opened.instruments());
    } else {
      log.info(
          "{} reconnected with {} instruments after {} ms outage",
          cfg.id(),
          opened.instruments(),
          opened.outage().toMillis());
    }
    try {
      cfg.lifecycleListener().onConnected(cfg.id(), opened.instruments(), opened.outage());
    } catch (RuntimeException e) {
      log.warn("{} listener.onConnected threw: {}", cfg.id(), e.getMessage());
    }
  }

  /** Close a client exactly once, whoever gets there first. */
  private void closeClient(StreamClient sc) {
    if (!liveClients.remove(sc)) return;
    try {
      sc.close();
    } catch (Exception e) {
      log.debug("{} client close: {}", cfg.id(), e.getMessage());
    }
  }

  private void onStreamDisconnect(StreamClient sc) {
    if (closed) return;
    // Ignore stale callbacks from a client we've already rotated out (e.g.
    // preemptive renew fires while a late error from the old socket arrives).
    if (current.get() != sc) {
      log.debug("{} ignoring stale disconnect from rotated client", cfg.id());
      return;
    }
    // Name them. A dropped link is not an abstract event: it is these instruments, on this venue,
    // not being captured until it comes back — and when a link is dropped repeatedly, this line is
    // what identifies the subset that is quietly missing from the store.
    log.warn(
        "{} WS disconnected, scheduling reconnect — {} instruments affected: [{}]",
        cfg.id(),
        subscribedCount(),
        instrumentsForLog());
    // Mark the drop instant so the next successful connect can compute outage.
    // compareAndSet keeps the first-disconnect timestamp if multiple disconnect
    // signals arrive (e.g. error → close cascade) before we reconnect.
    disconnectedAtEpochMs.compareAndSet(0, System.currentTimeMillis());
    try {
      cfg.lifecycleListener().onDisconnected(cfg.id());
    } catch (RuntimeException e) {
      log.warn("{} listener.onDisconnected threw: {}", cfg.id(), e.getMessage());
    }
    // Hand off to the scheduler so we never block the WS reader thread and so
    // any in-flight buffered emissions drain cleanly before reopen.
    cfg.scheduler().schedule(() -> reconnectWithBackoff(0), 100, TimeUnit.MILLISECONDS);
  }

  private void subscribeOne(StreamClient sc, Instrument ins) {
    for (Subscription s : cfg.subscriptions()) {
      switch (s) {
        case TICKER -> sc.subscribeTicker(ins, recordingTickerSink);
        case KLINE -> sc.subscribeKline(ins, cfg.klineInterval(), recordingKlineSink);
        case FUNDING_RATE -> sc.subscribeFundingRate(ins, recordingFundingSink);
      }
    }
  }

  /**
   * Arm the one preemptive-renewal timer, cancelling whichever was armed before. Every successful
   * connect calls this, including the ones caused by a dropped socket or a forced reconnect rather
   * than by a renewal; without the cancel each of those left an extra timer behind, and since every
   * timer re-arms itself when it fires, the number of live timers per link only ever grew. A link
   * with many of them renews many times a day, and the renewals land close enough together to
   * overlap.
   */
  private void scheduleRenewal() {
    if (closed) return;
    Duration delay = cfg.wsLimits().renewBefore();
    ScheduledFuture<?> next =
        cfg.scheduler().schedule(this::renew, delay.toMillis(), TimeUnit.MILLISECONDS);
    ScheduledFuture<?> previous = renewalTask.getAndSet(next);
    if (previous != null) previous.cancel(false);
    if (closed) cancelRenewal();
  }

  private void cancelRenewal() {
    ScheduledFuture<?> r = renewalTask.getAndSet(null);
    if (r != null) r.cancel(false);
  }

  private void renew() {
    if (closed) return;
    if (reconnecting.get()) {
      // A reconnect is already replacing the client, which is all a renewal would do. Keep the
      // timer armed in case that reconnect never gets as far as arming it.
      log.debug("{} renewal skipped: reconnect already in flight", cfg.id());
      scheduleRenewal();
      return;
    }
    log.info("{} preemptive renewal", cfg.id());
    reconnectWithBackoff(0);
  }

  /**
   * Reopen the WebSocket and re-subscribe. Retries with exponential backoff
   * (capped at 60 s) as long as the link isn't closed.
   *
   * <p>One reconnect at a time per link: a request that arrives while another is replacing the
   * client is dropped, because that reconnect ends in a fresh client anyway.
   */
  public void reconnectWithBackoff(int attempt) {
    if (closed) return;
    if (!reconnecting.compareAndSet(false, true)) {
      log.debug("{} reconnect already in flight — request dropped", cfg.id());
      return;
    }
    reconnectAttempt(attempt);
  }

  private void reconnectAttempt(int attempt) {
    boolean released = false;
    boolean retryScheduled = false;
    try {
      if (closed) return;
      StreamClient old = current.getAndSet(null);
      if (old != null) closeClient(old);
      Opened opened = openAndInstall();
      // The replacement is installed: anything that arrives from here on (a drop of the new
      // client, the next renewal) has a client to act on, so let it through.
      reconnecting.set(false);
      released = true;
      announce(opened);
      scheduleRenewal();
    } catch (InterruptedException ie) {
      // The connection-rate gate aborted our wait because the link is closing. Restore the
      // interrupt flag and stop — do NOT reschedule a doomed reconnect.
      Thread.currentThread().interrupt();
      log.debug("{} reconnect aborted (closing)", cfg.id());
    } catch (Exception e) {
      if (closed) return; // closed mid-attempt — don't keep retrying
      int next = reconnectFailures.incrementAndGet();
      // A handshake-termination / explicit rate-limit error means the exchange is actively
      // throttling this IP: feed it to the shared limiter so the WHOLE exchange backs off harder.
      if (ConnectionRateLimiter.isBackpressureSignal(e)) {
        cfg.connectionRateLimiter().onFailure();
      }
      long backoffMs = backoffMillis(attempt, ThreadLocalRandom.current().nextDouble());
      log.warn("{} reconnect attempt {} failed ({}); retrying in {}ms", cfg.id(), next, e.getMessage(), backoffMs);
      // Surface the failure to the listener so we can count per-link reconnect failures
      // separately from the eventual successful reconnect that fires onConnected.
      try {
        cfg.lifecycleListener().onReconnectFailed(cfg.id(), next, e);
      } catch (RuntimeException re) {
        log.warn("{} listener.onReconnectFailed threw: {}", cfg.id(), re.getMessage());
      }
      try {
        // Still the same reconnect: the flag stays set across the wait.
        cfg.scheduler().schedule(() -> reconnectAttempt(attempt + 1),
            backoffMs, TimeUnit.MILLISECONDS);
        retryScheduled = true;
      } catch (RejectedExecutionException rejected) {
        log.debug("{} retry not scheduled (scheduler shut down)", cfg.id());
      }
    } finally {
      if (!released && !retryScheduled) reconnecting.set(false);
    }
  }

  /**
   * Jittered exponential backoff delay (ms) for a reconnect attempt. The base grows exponentially
   * (1s, 2s, 4s, … capped at 60s) and equal-jitter — {@code base/2 + rand[0, base/2)} — is applied
   * so that many links failing together do NOT reconnect in lockstep (the lockstep was what let a
   * synchronized retry wave breach the exchange's per-IP connection-rate limit). Pure and
   * deterministic in {@code rand01} so it is unit-testable.
   *
   * @param attempt zero-based retry attempt
   * @param rand01 a value in {@code [0, 1)}, e.g. {@link ThreadLocalRandom#nextDouble()}
   * @return delay in milliseconds, always within {@code [base/2, base]} and {@code <= 60000}
   */
  static long backoffMillis(int attempt, double rand01) {
    long base = Math.min(60_000L, 1_000L << Math.min(Math.max(attempt, 0), 6));
    long half = base / 2;
    return half + (long) (rand01 * half);
  }
}
