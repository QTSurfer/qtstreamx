package com.qtsurfer.qtstreamx.link;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.qtsurfer.qtstreamx.core.client.StreamClient;
import com.qtsurfer.qtstreamx.core.model.FundingRate;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.model.Kline;
import com.qtsurfer.qtstreamx.core.model.Ticker;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Covers the zombie-recovery surface added to {@link LinkManager}: the {@code messagesReceived}
 * counter (the watchdog's stall signal) and {@code reconnectAll()} (the recovery action).
 */
class LinkManagerReconnectAllTest {

  private static final Set<Instrument> INSTRUMENTS =
      Set.of(new Instrument("BTC", "USDT"), new Instrument("ETH", "USDT"));

  private ScheduledExecutorService scheduler;

  @BeforeEach
  void setUp() {
    scheduler = Executors.newScheduledThreadPool(2);
  }

  @AfterEach
  void tearDown() {
    scheduler.shutdownNow();
  }

  @Test
  void messages_received_increments_on_inbound_kline() {
    RecordingFactory factory = new RecordingFactory();
    List<Kline> received = new CopyOnWriteArrayList<>();
    LinkManager mgr =
        new LinkManager(config(factory), new StubCache(), scheduler).onKline(received::add);

    mgr.start();
    await().atMost(Duration.ofSeconds(5)).until(() -> mgr.connectedLinkCount() == 1);
    assertThat(mgr.messagesReceived()).isZero();

    // Drive an inbound kline through the wired sink, as a live WS frame would.
    StubStreamClient client = factory.clients.get(0);
    Instrument any = client.klineSubscriptions.keySet().iterator().next();
    client.klineSubscriptions.get(any).accept(sampleKline(any));

    assertThat(mgr.messagesReceived()).isEqualTo(1);
    assertThat(received).hasSize(1);
    mgr.close();
  }

  @Test
  void last_event_micros_tracks_inbound_event_timestamp() {
    RecordingFactory factory = new RecordingFactory();
    LinkManager mgr = new LinkManager(config(factory), new StubCache(), scheduler).onKline(k -> {});

    mgr.start();
    await().atMost(Duration.ofSeconds(5)).until(() -> mgr.connectedLinkCount() == 1);
    assertThat(mgr.lastEventMicros()).isZero();

    StubStreamClient client = factory.clients.get(0);
    Instrument any = client.klineSubscriptions.keySet().iterator().next();
    client.klineSubscriptions.get(any).accept(sampleKline(any));

    // sampleKline carries open-time 1_000 µs — captured verbatim as the freshest event ts.
    assertThat(mgr.lastEventMicros()).isEqualTo(1_000L);
    mgr.close();
  }

  @Test
  void reconnectAll_forces_each_link_to_drop_and_reopen() {
    RecordingFactory factory = new RecordingFactory();
    LinkManager mgr = new LinkManager(config(factory), new StubCache(), scheduler).onKline(k -> {});

    mgr.start();
    await().atMost(Duration.ofSeconds(5)).until(() -> mgr.connectedLinkCount() == 1);
    int beforeClients = factory.clients.size();
    StubStreamClient original = factory.clients.get(0);

    mgr.reconnectAll();

    // A fresh client must be built (factory re-invoked) and the stale one closed.
    await().atMost(Duration.ofSeconds(5)).until(() -> factory.clients.size() > beforeClients);
    await().atMost(Duration.ofSeconds(5)).until(() -> mgr.connectedLinkCount() == 1);
    StubStreamClient replacement = factory.clients.get(factory.clients.size() - 1);
    assertThat(replacement).isNotSameAs(original);
    assertThat(replacement.isConnected()).isTrue();
    assertThat(replacement.klineSubscriptions).hasSize(INSTRUMENTS.size());
    mgr.close();
  }

  @Test
  void a_silent_link_is_reconnected_by_the_watchdog_without_reconnectAll() {
    // The failure reconnectAll() cannot self-trigger on: nothing calls it here. This is the
    // per-link path — checkSilentLinks() finding a link that is connected but has delivered
    // nothing, and reconnecting just that link on its own, without waiting on the coarser
    // manager-level zombie detection to notice.
    RecordingFactory factory = new RecordingFactory();
    LinkManager mgr =
        new LinkManager(configWithSilenceThreshold(factory, Duration.ofMillis(150)), new StubCache(), scheduler)
            .onKline(k -> {});

    mgr.start();
    await().atMost(Duration.ofSeconds(5)).until(() -> mgr.connectedLinkCount() == 1);
    int beforeClients = factory.clients.size();
    StubStreamClient original = factory.clients.get(0);

    // Never deliver a message on `original` — the link is connected but silent, exactly the
    // isSilent() case, and never disconnects, so onDisconnect-driven reconnect never fires.

    // checkSilentLinks() floors its check period at 5s regardless of how short the configured
    // threshold is (LinkManager: periodMs = max(5_000, threshold.toMillis()/3)), and its first
    // pass only fires after that same period — so the earliest a 150ms threshold can possibly be
    // caught is ~5s in, not sooner. Budget comfortably past that, not exactly at it.
    await()
        .atMost(Duration.ofSeconds(10))
        .until(() -> factory.clients.size() > beforeClients);
    await().atMost(Duration.ofSeconds(5)).until(() -> mgr.connectedLinkCount() == 1);
    StubStreamClient replacement = factory.clients.get(factory.clients.size() - 1);
    assertThat(replacement).isNotSameAs(original);
    assertThat(replacement.isConnected()).isTrue();
    mgr.close();
  }

  // --------------------------------------------------------------------------

  private LinkManager.Configuration config(RecordingFactory factory) {
    return new LinkManager.Configuration(
        "binance-spot",
        WsLimits.binanceSpot(),
        Set.of(Link.Subscription.KLINE),
        "1s",
        0, // targetStreamsPerLink → default (single link for two instruments)
        0, // streamsPerInstrument → default 1
        Duration.ofHours(1), // never auto-refresh during the test
        Duration.ZERO, // no inter-link jitter — fast reconnect in the test
        factory);
  }

  private LinkManager.Configuration configWithSilenceThreshold(
      RecordingFactory factory, Duration silenceThreshold) {
    return new LinkManager.Configuration(
        "binance-spot",
        WsLimits.binanceSpot(),
        Set.of(Link.Subscription.KLINE),
        "1s",
        0, // targetStreamsPerLink → default (single link for two instruments)
        0, // streamsPerInstrument → default 1
        Duration.ofHours(1), // never auto-refresh during the test
        Duration.ZERO, // no inter-link jitter — fast reconnect in the test
        factory,
        LinkLifecycleListener.NOOP,
        false, // randomizeLinkGrouping
        silenceThreshold);
  }

  private static Kline sampleKline(Instrument ins) {
    return new Kline(
        ins,
        "1s",
        BigDecimal.ONE,
        BigDecimal.ONE,
        BigDecimal.ONE,
        BigDecimal.ONE,
        BigDecimal.ONE,
        BigDecimal.ONE,
        1L,
        true,
        1_000L,
        2_000L);
  }

  /** Builds (and records) a new {@link StubStreamClient} per bucket so reconnects are observable. */
  static final class RecordingFactory implements Function<Set<Instrument>, StreamClient> {
    final List<StubStreamClient> clients = new CopyOnWriteArrayList<>();

    @Override
    public StreamClient apply(Set<Instrument> bucket) {
      StubStreamClient c = new StubStreamClient();
      clients.add(c);
      return c;
    }
  }

  static final class StubCache implements InstrumentsCache {
    @Override
    public String exchangeKey() {
      return "binance-spot";
    }

    @Override
    public CompletionStage<Set<Instrument>> refresh() {
      return CompletableFuture.completedFuture(INSTRUMENTS);
    }

    @Override
    public Set<Instrument> snapshot() {
      return INSTRUMENTS;
    }

    @Override
    public boolean isLoaded() {
      return true;
    }
  }

  /** Minimal StreamClient double recording subscriptions and connect/close state. */
  static final class StubStreamClient implements StreamClient {
    final java.util.Map<Instrument, Consumer<Ticker>> tickerSubscriptions = new ConcurrentHashMap<>();
    final java.util.Map<Instrument, Consumer<Kline>> klineSubscriptions = new ConcurrentHashMap<>();
    final java.util.Map<Instrument, Consumer<FundingRate>> fundingSubscriptions =
        new ConcurrentHashMap<>();
    private final AtomicBoolean connected = new AtomicBoolean();
    volatile Runnable disconnectHandler;

    @Override
    public void connect() {
      connected.set(true);
    }

    @Override
    public void subscribeTicker(Instrument instrument, Consumer<Ticker> handler) {
      tickerSubscriptions.put(instrument, handler);
    }

    @Override
    public void subscribeKline(Instrument instrument, String interval, Consumer<Kline> handler) {
      klineSubscriptions.put(instrument, handler);
    }

    @Override
    public void subscribeFundingRate(Instrument instrument, Consumer<FundingRate> handler) {
      fundingSubscriptions.put(instrument, handler);
    }

    @Override
    public boolean isConnected() {
      return connected.get();
    }

    @Override
    public void close() {
      connected.set(false);
    }

    @Override
    public void onDisconnect(Runnable handler) {
      this.disconnectHandler = handler;
    }
  }
}
