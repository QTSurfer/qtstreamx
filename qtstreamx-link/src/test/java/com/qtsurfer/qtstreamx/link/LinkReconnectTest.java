package com.qtsurfer.qtstreamx.link;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.qtsurfer.qtstreamx.core.client.StreamClient;
import com.qtsurfer.qtstreamx.core.model.FundingRate;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.model.Kline;
import com.qtsurfer.qtstreamx.core.model.Ticker;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LinkReconnectTest {

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
  void initial_connect_subscribes_every_instrument() throws Exception {
    StubStreamClient stub = new StubStreamClient();
    Link link = buildLink(stub);

    Set<Instrument> instruments =
        Set.of(new Instrument("BTC", "USDT"), new Instrument("ETH", "USDT"));
    link.connect(instruments);

    assertThat(stub.isConnected()).isTrue();
    assertThat(stub.klineSubscriptions).hasSize(2);
    assertThat(link.subscribedCount()).isEqualTo(2);
    link.close();
  }

  @Test
  void reconnect_rebuilds_stream_client_and_resubscribes() throws Exception {
    StubStreamClient first = new StubStreamClient();
    StubStreamClient second = new StubStreamClient();
    var factory =
        new java.util.function.Supplier<StreamClient>() {
          int n = 0;

          @Override
          public StreamClient get() {
            return n++ == 0 ? first : second;
          }
        };

    Link.Configuration cfg =
        new Link.Configuration(
            "link-test",
            WsLimits.binanceSpot(),
            Set.of(Link.Subscription.KLINE),
            "1s",
            factory,
            t -> {},
            k -> {},
            f -> {},
            scheduler);
    Link link = new Link(cfg);

    Set<Instrument> instruments =
        Set.of(new Instrument("BTC", "USDT"), new Instrument("ETH", "USDT"));
    link.connect(instruments);
    assertThat(first.isConnected()).isTrue();

    // Trigger reconnect — the next client (second) should be wired and subscribed.
    link.reconnectWithBackoff(0);

    await().atMost(Duration.ofSeconds(5)).until(second::isConnected);
    assertThat(second.klineSubscriptions).hasSize(2);

    link.close();
  }

  @Test
  void add_instrument_while_connected_subscribes_live() throws Exception {
    StubStreamClient stub = new StubStreamClient();
    Link link = buildLink(stub);
    link.connect(Set.of(new Instrument("BTC", "USDT")));

    link.addInstrument(new Instrument("SOL", "USDT"));
    assertThat(link.subscribedCount()).isEqualTo(2);
    assertThat(stub.klineSubscriptions).containsKey(new Instrument("SOL", "USDT"));
    link.close();
  }

  @Test
  void close_stops_new_reconnects() throws Exception {
    StubStreamClient stub = new StubStreamClient();
    Link link = buildLink(stub);
    link.connect(Set.of(new Instrument("BTC", "USDT")));

    link.close();
    link.reconnectWithBackoff(0);
    // With the link closed, reconnect must not produce a new connected client.
    // The existing stub stays but won't be replaced.
    assertThat(link.isConnected()).isFalse();
  }

  @Test
  void disconnect_callback_triggers_reconnect() throws Exception {
    StubStreamClient first = new StubStreamClient();
    StubStreamClient second = new StubStreamClient();
    var factory =
        new java.util.function.Supplier<StreamClient>() {
          int n = 0;

          @Override
          public StreamClient get() {
            return n++ == 0 ? first : second;
          }
        };

    Link.Configuration cfg =
        new Link.Configuration(
            "link-drop",
            WsLimits.binanceSpot(),
            Set.of(Link.Subscription.KLINE),
            "1s",
            factory,
            t -> {},
            k -> {},
            f -> {},
            scheduler);
    Link link = new Link(cfg);

    link.connect(Set.of(new Instrument("BTC", "USDT"), new Instrument("ETH", "USDT")));
    assertThat(first.isConnected()).isTrue();
    assertThat(first.disconnectHandler).isNotNull();

    // Simulate a WS drop on the current client — Link should reopen via factory.
    first.fireDisconnect();

    await().atMost(Duration.ofSeconds(5)).until(second::isConnected);
    assertThat(second.klineSubscriptions).hasSize(2);
    link.close();
  }

  @Test
  void stale_disconnect_after_rotate_is_ignored() throws Exception {
    StubStreamClient first = new StubStreamClient();
    StubStreamClient second = new StubStreamClient();
    StubStreamClient third = new StubStreamClient();
    var factory =
        new java.util.function.Supplier<StreamClient>() {
          int n = 0;

          @Override
          public StreamClient get() {
            return switch (n++) {
              case 0 -> first;
              case 1 -> second;
              default -> third;
            };
          }
        };

    Link.Configuration cfg =
        new Link.Configuration(
            "link-stale",
            WsLimits.binanceSpot(),
            Set.of(Link.Subscription.KLINE),
            "1s",
            factory,
            t -> {},
            k -> {},
            f -> {},
            scheduler);
    Link link = new Link(cfg);
    link.connect(Set.of(new Instrument("BTC", "USDT")));

    // Manual rotate to second — now `first` is stale.
    link.reconnectWithBackoff(0);
    await().atMost(Duration.ofSeconds(5)).until(second::isConnected);

    // Late callback from the already-rotated first client must not kick off
    // another reconnect.
    first.fireDisconnect();

    // Wait a bit and confirm third never got constructed.
    Thread.sleep(300);
    assertThat(third.isConnected()).isFalse();
    link.close();
  }


  // --------------------------------------------------------------------------
  // Client leaks: every client a link opens must be closed by the link
  // --------------------------------------------------------------------------

  private static final Instrument BTC = new Instrument("BTC", "USDT");

  private Link link(String id, Supplier<StreamClient> factory, ScheduledExecutorService ex) {
    return new Link(
        new Link.Configuration(
            id,
            WsLimits.binanceSpot(),
            Set.of(Link.Subscription.KLINE),
            "1s",
            factory,
            t -> {},
            k -> {},
            f -> {},
            ex));
  }

  @Test
  void concurrent_reconnects_leave_exactly_one_client_open() throws Exception {
    List<StubStreamClient> made = new CopyOnWriteArrayList<>();
    CountDownLatch release = new CountDownLatch(1);
    Supplier<StreamClient> factory =
        () -> {
          StubStreamClient c = new StubStreamClient();
          if (!made.isEmpty()) c.connectGate = release; // only the replacements block
          made.add(c);
          return c;
        };
    Link link = link("link-race", factory, scheduler);
    link.connect(Set.of(BTC));
    assertThat(link.openClients()).isEqualTo(1);

    // Four triggers (renewal, drop, silence watchdog, reconnect-all) landing together.
    ExecutorService pool = Executors.newFixedThreadPool(4);
    List<Future<?>> racers = new ArrayList<>();
    for (int i = 0; i < 4; i++) racers.add(pool.submit(() -> link.reconnectWithBackoff(0)));
    await().atMost(Duration.ofSeconds(5)).until(() -> made.size() >= 2);
    Thread.sleep(300); // long enough for a second racer to open its own client, if it is allowed to
    release.countDown();
    for (Future<?> f : racers) f.get(5, TimeUnit.SECONDS);
    pool.shutdownNow();

    assertThat(made.stream().filter(StubStreamClient::isConnected).count())
        .as("clients still connected after the racing reconnects settled")
        .isEqualTo(1);
    assertThat(link.openClients()).isEqualTo(1);
    assertThat(made.get(0).closeCount()).as("the replaced client").isEqualTo(1);

    link.close();
    assertThat(link.openClients()).isZero();
    assertThat(made.stream().filter(StubStreamClient::isConnected).count()).isZero();
  }

  @Test
  void renewal_timers_do_not_accumulate_across_reconnects() throws Exception {
    ScheduledThreadPoolExecutor ex = new ScheduledThreadPoolExecutor(2);
    ex.setRemoveOnCancelPolicy(true);
    try {
      List<StubStreamClient> made = new CopyOnWriteArrayList<>();
      Supplier<StreamClient> factory =
          () -> {
            StubStreamClient c = new StubStreamClient();
            made.add(c);
            return c;
          };
      Link link = link("link-timers", factory, ex);
      link.connect(Set.of(BTC));

      // Three unplanned reconnects (dropped socket). Each used to leave one more renewal timer.
      for (int i = 1; i <= 3; i++) {
        made.get(i - 1).fireDisconnect();
        int expected = i + 1;
        await().atMost(Duration.ofSeconds(5)).until(() -> made.size() == expected && made.get(expected - 1).isConnected());
        // the 100ms hand-off task has run and the renewal is armed again
        await().atMost(Duration.ofSeconds(5)).until(() -> ex.getQueue().size() == 1);
      }

      assertThat(ex.getQueue()).as("pending renewal timers").hasSize(1);
      assertThat(link.openClients()).isEqualTo(1);
      link.close();
      assertThat(ex.getQueue()).as("timers left after close").isEmpty();
    } finally {
      ex.shutdownNow();
    }
  }

  @Test
  void close_while_a_reconnect_is_in_flight_leaves_no_client_open() throws Exception {
    List<StubStreamClient> made = new CopyOnWriteArrayList<>();
    CountDownLatch release = new CountDownLatch(1);
    Supplier<StreamClient> factory =
        () -> {
          StubStreamClient c = new StubStreamClient();
          if (!made.isEmpty()) c.connectGate = release;
          made.add(c);
          return c;
        };
    Link link = link("link-close-race", factory, scheduler);
    link.connect(Set.of(BTC));

    ExecutorService pool = Executors.newSingleThreadExecutor();
    Future<?> reconnect = pool.submit(() -> link.reconnectWithBackoff(0));
    await().atMost(Duration.ofSeconds(5)).until(() -> made.size() == 2);
    link.close(); // the replacement is not installed yet, so close() cannot see it
    release.countDown();
    reconnect.get(5, TimeUnit.SECONDS);
    pool.shutdownNow();

    assertThat(link.openClients()).isZero();
    assertThat(made.stream().filter(StubStreamClient::isConnected).count()).isZero();
  }

  @Test
  void a_client_whose_connect_fails_is_closed() throws Exception {
    StubStreamClient broken = new StubStreamClient();
    broken.failConnect = true;
    Link link = link("link-broken", () -> broken, scheduler);

    assertThatThrownBy(() -> link.connect(Set.of(BTC))).isInstanceOf(IllegalStateException.class);

    assertThat(broken.closeCount()).isEqualTo(1);
    assertThat(link.openClients()).isZero();
  }

  @Test
  void a_failed_reconnect_attempt_closes_its_client_before_retrying() throws Exception {
    StubStreamClient first = new StubStreamClient();
    StubStreamClient broken = new StubStreamClient();
    broken.failConnect = true;
    StubStreamClient third = new StubStreamClient();
    List<StubStreamClient> order = List.of(first, broken, third);
    AtomicInteger n = new AtomicInteger();
    Link link = link("link-retry", () -> order.get(n.getAndIncrement()), scheduler);
    link.connect(Set.of(BTC));

    link.reconnectWithBackoff(0);

    await().atMost(Duration.ofSeconds(10)).until(third::isConnected);
    assertThat(broken.closeCount()).as("the attempt that failed").isEqualTo(1);
    assertThat(link.openClients()).isEqualTo(1);
    link.close();
  }

  // --------------------------------------------------------------------------

  private Link buildLink(StubStreamClient stub) {
    Link.Configuration cfg =
        new Link.Configuration(
            "link-test",
            WsLimits.binanceSpot(),
            Set.of(Link.Subscription.KLINE),
            "1s",
            () -> stub,
            t -> {},
            k -> {},
            f -> {},
            scheduler);
    return new Link(cfg);
  }

  /** Minimal StreamClient double that records subscribe/connect calls. */
  static final class StubStreamClient implements StreamClient {
    final java.util.Map<Instrument, Consumer<Ticker>> tickerSubscriptions =
        new ConcurrentHashMap<>();
    final java.util.Map<Instrument, Consumer<Kline>> klineSubscriptions =
        new ConcurrentHashMap<>();
    final java.util.Map<Instrument, Consumer<FundingRate>> fundingSubscriptions =
        new ConcurrentHashMap<>();
    private final AtomicBoolean connected = new AtomicBoolean();
    private final AtomicInteger connectCount = new AtomicInteger();
    private final AtomicInteger closeCount = new AtomicInteger();
    volatile Runnable disconnectHandler;
    /** When set, connect() blocks until it is released. */
    volatile CountDownLatch connectGate;
    /** When set, connect() fails the way a refused handshake does. */
    volatile boolean failConnect;
    private final AtomicBoolean disconnectFired = new AtomicBoolean();

    int closeCount() {
      return closeCount.get();
    }

    @Override
    public void connect() {
      CountDownLatch gate = connectGate;
      if (gate != null) {
        try {
          gate.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("interrupted while connecting", e);
        }
      }
      if (failConnect) throw new IllegalStateException("handshake refused");
      connected.set(true);
      connectCount.incrementAndGet();
      disconnectFired.set(false);
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
      closeCount.incrementAndGet();
      connected.set(false);
    }

    @Override
    public void onDisconnect(Runnable handler) {
      this.disconnectHandler = handler;
    }

    /** Test hook: simulate a WS dropout; fires the registered handler once. */
    void fireDisconnect() {
      connected.set(false);
      if (disconnectHandler != null && disconnectFired.compareAndSet(false, true)) {
        disconnectHandler.run();
      }
    }
  }
}
