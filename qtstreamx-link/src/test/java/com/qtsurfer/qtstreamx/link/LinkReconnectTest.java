package com.qtsurfer.qtstreamx.link;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.qtsurfer.qtstreamx.core.client.StreamClient;
import com.qtsurfer.qtstreamx.core.model.FundingRate;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.model.Kline;
import com.qtsurfer.qtstreamx.core.model.Ticker;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
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
    volatile Runnable disconnectHandler;
    private final AtomicBoolean disconnectFired = new AtomicBoolean();

    @Override
    public void connect() {
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
