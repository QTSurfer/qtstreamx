package com.qtsurfer.qtstreamx.link;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.qtsurfer.qtstreamx.core.client.StreamClient;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LinkLifecycleListenerTest {

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
  void initial_connect_fires_listener_with_zero_outage() throws Exception {
    RecordingListener listener = new RecordingListener();
    LinkReconnectTest.StubStreamClient stub = new LinkReconnectTest.StubStreamClient();
    Link link = buildLink("link-init", () -> stub, listener);

    link.connect(Set.of(new Instrument("BTC", "USDT"), new Instrument("ETH", "USDT")));

    assertThat(listener.connectedEvents).hasSize(1);
    ConnectedEvent ev = listener.connectedEvents.get(0);
    assertThat(ev.linkId).isEqualTo("link-init");
    assertThat(ev.instrumentCount).isEqualTo(2);
    assertThat(ev.outage).isEqualTo(Duration.ZERO);
    assertThat(listener.disconnectedEvents).isEmpty();
    link.close();
  }

  @Test
  void disconnect_then_reconnect_emits_drop_and_reconnect_with_outage() throws Exception {
    RecordingListener listener = new RecordingListener();
    LinkReconnectTest.StubStreamClient first = new LinkReconnectTest.StubStreamClient();
    LinkReconnectTest.StubStreamClient second = new LinkReconnectTest.StubStreamClient();
    AtomicInteger n = new AtomicInteger();
    Link link =
        buildLink(
            "link-drop",
            () -> n.getAndIncrement() == 0 ? first : second,
            listener);

    link.connect(Set.of(new Instrument("BTC", "USDT")));
    // Initial connect — one onConnected, zero outage.
    assertThat(listener.connectedEvents).hasSize(1);

    // Simulate WS drop. Link schedules reconnect ~100ms out; on the second client
    // success, onConnected must fire again with a non-zero outage value.
    first.fireDisconnect();

    await().atMost(Duration.ofSeconds(5)).until(() -> listener.connectedEvents.size() == 2);

    assertThat(listener.disconnectedEvents).containsExactly("link-drop");
    ConnectedEvent reconnect = listener.connectedEvents.get(1);
    assertThat(reconnect.linkId).isEqualTo("link-drop");
    assertThat(reconnect.outage).isPositive();
    link.close();
  }

  @Test
  void reconnect_failure_emits_onReconnectFailed_with_attempt_counter() throws Exception {
    RecordingListener listener = new RecordingListener();
    AtomicInteger n = new AtomicInteger();
    LinkReconnectTest.StubStreamClient finalClient = new LinkReconnectTest.StubStreamClient();

    Link link =
        buildLink(
            "link-fail",
            () -> {
              int call = n.getAndIncrement();
              // First call = initial connect (success). Calls 1, 2 = reconnect (throws).
              // Call 3 = eventual reconnect success so the test ends in a defined state.
              if (call == 0 || call >= 3) return finalClient;
              throw new RuntimeException("simulated boom-" + call);
            },
            listener);

    link.connect(Set.of(new Instrument("BTC", "USDT")));
    // Trigger the failing reconnect cycle. Backoff is exponential starting at 1s,
    // capped at 60s — the second reconnect retries at ~1s, third at ~2s.
    link.reconnectWithBackoff(0);

    await().atMost(Duration.ofSeconds(15)).until(() -> listener.reconnectFailures.size() >= 2);

    assertThat(listener.reconnectFailures.get(0).attempt).isEqualTo(1);
    assertThat(listener.reconnectFailures.get(0).cause).hasMessageContaining("simulated boom-1");
    assertThat(listener.reconnectFailures.get(1).attempt).isEqualTo(2);
    link.close();
  }

  // --------------------------------------------------------------------------

  private Link buildLink(
      String id, java.util.function.Supplier<StreamClient> factory, LinkLifecycleListener listener) {
    Link.Configuration cfg =
        new Link.Configuration(
            id,
            WsLimits.binanceSpot(),
            Set.of(Link.Subscription.KLINE),
            "1s",
            factory,
            t -> {},
            k -> {},
            f -> {},
            scheduler,
            listener);
    return new Link(cfg);
  }

  // Event records — kept dumb so tests just assert on fields directly.

  record ConnectedEvent(String linkId, int instrumentCount, Duration outage) {}

  record FailedEvent(String linkId, int attempt, Throwable cause) {}

  static final class RecordingListener implements LinkLifecycleListener {
    final List<ConnectedEvent> connectedEvents = new ArrayList<>();
    final List<String> disconnectedEvents = new ArrayList<>();
    final List<FailedEvent> reconnectFailures = new ArrayList<>();

    @Override
    public synchronized void onConnected(String linkId, int instrumentCount, Duration outage) {
      connectedEvents.add(new ConnectedEvent(linkId, instrumentCount, outage));
    }

    @Override
    public synchronized void onDisconnected(String linkId) {
      disconnectedEvents.add(linkId);
    }

    @Override
    public synchronized void onReconnectFailed(String linkId, int attempt, Throwable cause) {
      reconnectFailures.add(new FailedEvent(linkId, attempt, cause));
    }
  }
}
