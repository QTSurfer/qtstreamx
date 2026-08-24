package com.qtsurfer.qtstreamx.link;

import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.core.client.StreamClient;
import com.qtsurfer.qtstreamx.core.model.FundingRate;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.model.Kline;
import com.qtsurfer.qtstreamx.core.model.Ticker;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Per-link market-data accounting, and the silence it makes visible.
 *
 * <p>A WebSocket that is open, answering pings and delivering nothing reports {@code isConnected()
 * == true} indefinitely. Every instrument in that link's bucket stops being captured, with no
 * error, no disconnect and no reconnect — the only downstream symptom is data that quietly isn't
 * there. These tests pin the distinction between "connected" and "delivering".
 */
class LinkSilenceTest {

  private static final Instrument BTC = new Instrument("BTC", "USDT");
  private static final Instrument ETH = new Instrument("ETH", "USDT");

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
  void counts_messages_and_still_delivers_them_downstream() throws Exception {
    List<Kline> downstream = new ArrayList<>();
    StubStreamClient stub = new StubStreamClient();
    Link link = buildLink(stub, downstream::add);
    link.connect(Set.of(BTC, ETH));

    assertThat(link.messageCount()).isZero();
    assertThat(link.sinceLastMessage()).isEmpty();

    stub.klineSubscriptions.get(BTC).accept(kline(BTC));
    stub.klineSubscriptions.get(ETH).accept(kline(ETH));

    // The counter must be a wrapper, not a replacement: the sink still sees every message.
    assertThat(link.messageCount()).isEqualTo(2);
    assertThat(downstream).hasSize(2);
    assertThat(link.sinceLastMessage()).isPresent();
    link.close();
  }

  @Test
  void a_link_that_has_received_nothing_is_silent_once_it_has_been_up_long_enough()
      throws Exception {
    StubStreamClient stub = new StubStreamClient();
    Link link = buildLink(stub, k -> {});
    link.connect(Set.of(BTC));

    Thread.sleep(80);

    assertThat(link.isConnected()).isTrue();
    assertThat(link.isSilent(Duration.ofMillis(50))).isTrue();
    assertThat(link.sinceLastMessage()).isEmpty();
    link.close();
  }

  @Test
  void a_freshly_connected_link_is_not_silent() throws Exception {
    StubStreamClient stub = new StubStreamClient();
    Link link = buildLink(stub, k -> {});
    link.connect(Set.of(BTC));

    // It has received nothing, but it has had no time to. Reporting this would flag every
    // link that is merely starting — the normal case — instead of the broken one.
    assertThat(link.isSilent(Duration.ofSeconds(30))).isFalse();
    link.close();
  }

  @Test
  void a_link_delivering_data_is_not_silent() throws Exception {
    StubStreamClient stub = new StubStreamClient();
    Link link = buildLink(stub, k -> {});
    link.connect(Set.of(BTC));

    Thread.sleep(80);
    stub.klineSubscriptions.get(BTC).accept(kline(BTC));

    assertThat(link.isSilent(Duration.ofMillis(50))).isFalse();
    link.close();
  }

  @Test
  void a_link_that_goes_quiet_after_delivering_becomes_silent() throws Exception {
    StubStreamClient stub = new StubStreamClient();
    Link link = buildLink(stub, k -> {});
    link.connect(Set.of(BTC));
    stub.klineSubscriptions.get(BTC).accept(kline(BTC));

    assertThat(link.isSilent(Duration.ofMillis(50))).isFalse();
    Thread.sleep(80);

    // This is the zombie: still connected, message count frozen.
    assertThat(link.isConnected()).isTrue();
    assertThat(link.messageCount()).isEqualTo(1);
    assertThat(link.isSilent(Duration.ofMillis(50))).isTrue();
    link.close();
  }

  @Test
  void a_disconnected_link_is_not_reported_as_silent() throws Exception {
    StubStreamClient stub = new StubStreamClient();
    Link link = buildLink(stub, k -> {});
    link.connect(Set.of(BTC));
    Thread.sleep(80);
    stub.close();

    // A link that is down is a different fault with its own signal. Counting it here would
    // conflate "the socket dropped" with "the socket is lying to us".
    assertThat(link.isConnected()).isFalse();
    assertThat(link.isSilent(Duration.ofMillis(50))).isFalse();
    link.close();
  }

  @Test
  void message_count_survives_a_reconnect() throws Exception {
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
    Link link =
        new Link(
            new Link.Configuration(
                "link-reconnect-count",
                WsLimits.binanceSpot(),
                Set.of(Link.Subscription.KLINE),
                "1s",
                factory,
                t -> {},
                k -> {},
                f -> {},
                scheduler));
    link.connect(Set.of(BTC));
    first.klineSubscriptions.get(BTC).accept(kline(BTC));
    assertThat(link.messageCount()).isEqualTo(1);

    link.reconnectWithBackoff(0);
    Thread.sleep(200);
    second.klineSubscriptions.get(BTC).accept(kline(BTC));

    // Cumulative for the link's lifetime, not for the current socket: a link that reconnects
    // every few minutes must not look like it is always starting from zero.
    assertThat(link.messageCount()).isEqualTo(2);
    link.close();
  }

  // --------------------------------------------------------------------------

  private Link buildLink(StubStreamClient stub, Consumer<Kline> klineSink) {
    return new Link(
        new Link.Configuration(
            "link-silence",
            WsLimits.binanceSpot(),
            Set.of(Link.Subscription.KLINE),
            "1s",
            () -> stub,
            t -> {},
            klineSink,
            f -> {},
            scheduler));
  }

  private static Kline kline(Instrument ins) {
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
        System.currentTimeMillis() * 1000,
        System.currentTimeMillis() * 1000);
  }

  /** Minimal StreamClient double that keeps the handlers it was given so tests can drive them. */
  static final class StubStreamClient implements StreamClient {
    final Map<Instrument, Consumer<Ticker>> tickerSubscriptions = new ConcurrentHashMap<>();
    final Map<Instrument, Consumer<Kline>> klineSubscriptions = new ConcurrentHashMap<>();
    final Map<Instrument, Consumer<FundingRate>> fundingSubscriptions = new ConcurrentHashMap<>();
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

  @Test
  void names_its_instruments_for_the_log() throws Exception {
    StubStreamClient stub = new StubStreamClient();
    Link link = buildLink(stub, k -> {});
    link.connect(Set.of(BTC, ETH));

    // Sorted, so the same bucket always renders the same way and two nodes' lines can be diffed.
    assertThat(link.instrumentsForLog()).isEqualTo("BTC/USDT,ETH/USDT");
    link.close();
  }

  @Test
  void caps_the_instrument_list_so_one_line_cannot_run_away() throws Exception {
    StubStreamClient stub = new StubStreamClient();
    Link link = buildLink(stub, k -> {});
    Set<Instrument> many = new java.util.HashSet<>();
    for (int i = 0; i < 200; i++) many.add(new Instrument("C" + i, "USDT"));
    link.connect(many);

    String rendered = link.instrumentsForLog();
    assertThat(rendered).contains("(+80 more)");
    assertThat(rendered.split(",")).hasSize(121); // 120 symbols + the "… (+80 more)" tail
    link.close();
  }

  @Test
  void renders_a_bucket_the_link_does_not_own_yet() {
    // The startup partition map is logged before connect() hands the link its bucket. Asking the
    // link at that moment returns nothing — it genuinely owns nothing — which is how the first
    // version of this logging printed ten empty lists.
    assertThat(Link.renderInstruments(Set.of(BTC, ETH))).isEqualTo("BTC/USDT,ETH/USDT");
  }
}
