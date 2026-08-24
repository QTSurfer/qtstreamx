package com.qtsurfer.qtstreamx.canary;

import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.core.client.StreamClient;
import com.qtsurfer.qtstreamx.core.client.StreamClientConfig;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.discovery.binance.BinanceInstrumentsCache;
import com.qtsurfer.qtstreamx.exchange.bybit.BybitInstrumentsCache;
import com.qtsurfer.qtstreamx.link.InstrumentsCache;
import com.qtsurfer.qtstreamx.ws.jdk.GzipAwareJdkWebSocketClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Live capture smoke test — one parameterized case per exchange. Connects to the REAL exchange
 * WebSocket, subscribes a representative SAMPLE of instruments discovered via the exchange's REST
 * instrument list, and asserts the capture stack actually delivers ticker + kline + funding (perps)
 * for (almost) all of them within a bounded window. Self-contained: only the exchange's REST +
 * WS — no NATS / QDB / infra.
 *
 * <p>Designed to catch capture-coverage regressions like a partial-funding bug (bybit-linear funding
 * emitted for only ~half the perp universe): sampling many perps and asserting per-instrument funding
 * coverage makes such a bug fail the build, instead of silently shipping a hole downstream.
 *
 * <p>Tagged {@code it} and EXCLUDED from the default {@code test} run (live network). Run with
 * {@code ./gradlew :qtstreamx-canary:test -Pit}.
 */
@Tag("it")
class ExchangeCaptureIT {

  private static final Logger log = LoggerFactory.getLogger(ExchangeCaptureIT.class);

  /** How many instruments to sample (spread across the sorted universe). */
  private static final int SAMPLE = 40;
  /** Bounded capture window; we early-exit as soon as the coverage targets are met. */
  private static final Duration WINDOW = Duration.ofSeconds(90);
  /** Assertion strategy, built to be robust against the dead / pre-launch / illiquid instruments a
   *  spread sample of a large universe inevitably hits. Ticker + kline are checked only as STREAM
   *  LIVENESS — the feed connected and is producing. The HEADLINE check is funding-tracks-ticker: a
   *  perp the feed never snapshots is absent from BOTH ticker and funding, so the ratio is immune to
   *  dead instruments and only drops when funding lags a *captured* ticker — exactly the partial-funding
   *  hole. NOTE: this exercises the qtstreamx CLIENT in isolation; the integration-level backpressure
   *  bug (a downstream consumer stalling the WS reader) needs a publisher-level harness — out of scope. */
  private static final double TICKER_LIVENESS = 0.50; // feed connected + snapshotted >= half the sample
  private static final double KLINE_LIVENESS = 0.10; // >=10% emitted a kline = stream alive (trade-driven)
  private static final double FUNDING_TRACKS_TICKER = 0.90; // funding covers >= 90% of ticker-covered perps

  record Case(String exchangeKey, InstrumentsCache cache, String klineInterval, boolean derivatives) {
    @Override
    public String toString() {
      return exchangeKey;
    }
  }

  static Stream<Case> cases() {
    return Stream.of(
        new Case(
            "bybit-linear",
            new BybitInstrumentsCache(BybitInstrumentsCache.Category.LINEAR),
            "1",
            true),
        new Case(
            "bybit-spot",
            new BybitInstrumentsCache(BybitInstrumentsCache.Category.SPOT),
            "1",
            false),
        new Case(
            "binance-futures",
            new BinanceInstrumentsCache(BinanceInstrumentsCache.Market.FUTURES_USDT),
            "1m",
            true),
        new Case(
            "binance-spot",
            new BinanceInstrumentsCache(BinanceInstrumentsCache.Market.SPOT),
            "1m",
            false));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("cases")
  void capturesSampleOfInstruments(Case c) throws Exception {
    // 1. Discover the live universe and pick a deterministic, evenly-spread sample.
    Set<Instrument> universe = c.cache().refresh().toCompletableFuture().get(30, TimeUnit.SECONDS);
    assertThat(universe).as("%s instrument universe (REST)", c.exchangeKey()).isNotEmpty();
    List<Instrument> sorted =
        universe.stream().sorted(Comparator.comparing(Instrument::symbol)).toList();
    int step = Math.max(1, sorted.size() / SAMPLE);
    List<Instrument> sample = new ArrayList<>();
    for (int i = 0; i < sorted.size() && sample.size() < SAMPLE; i += step) {
      sample.add(sorted.get(i));
    }
    List<Instrument> perps = sample.stream().filter(Instrument::isDerivative).toList();
    log.info("{}: universe={} sample={} perps={}", c.exchangeKey(), universe.size(), sample.size(), perps.size());

    // 2. Collect the DISTINCT instruments seen per stream type (snapshot or update).
    Set<String> tickers = ConcurrentHashMap.newKeySet();
    Set<String> klines = ConcurrentHashMap.newKeySet();
    Set<String> frates = ConcurrentHashMap.newKeySet();

    StreamClient client =
        CaptureMain.buildClient(
            c.exchangeKey(), StreamClientConfig.withDefaults(GzipAwareJdkWebSocketClient::new));
    AtomicBoolean done = new AtomicBoolean(false);
    ScheduledExecutorService reconnect = Executors.newSingleThreadScheduledExecutor();
    // Stay resilient to mid-window drops (bybit-linear churns) so a dropped socket doesn't look
    // like a capture hole: on disconnect, reconnect — the client replays its subscriptions.
    client.onDisconnect(
        () -> {
          if (done.get()) return;
          reconnect.schedule(
              () -> {
                if (done.get()) return;
                try {
                  client.connect();
                } catch (Exception e) {
                  log.warn("{} reconnect failed: {}", c.exchangeKey(), e.getMessage());
                }
              },
              2,
              TimeUnit.SECONDS);
        });

    try (client) {
      for (Instrument ins : sample) {
        client.subscribeTicker(ins, t -> tickers.add(ins.symbol()));
        try {
          client.subscribeKline(ins, c.klineInterval(), k -> klines.add(ins.symbol()));
        } catch (UnsupportedOperationException ignored) {
          // exchange has no kline stream for this market — fine.
        }
        if (c.derivatives() && ins.isDerivative()) {
          try {
            client.subscribeFundingRate(ins, f -> frates.add(ins.symbol()));
          } catch (UnsupportedOperationException ignored) {
            // not a perp / no funding — fine.
          }
        }
      }
      client.connect();

      long deadline = System.currentTimeMillis() + WINDOW.toMillis();
      while (System.currentTimeMillis() < deadline) {
        boolean tickOk = tickers.size() >= sample.size() * TICKER_LIVENESS;
        boolean frOk = perps.isEmpty() || frates.size() >= tickers.size() * FUNDING_TRACKS_TICKER;
        if (tickOk && frOk) break;
        Thread.sleep(1000);
      }
    } finally {
      done.set(true);
      reconnect.shutdownNow();
    }

    log.info(
        "{}: coverage ticker={}/{} kline={}/{} funding={}/{}",
        c.exchangeKey(), tickers.size(), sample.size(), klines.size(), sample.size(),
        frates.size(), perps.size());

    // 3. Assertions. Ticker + kline are stream-liveness; funding-tracks-ticker is the headline.
    assertThat((double) tickers.size() / sample.size())
        .as("%s ticker stream liveness (%d/%d sampled)", c.exchangeKey(), tickers.size(), sample.size())
        .isGreaterThanOrEqualTo(TICKER_LIVENESS);
    assertThat((double) klines.size() / sample.size())
        .as("%s kline stream liveness (%d/%d sampled emitted a kline)",
            c.exchangeKey(), klines.size(), sample.size())
        .isGreaterThanOrEqualTo(KLINE_LIVENESS);
    if (!perps.isEmpty() && !tickers.isEmpty()) {
      // Dead-instrument-immune: a perp the feed never snapshots is absent from BOTH ticker and
      // funding, so this ratio only drops when funding lags a *captured* ticker — the funding hole.
      assertThat((double) frates.size() / tickers.size())
          .as("%s funding must track ticker (%d funding / %d ticker)",
              c.exchangeKey(), frates.size(), tickers.size())
          .isGreaterThanOrEqualTo(FUNDING_TRACKS_TICKER);
    }
  }
}
