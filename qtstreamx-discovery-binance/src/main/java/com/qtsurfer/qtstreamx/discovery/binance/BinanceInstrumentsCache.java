package com.qtsurfer.qtstreamx.discovery.binance;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.link.InstrumentsCache;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hits Binance {@code /exchangeInfo} REST to discover tradable instruments.
 *
 * <p>Pass the appropriate base URL on construction:
 *
 * <ul>
 *   <li>spot: {@code https://api.binance.com/api/v3/exchangeInfo}
 *   <li>USDT-margined futures: {@code https://fapi.binance.com/fapi/v1/exchangeInfo}
 *   <li>coin-margined futures: {@code https://dapi.binance.com/dapi/v1/exchangeInfo}
 * </ul>
 *
 * <p>Filters for {@code status = "TRADING"} (spot) or {@code contractStatus / status =
 * "TRADING"} (futures) so delisted symbols don't show up.
 */
public final class BinanceInstrumentsCache implements InstrumentsCache {

  private static final Logger log = LoggerFactory.getLogger(BinanceInstrumentsCache.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public enum Market {
    SPOT("binance-spot", "https://api.binance.com/api/v3/exchangeInfo", false),
    FUTURES_USDT("binance-futures-usdt", "https://fapi.binance.com/fapi/v1/exchangeInfo", true),
    FUTURES_COIN("binance-futures-coin", "https://dapi.binance.com/dapi/v1/exchangeInfo", true);

    final String exchangeKey;
    final String url;
    final boolean futures;

    Market(String exchangeKey, String url, boolean futures) {
      this.exchangeKey = exchangeKey;
      this.url = url;
      this.futures = futures;
    }
  }

  private final Market market;
  private final HttpClient http;
  private final AtomicReference<Set<Instrument>> cached = new AtomicReference<>(Set.of());
  private final AtomicBoolean loaded = new AtomicBoolean(false);

  public BinanceInstrumentsCache(Market market) {
    this(
        market,
        HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .version(HttpClient.Version.HTTP_2)
            .build());
  }

  public BinanceInstrumentsCache(Market market, HttpClient http) {
    this.market = market;
    this.http = http;
  }

  @Override
  public String exchangeKey() {
    return market.exchangeKey;
  }

  @Override
  public CompletionStage<Set<Instrument>> refresh() {
    HttpRequest req =
        HttpRequest.newBuilder(URI.create(market.url))
            .GET()
            .timeout(Duration.ofSeconds(30))
            .header("Accept", "application/json")
            .build();
    return http
        .sendAsync(req, HttpResponse.BodyHandlers.ofString())
        .thenApply(
            resp -> {
              if (resp.statusCode() != 200) {
                throw new RuntimeException(
                    market.exchangeKey + " exchangeInfo HTTP " + resp.statusCode());
              }
              Set<Instrument> parsed = parse(resp.body(), market);
              cached.set(parsed);
              loaded.set(true);
              log.info("{} discovered {} instruments", market.exchangeKey, parsed.size());
              return parsed;
            });
  }

  @Override
  public Set<Instrument> snapshot() {
    return cached.get();
  }

  @Override
  public boolean isLoaded() {
    return loaded.get();
  }

  static Set<Instrument> parse(String json, Market market) {
    try {
      JsonNode root = MAPPER.readTree(json);
      JsonNode symbols = root.path("symbols");
      Set<Instrument> out = new HashSet<>(symbols.size());
      for (JsonNode s : symbols) {
        String base = s.path("baseAsset").asText(null);
        String quote = s.path("quoteAsset").asText(null);
        if (base == null || quote == null) continue;

        String status = s.path("status").asText("");
        // Futures: "status" field may be "TRADING" on USDT-M, "contractStatus" on some schema revisions.
        if (market.futures && status.isEmpty()) {
          status = s.path("contractStatus").asText("");
        }
        if (!"TRADING".equalsIgnoreCase(status)) continue;

        if (market.futures) {
          // Derivative — settle currency is one of quote (USDT-M) or base (coin-M).
          String contractType = s.path("contractType").asText("");
          if (!contractType.equalsIgnoreCase("PERPETUAL")) continue;
          String marginAsset = s.path("marginAsset").asText(quote);
          out.add(new Instrument(base, quote, marginAsset));
        } else {
          out.add(new Instrument(base, quote));
        }
      }
      return out;
    } catch (Exception e) {
      throw new RuntimeException("failed to parse exchangeInfo", e);
    }
  }

  /** Test hook: seed the cache without hitting the network. */
  void seed(Set<Instrument> instruments) {
    cached.set(Set.copyOf(instruments));
    loaded.set(true);
  }

  static CompletableFuture<Set<Instrument>> failed(Throwable err) {
    CompletableFuture<Set<Instrument>> f = new CompletableFuture<>();
    f.completeExceptionally(err);
    return f;
  }
}
