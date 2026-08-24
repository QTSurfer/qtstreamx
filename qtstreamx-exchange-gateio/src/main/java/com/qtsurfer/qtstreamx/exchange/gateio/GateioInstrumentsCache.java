package com.qtsurfer.qtstreamx.exchange.gateio;

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
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Gate.io instrument cache. Spot: {@code /api/v4/spot/currency_pairs} (trade_status filter).
 * Futures USDT: {@code /api/v4/futures/usdt/contracts} (in_delisting filter). Linear USDⓈ-M
 * perps only; inverse markets live under a different path and aren't pulled here.
 */
public final class GateioInstrumentsCache implements InstrumentsCache {

    private static final Logger log = LoggerFactory.getLogger(GateioInstrumentsCache.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public enum Market {
        SPOT("gateio-spot", "https://api.gateio.ws/api/v4/spot/currency_pairs", false),
        FUTURES_USDT(
                "gateio-futures-usdt",
                "https://api.gateio.ws/api/v4/futures/usdt/contracts",
                true);

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

    public GateioInstrumentsCache(Market market) {
        this(
                market,
                HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(10))
                        .version(HttpClient.Version.HTTP_2)
                        .build());
    }

    public GateioInstrumentsCache(Market market, HttpClient http) {
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
                        .timeout(Duration.ofSeconds(20))
                        .header("Accept", "application/json")
                        .GET()
                        .build();
        return http.sendAsync(req, HttpResponse.BodyHandlers.ofString())
                .thenApply(
                        resp -> {
                            if (resp.statusCode() != 200) {
                                throw new RuntimeException(
                                        "Gate.io instruments HTTP " + resp.statusCode());
                            }
                            Set<Instrument> parsed = parse(resp.body(), market);
                            cached.set(parsed);
                            loaded.set(true);
                            log.info("Gate.io {} refreshed: {} tradable instruments",
                                    market.exchangeKey, parsed.size());
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

    static Set<Instrument> parse(String body, Market market) {
        try {
            JsonNode root = MAPPER.readTree(body);
            if (!root.isArray()) return Set.of();
            Set<Instrument> instruments = new HashSet<>(root.size() * 2);
            for (JsonNode sym : root) {
                if (market.futures) {
                    if (sym.path("in_delisting").asBoolean(false)) continue;
                    // Futures contracts use a {@code name} field like "BTC_USDT".
                    String name = sym.path("name").asText("");
                    int under = name.indexOf('_');
                    if (under <= 0) continue;
                    String base = name.substring(0, under);
                    String quote = name.substring(under + 1);
                    instruments.add(new Instrument(base, quote, quote));
                } else {
                    if (!"tradable".equalsIgnoreCase(sym.path("trade_status").asText(""))) continue;
                    String base = sym.path("base").asText("");
                    String quote = sym.path("quote").asText("");
                    if (base.isEmpty() || quote.isEmpty()) continue;
                    instruments.add(new Instrument(base, quote, null));
                }
            }
            return Set.copyOf(instruments);
        } catch (Exception e) {
            throw new RuntimeException("Parsing Gate.io instruments response: " + e.getMessage(), e);
        }
    }
}
