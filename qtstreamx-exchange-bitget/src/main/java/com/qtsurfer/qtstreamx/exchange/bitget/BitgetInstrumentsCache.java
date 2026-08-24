package com.qtsurfer.qtstreamx.exchange.bitget;

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
 * Bitget instrument cache. Spot: {@code /api/v2/spot/public/symbols} (status=online filter).
 * USDT futures: {@code /api/v2/mix/market/contracts?productType=USDT-FUTURES} (status=normal
 * filter, linear USDⓈ-M only). Inverse coin-M contracts live under a different productType
 * and aren't pulled here.
 */
public final class BitgetInstrumentsCache implements InstrumentsCache {

    private static final Logger log = LoggerFactory.getLogger(BitgetInstrumentsCache.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public enum Market {
        SPOT("bitget-spot", "https://api.bitget.com/api/v2/spot/public/symbols", false),
        USDT_FUTURES(
                "bitget-usdt-futures",
                "https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES",
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

    public BitgetInstrumentsCache(Market market) {
        this(
                market,
                HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(10))
                        .version(HttpClient.Version.HTTP_2)
                        .build());
    }

    public BitgetInstrumentsCache(Market market, HttpClient http) {
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
                                        "Bitget instruments HTTP " + resp.statusCode());
                            }
                            Set<Instrument> parsed = parse(resp.body(), market);
                            cached.set(parsed);
                            loaded.set(true);
                            log.info("Bitget {} refreshed: {} tradable instruments",
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
            JsonNode list = root.path("data");
            if (!list.isArray()) return Set.of();
            Set<Instrument> instruments = new HashSet<>(list.size() * 2);
            for (JsonNode sym : list) {
                if (market.futures) {
                    if (!"normal".equalsIgnoreCase(sym.path("symbolStatus").asText(""))) continue;
                    String base = sym.path("baseCoin").asText("");
                    String quote = sym.path("quoteCoin").asText("");
                    if (base.isEmpty() || quote.isEmpty()) continue;
                    String settle = sym.path("settleCoin").asText(quote);
                    instruments.add(new Instrument(base, quote, settle));
                } else {
                    if (!"online".equalsIgnoreCase(sym.path("status").asText(""))) continue;
                    String base = sym.path("baseCoin").asText("");
                    String quote = sym.path("quoteCoin").asText("");
                    if (base.isEmpty() || quote.isEmpty()) continue;
                    instruments.add(new Instrument(base, quote, null));
                }
            }
            return Set.copyOf(instruments);
        } catch (Exception e) {
            throw new RuntimeException("Parsing Bitget instruments response: " + e.getMessage(), e);
        }
    }
}
