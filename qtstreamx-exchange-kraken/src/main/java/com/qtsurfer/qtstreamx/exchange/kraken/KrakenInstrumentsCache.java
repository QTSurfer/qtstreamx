package com.qtsurfer.qtstreamx.exchange.kraken;

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
 * Kraken instrument discovery. Two REST endpoints:
 *
 * <ul>
 *   <li>Spot: {@code https://api.kraken.com/0/public/AssetPairs} — returns {@code wsname} ({@code
 *       "BTC/USD"}) and {@code status} ({@code "online"}).
 *   <li>Futures: {@code https://futures.kraken.com/derivatives/api/v3/instruments} — returns
 *       the {@code PF_/PI_/FF_} product ids plus {@code tradeable} boolean and a
 *       {@code category} enum.
 * </ul>
 *
 * <p>{@link Market} flips between the two. The spot endpoint's {@code wsname} is exactly the
 * symbol form Kraken v2 WS expects, so we reuse it directly; futures needs the XBT→BTC rewrite
 * to produce a canonical {@link Instrument}.
 */
public final class KrakenInstrumentsCache implements InstrumentsCache {

    private static final Logger log = LoggerFactory.getLogger(KrakenInstrumentsCache.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public enum Market {
        SPOT("kraken-spot", "https://api.kraken.com/0/public/AssetPairs", false),
        FUTURES_LINEAR(
                "kraken-futures-linear",
                "https://futures.kraken.com/derivatives/api/v3/instruments",
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

    public KrakenInstrumentsCache(Market market) {
        this(
                market,
                HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(10))
                        .version(HttpClient.Version.HTTP_2)
                        .build());
    }

    public KrakenInstrumentsCache(Market market, HttpClient http) {
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
                                        "Kraken instruments HTTP " + resp.statusCode());
                            }
                            Set<Instrument> parsed = parse(resp.body(), market);
                            cached.set(parsed);
                            loaded.set(true);
                            log.info("Kraken {} refreshed: {} tradable instruments",
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
            if (market.futures) {
                return parseFutures(root);
            }
            return parseSpot(root);
        } catch (Exception e) {
            throw new RuntimeException("Parsing Kraken instruments response: " + e.getMessage(), e);
        }
    }

    /**
     * Spot: {@code {"result":{"XBTUSD":{"wsname":"XBT/USD","status":"online",...}}}}. We rewrite
     * {@code XBT} → {@code BTC} on the base side so {@link Instrument#base()} matches every
     * other exchange.
     */
    private static Set<Instrument> parseSpot(JsonNode root) {
        JsonNode result = root.path("result");
        if (!result.isObject()) return Set.of();
        Set<Instrument> instruments = new HashSet<>(result.size() * 2);
        result.fields()
                .forEachRemaining(
                        e -> {
                            JsonNode pair = e.getValue();
                            if (!"online".equalsIgnoreCase(pair.path("status").asText(""))) return;
                            String wsname = pair.path("wsname").asText("");
                            int slash = wsname.indexOf('/');
                            if (slash <= 0) return;
                            String base = normaliseBase(wsname.substring(0, slash));
                            String quote = wsname.substring(slash + 1);
                            instruments.add(new Instrument(base, quote, null));
                        });
        return Set.copyOf(instruments);
    }

    /**
     * Futures: {@code {"instruments":[{"symbol":"PF_XBTUSD","tradeable":true,"category":...}]}}.
     * Keep only {@code PF_*} (linear perpetuals) for now — fixed-expiry {@code FF_*} and
     * inverse {@code PI_*} are out of scope.
     */
    private static Set<Instrument> parseFutures(JsonNode root) {
        JsonNode list = root.path("instruments");
        if (!list.isArray()) return Set.of();
        Set<Instrument> instruments = new HashSet<>(list.size() * 2);
        for (JsonNode inst : list) {
            if (!inst.path("tradeable").asBoolean(false)) continue;
            String symbol = inst.path("symbol").asText("").toUpperCase();
            if (!symbol.startsWith("PF_")) continue;
            String pair = symbol.substring(3); // "XBTUSD", "ETHUSD", "ETHUSDT"…
            // Quote detection: Kraken perps use USD / USDT / USDC. Longest-suffix match wins.
            String quote;
            String base;
            if (pair.endsWith("USDT")) {
                quote = "USDT";
                base = pair.substring(0, pair.length() - 4);
            } else if (pair.endsWith("USDC")) {
                quote = "USDC";
                base = pair.substring(0, pair.length() - 4);
            } else if (pair.endsWith("USD")) {
                quote = "USD";
                base = pair.substring(0, pair.length() - 3);
            } else {
                continue; // unknown quote layout — skip rather than guess
            }
            instruments.add(new Instrument(normaliseBase(base), quote, quote));
        }
        return Set.copyOf(instruments);
    }

    /** {@code XBT → BTC}; everything else untouched. */
    private static String normaliseBase(String base) {
        return "XBT".equals(base) ? "BTC" : base;
    }
}
