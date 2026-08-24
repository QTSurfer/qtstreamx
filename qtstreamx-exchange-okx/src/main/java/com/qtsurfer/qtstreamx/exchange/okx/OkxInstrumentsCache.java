package com.qtsurfer.qtstreamx.exchange.okx;

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
 * Discovers OKX instruments via {@code /api/v5/public/instruments}.
 *
 * <p>OKX segregates by {@code instType}:
 *
 * <ul>
 *   <li>{@code SPOT} — {@code BTC-USDT} style, {@code baseCcy} / {@code quoteCcy} fields
 *   <li>{@code SWAP} — {@code BTC-USDT-SWAP}, {@code settleCcy} field, {@code ctType=linear}
 *       for USDⓈ-M perps; {@code inverse} for coin-margined (this cache filters to
 *       {@code linear} only).
 * </ul>
 *
 * <p>Filters {@code state == "live"} so pre-launch and suspended symbols don't enter the
 * subscription set.
 */
public final class OkxInstrumentsCache implements InstrumentsCache {

    private static final Logger log = LoggerFactory.getLogger(OkxInstrumentsCache.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public enum InstType {
        SPOT("okx-spot", "https://www.okx.com/api/v5/public/instruments?instType=SPOT", false),
        SWAP("okx-swap-linear", "https://www.okx.com/api/v5/public/instruments?instType=SWAP", true);

        final String exchangeKey;
        final String url;
        final boolean futures;

        InstType(String exchangeKey, String url, boolean futures) {
            this.exchangeKey = exchangeKey;
            this.url = url;
            this.futures = futures;
        }
    }

    private final InstType instType;
    private final HttpClient http;
    private final AtomicReference<Set<Instrument>> cached = new AtomicReference<>(Set.of());
    private final AtomicBoolean loaded = new AtomicBoolean(false);

    public OkxInstrumentsCache(InstType instType) {
        this(
                instType,
                HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(10))
                        .version(HttpClient.Version.HTTP_2)
                        .build());
    }

    public OkxInstrumentsCache(InstType instType, HttpClient http) {
        this.instType = instType;
        this.http = http;
    }

    @Override
    public String exchangeKey() {
        return instType.exchangeKey;
    }

    @Override
    public CompletionStage<Set<Instrument>> refresh() {
        HttpRequest req =
                HttpRequest.newBuilder(URI.create(instType.url))
                        .timeout(Duration.ofSeconds(20))
                        .header("Accept", "application/json")
                        .GET()
                        .build();
        return http.sendAsync(req, HttpResponse.BodyHandlers.ofString())
                .thenApply(
                        resp -> {
                            if (resp.statusCode() != 200) {
                                throw new RuntimeException(
                                        "OKX instruments HTTP " + resp.statusCode());
                            }
                            Set<Instrument> parsed = parse(resp.body(), instType);
                            cached.set(parsed);
                            loaded.set(true);
                            log.info("OKX {} refreshed: {} tradable instruments",
                                    instType.exchangeKey, parsed.size());
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

    static Set<Instrument> parse(String body, InstType instType) {
        try {
            JsonNode root = MAPPER.readTree(body);
            JsonNode list = root.path("data");
            if (!list.isArray()) {
                return Set.of();
            }
            Set<Instrument> instruments = new HashSet<>(list.size() * 2);
            for (JsonNode sym : list) {
                if (!"live".equalsIgnoreCase(sym.path("state").asText(""))) continue;
                if (instType.futures) {
                    // Filter to linear USDⓈ-M only; inverse coin-margined perps share the SWAP
                    // endpoint but report ctType=inverse.
                    if (!"linear".equalsIgnoreCase(sym.path("ctType").asText(""))) continue;
                    String instId = sym.path("instId").asText("");
                    // instId format: BASE-QUOTE-SWAP. Split and reuse quote as settle (linear
                    // USDⓈ-M always settles in quote).
                    String[] parts = instId.split("-");
                    if (parts.length != 3) continue;
                    String base = parts[0];
                    String quote = parts[1];
                    String settle = sym.path("settleCcy").asText(quote);
                    instruments.add(new Instrument(base, quote, settle));
                } else {
                    String base = sym.path("baseCcy").asText("");
                    String quote = sym.path("quoteCcy").asText("");
                    if (base.isEmpty() || quote.isEmpty()) continue;
                    instruments.add(new Instrument(base, quote, null));
                }
            }
            return Set.copyOf(instruments);
        } catch (Exception e) {
            throw new RuntimeException("Parsing OKX instruments response: " + e.getMessage(), e);
        }
    }
}
