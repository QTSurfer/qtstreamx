package com.qtsurfer.qtstreamx.exchange.htx;

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
 * HTX (ex-Huobi) instrument cache.
 *
 * <ul>
 *   <li>Spot: {@code https://api.huobi.pro/v1/common/symbols} — filter {@code state=online}.
 *   <li>Linear swap (USDⓈ-M): {@code https://api.hbdm.com/linear-swap-api/v1/swap_contract_info}
 *       — filter {@code contract_status=1} (listed). Only {@code business_type=swap} entries are
 *       kept so dated futures (delivery contracts) are excluded.
 * </ul>
 *
 * Inverse coin-M swaps live under {@code /swap-api/v1/swap_contract_info} on {@code api.hbdm.com}
 * and aren't pulled here — this cache is linear-only, matching the rest of qtstreamx.
 */
public final class HtxInstrumentsCache implements InstrumentsCache {

    private static final Logger log = LoggerFactory.getLogger(HtxInstrumentsCache.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public enum Market {
        SPOT("htx-spot", "https://api.huobi.pro/v1/common/symbols", false),
        LINEAR_SWAP(
                "htx-linear-swap",
                "https://api.hbdm.com/linear-swap-api/v1/swap_contract_info",
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

    public HtxInstrumentsCache(Market market) {
        this(
                market,
                HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(10))
                        .version(HttpClient.Version.HTTP_2)
                        .build());
    }

    public HtxInstrumentsCache(Market market, HttpClient http) {
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
                                        "HTX instruments HTTP " + resp.statusCode());
                            }
                            Set<Instrument> parsed = parse(resp.body(), market);
                            cached.set(parsed);
                            loaded.set(true);
                            log.info("HTX {} refreshed: {} tradable instruments",
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
                    // contract_status: 0=pending, 1=listed, 2=off-shelf, etc. Listed only.
                    if (sym.path("contract_status").asInt(0) != 1) continue;
                    // business_type: "swap" (perpetual) vs "futures" (dated). Linear perps only.
                    String bt = sym.path("business_type").asText("");
                    if (!bt.isEmpty() && !"swap".equalsIgnoreCase(bt)) continue;
                    String base = sym.path("symbol").asText("");
                    String quote = sym.path("trade_partition").asText("");
                    if (base.isEmpty() || quote.isEmpty()) continue;
                    instruments.add(new Instrument(base.toUpperCase(), quote.toUpperCase(),
                            quote.toUpperCase()));
                } else {
                    if (!"online".equalsIgnoreCase(sym.path("state").asText(""))) continue;
                    String base = sym.path("base-currency").asText("");
                    String quote = sym.path("quote-currency").asText("");
                    if (base.isEmpty() || quote.isEmpty()) continue;
                    instruments.add(new Instrument(base.toUpperCase(), quote.toUpperCase(), null));
                }
            }
            return Set.copyOf(instruments);
        } catch (Exception e) {
            throw new RuntimeException("Parsing HTX instruments response: " + e.getMessage(), e);
        }
    }
}
