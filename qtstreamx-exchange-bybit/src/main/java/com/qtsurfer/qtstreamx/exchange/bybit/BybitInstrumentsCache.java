package com.qtsurfer.qtstreamx.exchange.bybit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.link.InstrumentsCache;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Discovers Bybit instruments via {@code /v5/market/instruments-info}.
 *
 * <p>The API segregates spot, linear and inverse under the {@code category} query param;
 * {@link Category} maps each to its exchange key and URL. Filters for {@code status = "Trading"}
 * so halted or delisted symbols don't enter the WS subscription set.
 *
 * <p>Linear responses include a {@code fundingInterval} in minutes per symbol; high-volume pairs
 * like BTCUSDT and ETHUSDT settle every 4h instead of the 8h default. The cache keeps the
 * mapping in {@link #fundingIntervalHours(Instrument)} so the streaming client can scale
 * {@code r1} / {@code r8} correctly in the NATS bridge.
 */
public final class BybitInstrumentsCache implements InstrumentsCache {

    private static final Logger log = LoggerFactory.getLogger(BybitInstrumentsCache.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public enum Category {
        SPOT("bybit-spot", "https://api.bybit.com/v5/market/instruments-info?category=spot", false),
        LINEAR(
                "bybit-linear",
                "https://api.bybit.com/v5/market/instruments-info?category=linear",
                true);

        final String exchangeKey;
        final String url;
        final boolean futures;

        Category(String exchangeKey, String url, boolean futures) {
            this.exchangeKey = exchangeKey;
            this.url = url;
            this.futures = futures;
        }
    }

    /** Safety bound on cursor-following so a misbehaving cursor can never loop forever. */
    private static final int MAX_PAGES = 20;

    private final Category category;
    private final Function<String, CompletionStage<String>> pageFetcher;
    private final AtomicReference<Set<Instrument>> cached = new AtomicReference<>(Set.of());
    private final AtomicReference<Map<String, Integer>> fundingIntervals =
            new AtomicReference<>(Map.of());
    private final AtomicBoolean loaded = new AtomicBoolean(false);

    public BybitInstrumentsCache(Category category) {
        this(
                category,
                HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(10))
                        .version(HttpClient.Version.HTTP_2)
                        .build());
    }

    public BybitInstrumentsCache(Category category, HttpClient http) {
        this.category = category;
        this.pageFetcher = cursor -> httpFetchPage(http, category, cursor);
    }

    /** Test seam: inject a page fetcher (cursor → response body) to exercise pagination sans HTTP. */
    BybitInstrumentsCache(Category category, Function<String, CompletionStage<String>> pageFetcher) {
        this.category = category;
        this.pageFetcher = pageFetcher;
    }

    @Override
    public String exchangeKey() {
        return category.exchangeKey;
    }

    @Override
    public CompletionStage<Set<Instrument>> refresh() {
        Set<Instrument> acc = new HashSet<>();
        Map<String, Integer> fundingAcc = new HashMap<>();
        return fetchPages(null, acc, fundingAcc, 0)
                .thenApply(
                        ignored -> {
                            Set<Instrument> result = Set.copyOf(acc);
                            cached.set(result);
                            fundingIntervals.set(Map.copyOf(fundingAcc));
                            loaded.set(true);
                            log.info(
                                    "Bybit {} refreshed: {} tradable instruments",
                                    category.exchangeKey,
                                    result.size());
                            return result;
                        });
    }

    /**
     * Walk the paginated instruments-info response. Bybit's linear/inverse catalogues exceed
     * the 500-symbol default page (~620 perps vs 500), returning a {@code result.nextPageCursor} for
     * the next page; spot fits one page (empty cursor). Fetching only page one silently dropped the
     * tail — and which symbols land on page two shifts over time, so two deployments discovered
     * divergent subsets. We follow the cursor — passed back verbatim, since Bybit returns it already
     * percent-encoded — until it is empty or {@link #MAX_PAGES} is reached, accumulating every page.
     */
    private CompletionStage<Void> fetchPages(
            String cursor, Set<Instrument> acc, Map<String, Integer> fundingAcc, int page) {
        return pageFetcher
                .apply(cursor)
                .thenCompose(
                        body -> {
                            ParsedList parsed = parse(body, category);
                            acc.addAll(parsed.instruments());
                            fundingAcc.putAll(parsed.fundingIntervals());
                            String next = parsed.nextPageCursor();
                            if (next != null && !next.isBlank() && page + 1 < MAX_PAGES) {
                                return fetchPages(next, acc, fundingAcc, page + 1);
                            }
                            if (next != null && !next.isBlank()) {
                                log.warn(
                                        "Bybit {} discovery stopped at MAX_PAGES={} with a non-empty"
                                                + " cursor — instrument set may be truncated",
                                        category.exchangeKey,
                                        MAX_PAGES);
                            }
                            return CompletableFuture.completedFuture(null);
                        });
    }

    /** Fetch one page ({@code cursor} null/blank for the first). Returns the raw JSON body. */
    private static CompletionStage<String> httpFetchPage(
            HttpClient http, Category category, String cursor) {
        // limit=1000 grabs the whole catalogue in as few pages as Bybit allows (max page size).
        StringBuilder url = new StringBuilder(category.url).append("&limit=1000");
        if (cursor != null && !cursor.isBlank()) {
            url.append("&cursor=").append(cursor);
        }
        HttpRequest req =
                HttpRequest.newBuilder(URI.create(url.toString()))
                        .timeout(Duration.ofSeconds(20))
                        .header("Accept", "application/json")
                        .GET()
                        .build();
        return http.sendAsync(req, HttpResponse.BodyHandlers.ofString())
                .thenApply(
                        resp -> {
                            if (resp.statusCode() != 200) {
                                throw new RuntimeException(
                                        "Bybit instruments-info HTTP " + resp.statusCode());
                            }
                            return resp.body();
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

    /**
     * Lookup the funding interval in hours for a linear perp. Returns 8 (the Bybit default) for
     * unknown instruments, spot instruments, or when the REST response didn't include a
     * fundingInterval for this symbol.
     */
    public int fundingIntervalHours(Instrument instrument) {
        Integer minutes =
                fundingIntervals.get().get(BybitSymbols.toStream(instrument));
        if (minutes == null || minutes <= 0) {
            return 8;
        }
        // Round to nearest hour; Bybit only ever uses multiples of 60 (240, 480…) so this is
        // exact in practice.
        return Math.max(1, Math.round(minutes / 60f));
    }

    static ParsedList parse(String body, Category category) {
        try {
            JsonNode root = MAPPER.readTree(body);
            JsonNode result = root.path("result");
            JsonNode list = result.path("list");
            String nextPageCursor = result.path("nextPageCursor").asText("");
            if (!list.isArray()) {
                return new ParsedList(Set.of(), Map.of(), "");
            }
            Set<Instrument> instruments = new HashSet<>(list.size() * 2);
            Map<String, Integer> fundingMap = new HashMap<>();
            for (JsonNode sym : list) {
                if (!"Trading".equalsIgnoreCase(sym.path("status").asText(""))) continue;
                String base = sym.path("baseCoin").asText("");
                String quote = sym.path("quoteCoin").asText("");
                if (base.isEmpty() || quote.isEmpty()) continue;
                String settle = category.futures ? sym.path("settleCoin").asText(quote) : null;
                instruments.add(new Instrument(base, quote, settle != null && settle.isEmpty() ? null : settle));
                if (category.futures && sym.has("fundingInterval")) {
                    int interval = sym.path("fundingInterval").asInt(0);
                    if (interval > 0) {
                        fundingMap.put(sym.path("symbol").asText(""), interval);
                    }
                }
            }
            return new ParsedList(Set.copyOf(instruments), Map.copyOf(fundingMap), nextPageCursor);
        } catch (Exception e) {
            throw new RuntimeException("Parsing Bybit instruments response: " + e.getMessage(), e);
        }
    }

    record ParsedList(
            Set<Instrument> instruments, Map<String, Integer> fundingIntervals, String nextPageCursor) {}
}
