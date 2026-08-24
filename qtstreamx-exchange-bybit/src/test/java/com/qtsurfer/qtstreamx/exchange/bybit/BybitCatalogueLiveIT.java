package com.qtsurfer.qtstreamx.exchange.bybit;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Discovery against the live Bybit catalogue.
 *
 * <p>The unit tests around {@link BybitInstrumentsCache} pin parsing and pagination against
 * recorded fixtures. They cannot see the failure that actually costs data: the real catalogue
 * growing past what discovery walks, or a filter quietly excluding a class of instrument that the
 * fixtures happen not to contain. Both are silent — the publisher reports a healthy instrument
 * count either way, because the count it reports is the count it discovered.
 *
 * <p>Written after {@code AKE/USDT/USDT} — a linear perp with 128.5 M USDT of 24 h turnover — was
 * found to have no captured data on one fleet and to have stopped on the other, and the first
 * question that had to be answered was whether discovery had dropped it. It had not; the fault was
 * further down. This test exists so that question is answered by a test run rather than by an
 * afternoon of manual comparison.
 *
 * <p>Hits the public REST API, so it is opt-in: set {@code QTSTREAMX_LIVE_IT=1}. Ordinary
 * {@code ./gradlew build} runs stay hermetic.
 */
@EnabledIfEnvironmentVariable(named = "QTSTREAMX_LIVE_IT", matches = "1")
class BybitCatalogueLiveIT {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void linear_discovery_loses_no_tradable_instrument() throws Exception {
        Set<Instrument> expected = fetchExpected("linear", true);
        Set<Instrument> discovered =
                new BybitInstrumentsCache(BybitInstrumentsCache.Category.LINEAR)
                        .refresh()
                        .toCompletableFuture()
                        .get(60, TimeUnit.SECONDS);

        // Compare the sets, not their sizes: two sets of equal size can still differ, and the
        // failure being guarded against is exactly "some instruments went missing while others
        // arrived". Any element on either side is named in the failure message.
        assertThat(discovered).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void spot_discovery_loses_no_tradable_instrument() throws Exception {
        Set<Instrument> expected = fetchExpected("spot", false);
        Set<Instrument> discovered =
                new BybitInstrumentsCache(BybitInstrumentsCache.Category.SPOT)
                        .refresh()
                        .toCompletableFuture()
                        .get(60, TimeUnit.SECONDS);

        assertThat(discovered).containsExactlyInAnyOrderElementsOf(expected);
    }

    /**
     * The linear catalogue must not be sitting at the page cap.
     *
     * <p>At {@code limit=1000} — what the cache requests — the whole catalogue currently fits one
     * page, so the cursor comes back blank and the pagination path is exercised only by the fixture
     * unit tests. That is fine while there is headroom. What is not fine is the day the catalogue
     * reaches 1000: a full page with a blank cursor is indistinguishable from a complete one, and
     * discovery would silently stop at the cap. This asserts the headroom that makes the
     * single-page behaviour safe, and fails while there is still time to react.
     *
     * <p>The historical hazard is on record in {@link BybitInstrumentsCache}: fetching only page one
     * at the default page size of 500 dropped the tail, and which symbols landed on page two shifted
     * over time, so two deployments discovered divergent subsets.
     */
    @Test
    void linear_catalogue_has_headroom_below_the_page_cap() throws Exception {
        JsonNode result = MAPPER.readTree(get(url("linear", null))).path("result");
        int onFirstPage = result.path("list").size();
        String cursor = result.path("nextPageCursor").asText("");

        assertThat(onFirstPage).isPositive();
        if (cursor.isBlank()) {
            assertThat(onFirstPage)
                    .as(
                            "linear catalogue returned a FULL page of %d with no cursor — at the cap"
                                + " a complete page and a truncated one look identical",
                            onFirstPage)
                    .isLessThan(1000);
        }
    }

    // --------------------------------------------------------------------------

    /**
     * The catalogue as the exchange reports it, reduced the way the cache reduces it: tradable
     * only, keyed by (base, quote, settle). Dated futures collapse onto the perpetual that shares
     * their triple, which is why the linear instrument count sits below the raw symbol count.
     */
    private static Set<Instrument> fetchExpected(String category, boolean futures)
            throws Exception {
        Set<Instrument> out = new HashSet<>();
        String cursor = null;
        for (int page = 0; page < 20; page++) {
            JsonNode result = MAPPER.readTree(get(url(category, cursor))).path("result");
            for (JsonNode sym : result.path("list")) {
                if (!"Trading".equalsIgnoreCase(sym.path("status").asText(""))) continue;
                String base = sym.path("baseCoin").asText("");
                String quote = sym.path("quoteCoin").asText("");
                if (base.isEmpty() || quote.isEmpty()) continue;
                String settle = futures ? sym.path("settleCoin").asText(quote) : null;
                out.add(new Instrument(base, quote, settle != null && settle.isEmpty() ? null : settle));
            }
            cursor = result.path("nextPageCursor").asText("");
            if (cursor.isBlank()) break;
        }
        assertThat(out).as("live catalogue for %s came back empty", category).isNotEmpty();
        return out;
    }

    private static String url(String category, String cursor) {
        String u =
                "https://api.bybit.com/v5/market/instruments-info?category=" + category + "&limit=1000";
        return (cursor == null || cursor.isBlank()) ? u : u + "&cursor=" + cursor;
    }

    private static String get(String url) throws Exception {
        HttpClient http =
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        HttpResponse<String> resp =
                http.send(
                        HttpRequest.newBuilder(URI.create(url))
                                .timeout(Duration.ofSeconds(20))
                                .header("Accept", "application/json")
                                .GET()
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
        assertThat(resp.statusCode()).as("Bybit instruments-info").isEqualTo(200);
        return resp.body();
    }
}
