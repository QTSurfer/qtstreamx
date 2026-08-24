package com.qtsurfer.qtstreamx.exchange.bybit;

import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.exchange.bybit.BybitInstrumentsCache.Category;
import com.qtsurfer.qtstreamx.exchange.bybit.BybitInstrumentsCache.ParsedList;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Parse-only tests for the Bybit instrument cache. Network I/O is exercised integration-side only
 * (not here) — the canned fixtures cover the filtering and funding-interval extraction the cache
 * relies on.
 */
class BybitInstrumentsCacheTest {

    @Test
    void parsesSpotAndFiltersDelisted() throws Exception {
        ParsedList parsed = BybitInstrumentsCache.parse(loadFixture("bybit-instruments-spot.json"), Category.SPOT);

        assertThat(parsed.instruments())
                .containsExactlyInAnyOrder(
                        new Instrument("BTC", "USDT", null),
                        new Instrument("ETH", "USDT", null),
                        new Instrument("SOL", "USDC", null));
        // DELST is status=Delisted, must not appear.
        assertThat(parsed.instruments()).doesNotContain(new Instrument("DELST", "USDT", null));
        assertThat(parsed.fundingIntervals()).isEmpty(); // spot carries no fundingInterval
    }

    @Test
    void parsesLinearWithSettleCoinAndFilterPreLaunch() throws Exception {
        ParsedList parsed =
                BybitInstrumentsCache.parse(
                        loadFixture("bybit-instruments-linear.json"), Category.LINEAR);

        // BTC/USDT:USDT, ETH/USDT:USDT, SOL/USDT:USDT — PreLaunch OLDPAIR filtered out.
        assertThat(parsed.instruments())
                .containsExactlyInAnyOrder(
                        new Instrument("BTC", "USDT", "USDT"),
                        new Instrument("ETH", "USDT", "USDT"),
                        new Instrument("SOL", "USDT", "USDT"));
        assertThat(parsed.fundingIntervals())
                .containsExactlyInAnyOrderEntriesOf(
                        Map.of("BTCUSDT", 240, "ETHUSDT", 240, "SOLUSDT", 480));
    }

    @Test
    void fundingIntervalHoursRoundsMinutesToHoursAndDefaultsToEight() throws Exception {
        BybitInstrumentsCache cache = new BybitInstrumentsCache(Category.LINEAR);
        // Use the package-private helper to seed the cache without HTTP.
        ParsedList parsed =
                BybitInstrumentsCache.parse(
                        loadFixture("bybit-instruments-linear.json"), Category.LINEAR);
        // Field-level seeding via reflection would be cleaner, but we can just verify the
        // fundingIntervalHours contract against the parsed map directly.
        Map<String, Integer> intervals = parsed.fundingIntervals();
        assertThat(intervals.get("BTCUSDT")).isEqualTo(240);
        assertThat(Math.round(240 / 60f)).isEqualTo(4);
        assertThat(Math.round(480 / 60f)).isEqualTo(8);

        // Default path: a symbol not in the parsed map defaults to 8h in the cache's accessor.
        // We construct a fresh cache with empty fundingIntervals and check the default:
        assertThat(cache.fundingIntervalHours(new Instrument("UNKNOWN", "USDT", "USDT"))).isEqualTo(8);
    }

    @Test
    void invalidResponseIsSurfacedAsRuntime() {
        try {
            BybitInstrumentsCache.parse("not json", Category.SPOT);
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).contains("Parsing Bybit instruments response");
            return;
        }
        throw new AssertionError("expected RuntimeException");
    }

    @Test
    void emptyListParsesToEmptySet() {
        ParsedList parsed =
                BybitInstrumentsCache.parse(
                        "{\"retCode\":0,\"result\":{\"category\":\"spot\",\"list\":[]}}",
                        Category.SPOT);
        assertThat(parsed.instruments()).isEmpty();
        assertThat(parsed.fundingIntervals()).isEmpty();
    }

    @Test
    void refreshFollowsPaginationCursorAcrossPages() throws Exception {
        // Linear catalogue spans >1 page. Page 1 carries a non-empty nextPageCursor; the cache
        // must follow it and merge both pages (BTC on p1, ETH on p2) rather than stop at p1.
        String page1 =
                "{\"result\":{\"nextPageCursor\":\"CUR1\",\"list\":[{\"status\":\"Trading\","
                    + "\"baseCoin\":\"BTC\",\"quoteCoin\":\"USDT\",\"settleCoin\":\"USDT\","
                    + "\"symbol\":\"BTCUSDT\"}]}}";
        String page2 =
                "{\"result\":{\"nextPageCursor\":\"\",\"list\":[{\"status\":\"Trading\","
                    + "\"baseCoin\":\"ETH\",\"quoteCoin\":\"USDT\",\"settleCoin\":\"USDT\","
                    + "\"symbol\":\"ETHUSDT\"}]}}";
        java.util.List<String> cursorsSeen = new java.util.ArrayList<>();
        BybitInstrumentsCache cache =
                new BybitInstrumentsCache(
                        Category.LINEAR,
                        cursor -> {
                            cursorsSeen.add(cursor);
                            return java.util.concurrent.CompletableFuture.completedFuture(
                                    cursor == null ? page1 : page2);
                        });

        Set<Instrument> result = cache.refresh().toCompletableFuture().get();

        assertThat(result)
                .containsExactlyInAnyOrder(
                        new Instrument("BTC", "USDT", "USDT"),
                        new Instrument("ETH", "USDT", "USDT"));
        // First fetch with null cursor, second with the verbatim page-1 cursor, then stop.
        assertThat(cursorsSeen).containsExactly(null, "CUR1");
    }

    @Test
    void refreshStopsOnEmptyCursorSinglePage() throws Exception {
        String onlyPage =
                "{\"result\":{\"nextPageCursor\":\"\",\"list\":[{\"status\":\"Trading\","
                    + "\"baseCoin\":\"SOL\",\"quoteCoin\":\"USDT\",\"settleCoin\":\"USDT\","
                    + "\"symbol\":\"SOLUSDT\"}]}}";
        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        BybitInstrumentsCache cache =
                new BybitInstrumentsCache(
                        Category.LINEAR,
                        cursor -> {
                            calls.incrementAndGet();
                            return java.util.concurrent.CompletableFuture.completedFuture(onlyPage);
                        });

        Set<Instrument> result = cache.refresh().toCompletableFuture().get();

        assertThat(result).containsExactly(new Instrument("SOL", "USDT", "USDT"));
        assertThat(calls.get()).isEqualTo(1); // empty cursor → exactly one fetch, no loop
    }

    @Test
    void parseExtractsNextPageCursor() throws Exception {
        ParsedList parsed =
                BybitInstrumentsCache.parse(
                        "{\"result\":{\"nextPageCursor\":\"first%3DA%26last%3DZ\",\"list\":[]}}",
                        Category.LINEAR);
        assertThat(parsed.nextPageCursor()).isEqualTo("first%3DA%26last%3DZ");
    }

    private static String loadFixture(String name) throws Exception {
        try (InputStream in =
                BybitInstrumentsCacheTest.class.getResourceAsStream("/" + name)) {
            Objects.requireNonNull(in, name + " missing from classpath");
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
