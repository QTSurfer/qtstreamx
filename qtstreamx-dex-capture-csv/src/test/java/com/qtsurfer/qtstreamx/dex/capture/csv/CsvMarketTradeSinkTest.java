package com.qtsurfer.qtstreamx.dex.capture.csv;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIOException;

import com.qtsurfer.qtstreamx.core.client.MarketTradeAcknowledgement;
import com.qtsurfer.qtstreamx.core.client.MarketTradeBatch;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.model.MarketId;
import com.qtsurfer.qtstreamx.core.model.MarketTrade;
import com.qtsurfer.qtstreamx.core.model.TradeSide;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CsvMarketTradeSinkTest {

    @TempDir
    Path directory;

    @Test
    void writesExactSchemaAndPlainDecimalRows() throws Exception {
        Path output = directory.resolve("nested/trades.csv");

        try (CsvMarketTradeSink sink = new CsvMarketTradeSink(output, market("0xpool"))) {
            assertThat(sink.handle(new MarketTradeBatch(List.of(trade("event-1", "0xpool")))))
                    .isEqualTo(MarketTradeAcknowledgement.ACKNOWLEDGED);
        }

        assertThat(Files.readString(output, StandardCharsets.UTF_8))
                .isEqualTo(
                        CsvMarketTradeSink.HEADER
                                + "\n"
                                + "event-1,1700000000123456,2000.000000,1.2300,2460.000000,BUY\n");
        assertThat(Files.readString(output.resolveSibling("trades.csv.metadata.csv"), StandardCharsets.UTF_8))
                .isEqualTo(
                        CsvMarketTradeSink.METADATA_HEADER
                                + "\n"
                                + "uniswap-v3,eip155:1,0xpool,WETH/USDC,1700000000123456,1700000000123456\n");
    }

    @Test
    void escapesDelimitedQuotedAndMultilineFields() throws Exception {
        Path output = directory.resolve("trades.csv");
        MarketTrade trade = trade("event,\"one\"\ntwo", "0xpool");

        try (CsvMarketTradeSink sink = new CsvMarketTradeSink(output, market("0xpool"))) {
            sink.handle(new MarketTradeBatch(List.of(trade)));
        }

        assertThat(Files.readString(output, StandardCharsets.UTF_8))
                .contains("\"event,\"\"one\"\"\ntwo\"")
                .doesNotContain("uniswap-v3");
        try (CsvMarketTradeSink reopened = new CsvMarketTradeSink(output, market("0xpool"))) {
            assertThat(reopened.handle(new MarketTradeBatch(List.of(trade))))
                    .isEqualTo(MarketTradeAcknowledgement.ACKNOWLEDGED);
        }
        assertThat(Files.readAllLines(output, StandardCharsets.UTF_8)).hasSize(3);
    }

    @Test
    void reopensAndDeduplicatesCheckpointLagReplay() throws Exception {
        Path output = directory.resolve("trades.csv");
        try (CsvMarketTradeSink sink = new CsvMarketTradeSink(output, market("0xpool"))) {
            sink.handle(new MarketTradeBatch(List.of(trade("first", "0xpool"), trade("replay", "0xpool"))));
        }

        try (CsvMarketTradeSink sink = new CsvMarketTradeSink(output, market("0xpool"))) {
            sink.handle(new MarketTradeBatch(List.of(trade("replay", "0xpool"), trade("last", "0xpool"))));
        }

        assertThat(Files.readAllLines(output, StandardCharsets.UTF_8))
                .containsExactly(
                        CsvMarketTradeSink.HEADER,
                        "first,1700000000123456,2000.000000,1.2300,2460.000000,BUY",
                        "replay,1700000000123456,2000.000000,1.2300,2460.000000,BUY",
                        "last,1700000000123456,2000.000000,1.2300,2460.000000,BUY");
    }

    @Test
    void reopeningRetainsTheFullMetadataTimeRange() throws Exception {
        Path output = directory.resolve("trades.csv");
        try (CsvMarketTradeSink sink = new CsvMarketTradeSink(output, market("0xpool"))) {
            sink.handle(new MarketTradeBatch(List.of(trade("first", "0xpool"))));
        }
        try (CsvMarketTradeSink sink = new CsvMarketTradeSink(output, market("0xpool"))) {
            sink.handle(new MarketTradeBatch(List.of(trade("first", "0xpool"))));
        }
        assertThat(Files.readAllLines(output.resolveSibling("trades.csv.metadata.csv")))
                .containsExactly(
                        CsvMarketTradeSink.METADATA_HEADER,
                        "uniswap-v3,eip155:1,0xpool,WETH/USDC,1700000000123456,1700000000123456");
    }

    @Test
    void failsClosedBeforeAppendingAnIncompatibleExistingCapture() throws Exception {
        Path output = directory.resolve("trades.csv");
        String original = "wrong,header\nvalue,row\n";
        Files.writeString(output, original, StandardCharsets.UTF_8);

        assertThatIOException().isThrownBy(() -> new CsvMarketTradeSink(output, market("0xpool")));

        assertThat(Files.readString(output, StandardCharsets.UTF_8)).isEqualTo(original);
    }

    @Test
    void rejectsDuplicateOrIncompleteExistingRecords() throws Exception {
        Path duplicate = directory.resolve("duplicate.csv");
        Files.writeString(
                duplicate,
                CsvMarketTradeSink.HEADER
                        + "\n"
                        + "event-1,1700000000123456,2000.000000,1.2300,2460.000000,BUY\n"
                        + "event-1,1700000000123456,2000.000000,1.2300,2460.000000,BUY\n",
                StandardCharsets.UTF_8);
        Path incomplete = directory.resolve("incomplete.csv");
        Files.writeString(incomplete, CsvMarketTradeSink.HEADER + "\npartial", StandardCharsets.UTF_8);

        assertThatIOException().isThrownBy(() -> new CsvMarketTradeSink(duplicate, market("0xpool")));
        assertThatIOException().isThrownBy(() -> new CsvMarketTradeSink(incomplete, market("0xpool")));
    }

    @Test
    void poisonsSinkWhenForceFailsBeforeAcknowledgement() throws Exception {
        FailingFileAccess fileAccess = new FailingFileAccess();
        CsvMarketTradeSink sink = new CsvMarketTradeSink(directory.resolve("trades.csv"), market("0xpool"), fileAccess);

        assertThatIOException()
                .isThrownBy(() -> sink.handle(new MarketTradeBatch(List.of(trade("event-1", "0xpool")))))
                .withMessage("forced failure");
        assertThatIOException().isThrownBy(() -> sink.handle(new MarketTradeBatch(List.of(trade("event-2", "0xpool")))))
                .withMessageContaining("previous write failure");
        assertThat(fileAccess.appendCalls).isEqualTo(2);
    }

    private static MarketTrade trade(String eventId, String nativeId) {
        return new MarketTrade(
                market(nativeId),
                eventId,
                new BigDecimal("2000.000000"),
                new BigDecimal("1.2300"),
                new BigDecimal("2460.000000"),
                TradeSide.BUY,
                1_700_000_000_123_456L);
    }

    private static MarketId market(String nativeId) {
        return new MarketId("uniswap-v3", "eip155:1", nativeId, new Instrument("WETH", "USDC", null));
    }

    private static final class FailingFileAccess implements CsvMarketTradeSink.CsvFileAccess {
        private int appendCalls;

        @Override
        public boolean exists(Path path) {
            return false;
        }

        @Override
        public boolean isDirectory(Path path) {
            return false;
        }

        @Override
        public byte[] readAllBytes(Path path) {
            throw new AssertionError("not used");
        }

        @Override
        public void createDirectories(Path path) {
            // No filesystem interaction is required for this deterministic failure seam.
        }

        @Override
        public void appendAndForce(Path path, byte[] bytes) throws IOException {
            appendCalls++;
            if (appendCalls > 1) {
                throw new IOException("forced failure");
            }
        }
    }
}
