package com.qtsurfer.qtstreamx.canary;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.qtsurfer.qtstreamx.aggregation.CandleInterval;
import com.qtsurfer.qtstreamx.core.client.MarketTradeAcknowledgement;
import com.qtsurfer.qtstreamx.core.client.MarketTradeBatch;
import com.qtsurfer.qtstreamx.core.client.MarketTradeBatchHandler;
import com.qtsurfer.qtstreamx.core.client.RecoverableMarketTradeStream;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.model.MarketId;
import com.qtsurfer.qtstreamx.core.model.MarketTrade;
import com.qtsurfer.qtstreamx.core.model.TradeSide;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DexCaptureSessionRecoverableTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @TempDir
    Path outputDirectory;

    @Test
    void acknowledgesOnlyAfterEveryTradeIsRecordedAndAggregated() throws Exception {
        RecordingTradeStream stream = new RecordingTradeStream();
        try (DexCaptureSession session = new DexCaptureSession(
                stream, CandleInterval.ONE_MINUTE, outputDirectory)) {
            session.startRecoverable();

            MarketTradeAcknowledgement acknowledgement = stream.emit(new MarketTradeBatch(List.of(
                    trade("event-1", 1_000_000L),
                    trade("event-2", 2_000_000L))));

            assertThat(acknowledgement).isEqualTo(MarketTradeAcknowledgement.ACKNOWLEDGED);
            assertThat(session.tradeCount()).isEqualTo(2);
            assertThat(Files.readAllLines(outputDirectory.resolve("trades.ndjson"))).hasSize(2);
            assertThat(Files.readAllLines(outputDirectory.resolve("tickers.ndjson"))).hasSize(2);
        }
    }

    @Test
    void doesNotAcknowledgeWhenDownstreamProcessingFails() throws Exception {
        RecordingTradeStream stream = new RecordingTradeStream();
        try (DexCaptureSession session = new DexCaptureSession(
                stream, CandleInterval.ONE_MINUTE, outputDirectory)) {
            session.startRecoverable();

            assertThatThrownBy(() -> stream.emit(new MarketTradeBatch(List.of(
                            trade("event-1", 2_000_000L),
                            trade("event-2", 1_000_000L)))))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("out of order");
            assertThat(stream.acknowledgements).isZero();
            assertThat(Files.readAllLines(outputDirectory.resolve("trades.ndjson"))).isEmpty();
        }
    }

    @Test
    void restartKeepsCommittedHistoryAndDeduplicatesCheckpointLagReplay() throws Exception {
        RecordingTradeStream firstStream = new RecordingTradeStream();
        try (DexCaptureSession first = new DexCaptureSession(
                firstStream, CandleInterval.ONE_MINUTE, outputDirectory)) {
            first.startRecoverable();
            assertThat(firstStream.emit(new MarketTradeBatch(List.of(
                            trade("committed", 1_000_000L),
                            trade("checkpoint-lag", 2_000_000L)))))
                    .isEqualTo(MarketTradeAcknowledgement.ACKNOWLEDGED);
        }

        RecordingTradeStream replacementStream = new RecordingTradeStream();
        try (DexCaptureSession replacement = new DexCaptureSession(
                replacementStream, CandleInterval.ONE_MINUTE, outputDirectory)) {
            replacement.startRecoverable();
            assertThat(replacementStream.emit(new MarketTradeBatch(List.of(
                            trade("checkpoint-lag", 2_000_000L),
                            trade("new", 3_000_000L)))))
                    .isEqualTo(MarketTradeAcknowledgement.ACKNOWLEDGED);
        }

        assertThat(readEventIds(outputDirectory.resolve("trades.ndjson")))
                .containsExactly("committed", "checkpoint-lag", "new");
        assertThat(Files.readAllLines(outputDirectory.resolve("tickers.ndjson"))).hasSize(3);
    }

    private static List<String> readEventIds(Path path) throws Exception {
        try (var lines = Files.lines(path)) {
            return lines.map(line -> {
                try {
                    JsonNode trade = MAPPER.readTree(line);
                    return trade.path("eventId").asText();
                } catch (Exception exception) {
                    throw new IllegalStateException(exception);
                }
            }).toList();
        }
    }

    private static MarketTrade trade(String eventId, long timestamp) {
        return new MarketTrade(
                new MarketId(
                        "eip155:1",
                        "uniswap-v2",
                        "0x00000000000000000000000000000000000000ab",
                        new Instrument("WETH", "USDC")),
                eventId,
                new BigDecimal("2000"),
                BigDecimal.ONE,
                new BigDecimal("2000"),
                TradeSide.BUY,
                timestamp);
    }

    private static final class RecordingTradeStream implements RecoverableMarketTradeStream {
        private MarketTradeBatchHandler handler;
        private int acknowledgements;

        @Override
        public void onError(Consumer<Throwable> handler) {}

        @Override
        public void start(Consumer<MarketTrade> handler) {
            throw new AssertionError("legacy start must not be used");
        }

        @Override
        public void startRecoverable(MarketTradeBatchHandler handler) {
            this.handler = handler;
        }

        @Override
        public boolean isConnected() {
            return true;
        }

        @Override
        public void close() {}

        private MarketTradeAcknowledgement emit(MarketTradeBatch batch) throws Exception {
            MarketTradeAcknowledgement acknowledgement = handler.handle(batch);
            if (acknowledgement == MarketTradeAcknowledgement.ACKNOWLEDGED) {
                acknowledgements++;
            }
            return acknowledgement;
        }
    }
}
