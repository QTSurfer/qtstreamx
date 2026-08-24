package com.qtsurfer.qtstreamx.canary;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.qtsurfer.qtstreamx.core.model.MarketId;
import com.qtsurfer.qtstreamx.core.model.MarketKline;
import com.qtsurfer.qtstreamx.core.model.MarketTicker;
import com.qtsurfer.qtstreamx.core.model.MarketTrade;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Writes normalized DEX capture artifacts without serializing runtime RPC configuration. */
final class DexCaptureRecorder implements AutoCloseable {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final Path summaryPath;
    private final List<MarketTrade> existingTrades;
    private final BufferedWriter trades;
    private final FileChannel tradeSyncChannel;
    private final BufferedWriter tickers;
    private final BufferedWriter klines;
    private final BufferedWriter diagnostics;
    private final DiscoveryCaptureReport discoveryReport;
    private final Set<MarketId> markets = new LinkedHashSet<>();
    private final Set<String> tradeEventIds = new LinkedHashSet<>();
    private long tradeCount;
    private long tickerCount;
    private long klineCount;
    private long diagnosticCount;

    DexCaptureRecorder(Path outputDirectory) throws IOException {
        this(outputDirectory, null);
    }

    DexCaptureRecorder(Path outputDirectory, DiscoveryCaptureReport discoveryReport)
            throws IOException {
        Files.createDirectories(outputDirectory);
        this.discoveryReport = discoveryReport;
        summaryPath = outputDirectory.resolve("summary.json");
        Path tradesPath = outputDirectory.resolve("trades.ndjson");
        existingTrades = loadTrades(tradesPath);
        for (MarketTrade trade : existingTrades) {
            if (!tradeEventIds.add(trade.eventId())) {
                throw new IOException("DEX capture journal contains a duplicate event ID");
            }
            markets.add(trade.market());
            tradeCount++;
        }
        trades = appendWriter(tradesPath);
        tradeSyncChannel = FileChannel.open(tradesPath, StandardOpenOption.WRITE);
        tickers = writer(outputDirectory.resolve("tickers.ndjson"));
        klines = writer(outputDirectory.resolve("klines.ndjson"));
        diagnostics = writer(outputDirectory.resolve("diagnostics.ndjson"));
        if (discoveryReport != null) {
            discoveryReport.reasons().forEach(this::recordDiscoveryRejection);
        }
    }

    synchronized boolean recordTrade(MarketTrade trade) {
        if (tradeEventIds.contains(trade.eventId())) {
            return false;
        }
        write(trades, trade);
        try {
            tradeSyncChannel.force(true);
        } catch (IOException exception) {
            throw new IllegalStateException("DEX capture journal sync failed", exception);
        }
        tradeEventIds.add(trade.eventId());
        markets.add(trade.market());
        tradeCount++;
        return true;
    }

    synchronized boolean containsTrade(String eventId) {
        return tradeEventIds.contains(eventId);
    }

    List<MarketTrade> existingTrades() {
        return existingTrades;
    }

    synchronized void recordTicker(MarketTicker ticker) {
        write(tickers, ticker);
        markets.add(ticker.market());
        tickerCount++;
    }

    synchronized void recordKline(MarketKline kline) {
        write(klines, kline);
        markets.add(kline.market());
        klineCount++;
    }

    synchronized void recordLifecycle(String event) {
        writeDiagnostic("lifecycle", event, null);
    }

    synchronized void recordError(Throwable error) {
        writeDiagnostic("error", "stream-error", error.getClass().getName());
    }

    synchronized long tradeCount() {
        return tradeCount;
    }

    @Override
    public synchronized void close() throws IOException {
        IOException failure = null;
        try {
            writeSummary();
        } catch (IOException exception) {
            failure = exception;
        }
        failure = close(trades, failure);
        failure = close(tradeSyncChannel, failure);
        failure = close(tickers, failure);
        failure = close(klines, failure);
        failure = close(diagnostics, failure);
        if (failure != null) {
            throw failure;
        }
    }

    private void writeDiagnostic(String kind, String event, String errorType) {
        Map<String, Object> diagnostic = new LinkedHashMap<>();
        diagnostic.put("kind", kind);
        diagnostic.put("event", event);
        if (errorType != null) {
            diagnostic.put("errorType", errorType);
        }
        write(diagnostics, diagnostic);
        diagnosticCount++;
    }

    private void recordDiscoveryRejection(String reason, int count) {
        Map<String, Object> diagnostic = new LinkedHashMap<>();
        diagnostic.put("kind", "discovery");
        diagnostic.put("event", "rejected");
        diagnostic.put("reason", reason);
        diagnostic.put("count", count);
        write(diagnostics, diagnostic);
        diagnosticCount++;
    }

    private void writeSummary() throws IOException {
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("trades", tradeCount);
        summary.put("tickers", tickerCount);
        summary.put("klines", klineCount);
        summary.put("diagnostics", diagnosticCount);
        summary.put("markets", markets);
        if (discoveryReport != null) {
            summary.put("discovery", discoveryReport);
        }
        MAPPER.writeValue(summaryPath.toFile(), summary);
    }

    private static BufferedWriter writer(Path path) throws IOException {
        return Files.newBufferedWriter(
                path,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);
    }

    private static BufferedWriter appendWriter(Path path) throws IOException {
        return Files.newBufferedWriter(
                path,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
    }

    private static List<MarketTrade> loadTrades(Path path) throws IOException {
        if (!Files.exists(path)) {
            return List.of();
        }
        List<MarketTrade> loaded = new ArrayList<>();
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (line.isBlank()) {
                throw new IOException("DEX capture journal contains a blank record");
            }
            loaded.add(MAPPER.readValue(line, MarketTrade.class));
        }
        return List.copyOf(loaded);
    }

    private static void write(BufferedWriter writer, Object value) {
        try {
            writer.write(MAPPER.writeValueAsString(value));
            writer.newLine();
            writer.flush();
        } catch (IOException exception) {
            throw new IllegalStateException("DEX capture write failed", exception);
        }
    }

    private static IOException close(BufferedWriter writer, IOException failure) {
        try {
            writer.close();
        } catch (IOException exception) {
            if (failure == null) {
                return exception;
            }
            failure.addSuppressed(exception);
        }
        return failure;
    }

    private static IOException close(FileChannel channel, IOException failure) {
        try {
            channel.close();
        } catch (IOException exception) {
            if (failure == null) {
                return exception;
            }
            failure.addSuppressed(exception);
        }
        return failure;
    }
}
