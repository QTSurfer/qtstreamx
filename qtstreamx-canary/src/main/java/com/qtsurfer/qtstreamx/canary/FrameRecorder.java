package com.qtsurfer.qtstreamx.canary;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.qtsurfer.qtstreamx.core.model.FundingRate;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.model.Kline;
import com.qtsurfer.qtstreamx.core.model.Ticker;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Two-file JSONL writer:
 * <ul>
 *   <li>{@code raw.jsonl} — one line per WS frame (decoded text). Shape:
 *       {@code {"ts":<micros>,"dir":"in|out|lifecycle","endpoint":"<tag>","msg":"..."}}
 *   <li>{@code parsed.jsonl} — one line per parsed record from an adapter. Shape:
 *       {@code {"ts":<micros>,"kind":"ticker|kline|frate","instrument":"BTC/USDT[:USDT]","data":{...}}}
 * </ul>
 *
 * Thread-safe via synchronization on each writer. Not a performance-critical path — capture is
 * I/O bound on the WS anyway; we optimise for readability of the dump.
 */
final class FrameRecorder implements AutoCloseable {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final BufferedWriter rawOut;
    private final BufferedWriter parsedOut;
    private final AtomicLong rawLines = new AtomicLong();
    private final AtomicLong parsedLines = new AtomicLong();

    FrameRecorder(Path outDir) throws IOException {
        Files.createDirectories(outDir);
        this.rawOut = Files.newBufferedWriter(
                outDir.resolve("raw.jsonl"),
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        this.parsedOut = Files.newBufferedWriter(
                outDir.resolve("parsed.jsonl"),
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    long rawLines() { return rawLines.get(); }
    long parsedLines() { return parsedLines.get(); }

    void recordInbound(String endpointTag, String message) {
        writeRaw(endpointTag, "in", message);
    }

    void recordOutbound(String endpointTag, String message) {
        writeRaw(endpointTag, "out", message);
    }

    void recordLifecycle(String endpointTag, String event, String detail) {
        writeRaw(endpointTag, "lifecycle:" + event, detail);
    }

    void recordTicker(Ticker t) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("bid", str(t.bid()));
        data.put("bidSize", str(t.bidSize()));
        data.put("ask", str(t.ask()));
        data.put("askSize", str(t.askSize()));
        data.put("last", str(t.last()));
        data.put("open", str(t.open()));
        data.put("high", str(t.high()));
        data.put("low", str(t.low()));
        data.put("volume", str(t.volume()));
        data.put("quoteVolume", str(t.quoteVolume()));
        data.put("timestamp", t.timestamp());
        writeParsed("ticker", t.instrument(), data);
    }

    void recordKline(Kline k) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("interval", k.interval());
        data.put("open", str(k.open()));
        data.put("high", str(k.high()));
        data.put("low", str(k.low()));
        data.put("close", str(k.close()));
        data.put("volume", str(k.volume()));
        data.put("quoteVolume", str(k.quoteVolume()));
        data.put("trades", k.numberOfTrades());
        data.put("closed", k.closed());
        data.put("timestamp", k.timestamp());
        data.put("closeTime", k.closeTime());
        writeParsed("kline", k.instrument(), data);
    }

    void recordFundingRate(FundingRate f) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("rate", str(f.rate()));
        data.put("markPrice", str(f.markPrice()));
        data.put("nextFundingTime", f.nextFundingTime());
        data.put("intervalHours", f.intervalHours());
        data.put("timestamp", f.timestamp());
        writeParsed("frate", f.instrument(), data);
    }

    private synchronized void writeRaw(String endpointTag, String dir, String message) {
        try {
            Map<String, Object> line = new LinkedHashMap<>();
            line.put("ts", System.currentTimeMillis() * 1_000L);
            line.put("dir", dir);
            line.put("endpoint", endpointTag);
            line.put("msg", message);
            rawOut.write(MAPPER.writeValueAsString(line));
            rawOut.newLine();
            rawLines.incrementAndGet();
        } catch (IOException e) {
            throw new RuntimeException("raw write failed", e);
        }
    }

    private synchronized void writeParsed(String kind, Instrument instrument, Map<String, Object> data) {
        try {
            Map<String, Object> line = new LinkedHashMap<>();
            line.put("ts", System.currentTimeMillis() * 1_000L);
            line.put("kind", kind);
            line.put("instrument", formatInstrument(instrument));
            line.put("data", data);
            parsedOut.write(MAPPER.writeValueAsString(line));
            parsedOut.newLine();
            parsedLines.incrementAndGet();
        } catch (IOException e) {
            throw new RuntimeException("parsed write failed", e);
        }
    }

    static String formatInstrument(Instrument i) {
        String base = i.base() + "/" + i.quote();
        return i.settle() == null ? base : base + ":" + i.settle();
    }

    private static String str(java.math.BigDecimal b) {
        return b == null ? null : b.toPlainString();
    }

    @Override
    public synchronized void close() throws IOException {
        try {
            rawOut.flush();
            parsedOut.flush();
        } finally {
            rawOut.close();
            parsedOut.close();
        }
    }
}
