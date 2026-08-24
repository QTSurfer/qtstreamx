package com.qtsurfer.qtstreamx.canary;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Analyse two capture dirs (reference + target) and emit a Markdown comparison report.
 *
 * <p>Reference is expected to be the Binance capture for the same symbols and window; target is
 * the exchange being validated. The report covers:
 * <ul>
 *   <li>Event counts per kind (ticker/kline/frate) per instrument.
 *   <li>Events per minute on each side.
 *   <li>Price drift: median and p95 of |last_target - last_reference| / last_reference over
 *       the overlapping window, using nearest-in-time pairing.
 *   <li>Parse yield: parsed.jsonl lines ÷ raw inbound frames (proxy for adapter coverage).
 *   <li>Raw frame mix: inbound / outbound / lifecycle counts.
 * </ul>
 *
 * <pre>
 * --reference /tmp/canary/binance-spot
 * --target    /tmp/canary/bybit-spot
 * --report    /tmp/canary/report.md
 * </pre>
 */
public class AnalyzeMain {

    private static final Logger log = LoggerFactory.getLogger(AnalyzeMain.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = CaptureMain.parseArgs(args);
        Path reference = Path.of(required(opts, "reference"));
        Path target = Path.of(required(opts, "target"));
        Path report = Path.of(opts.getOrDefault("report", "/tmp/canary/report.md"));

        log.info("Analyzing reference={} target={}", reference, target);

        CaptureStats refStats = loadCapture(reference);
        CaptureStats tgtStats = loadCapture(target);

        String md = buildReport(reference, target, refStats, tgtStats);
        Files.writeString(report, md, StandardCharsets.UTF_8);
        log.info("Report written: {} ({} bytes)", report, md.length());
    }

    static CaptureStats loadCapture(Path dir) throws Exception {
        CaptureStats stats = new CaptureStats();
        Path parsed = dir.resolve("parsed.jsonl");
        Path raw = dir.resolve("raw.jsonl");
        if (!Files.exists(parsed)) throw new IllegalStateException("missing " + parsed);

        try (BufferedReader br = Files.newBufferedReader(parsed, StandardCharsets.UTF_8)) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isBlank()) continue;
                JsonNode n = MAPPER.readTree(line);
                String kind = n.path("kind").asText();
                String inst = n.path("instrument").asText();
                long ts = n.path("ts").asLong();
                stats.parsedCount.merge(keyOf(kind, inst), 1L, Long::sum);
                stats.updateWindow(ts);
                if ("ticker".equals(kind)) {
                    JsonNode data = n.path("data");
                    String last = data.path("last").asText("");
                    if (!last.isEmpty() && !"null".equals(last)) {
                        stats.tickerSeries
                                .computeIfAbsent(inst, k -> new ArrayList<>())
                                .add(new PricePoint(ts, new BigDecimal(last)));
                    }
                }
            }
        }

        if (Files.exists(raw)) {
            try (BufferedReader br = Files.newBufferedReader(raw, StandardCharsets.UTF_8)) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.isBlank()) continue;
                    JsonNode n = MAPPER.readTree(line);
                    String dir2 = n.path("dir").asText();
                    if (dir2.startsWith("lifecycle")) stats.rawLifecycle++;
                    else if ("in".equals(dir2)) stats.rawInbound++;
                    else if ("out".equals(dir2)) stats.rawOutbound++;
                }
            }
        }
        // Sort each series by timestamp for drift pairing.
        stats.tickerSeries.values().forEach(s -> s.sort(Comparator.comparingLong(p -> p.ts)));
        return stats;
    }

    static String buildReport(Path ref, Path tgt, CaptureStats r, CaptureStats t) {
        StringBuilder sb = new StringBuilder();
        sb.append("# Canary comparison report\n\n");
        sb.append("- **Reference**: `").append(ref).append("`\n");
        sb.append("- **Target**:    `").append(tgt).append("`\n\n");
        sb.append(windowLine("Reference window", r));
        sb.append(windowLine("Target window   ", t));
        sb.append('\n');

        sb.append("## Raw frame mix\n\n");
        sb.append("| side | inbound | outbound | lifecycle |\n");
        sb.append("|------|---------|----------|-----------|\n");
        sb.append(String.format("| ref  | %d | %d | %d |%n", r.rawInbound, r.rawOutbound, r.rawLifecycle));
        sb.append(String.format("| tgt  | %d | %d | %d |%n", t.rawInbound, t.rawOutbound, t.rawLifecycle));
        sb.append('\n');

        // Collect all (kind, instrument) keys across both.
        TreeMap<String, long[]> rowCounts = new TreeMap<>();
        r.parsedCount.forEach((k, v) -> rowCounts.computeIfAbsent(k, x -> new long[2])[0] = v);
        t.parsedCount.forEach((k, v) -> rowCounts.computeIfAbsent(k, x -> new long[2])[1] = v);

        sb.append("## Parsed events per kind × instrument\n\n");
        sb.append("| kind | instrument | ref count | ref /min | tgt count | tgt /min | delta |\n");
        sb.append("|------|------------|-----------|----------|-----------|----------|-------|\n");
        double refMin = Math.max(1, (double) r.durationMs() / 60_000d);
        double tgtMin = Math.max(1, (double) t.durationMs() / 60_000d);
        for (Map.Entry<String, long[]> e : rowCounts.entrySet()) {
            String[] parts = e.getKey().split("\\|", 2);
            long rc = e.getValue()[0], tc = e.getValue()[1];
            sb.append(String.format("| %s | %s | %d | %.1f | %d | %.1f | %+d |%n",
                    parts[0], parts[1], rc, rc / refMin, tc, tc / tgtMin, tc - rc));
        }
        sb.append('\n');

        sb.append("## Last-price drift (|tgt - ref| / ref)\n\n");
        sb.append("Per-instrument drift using nearest-neighbour pairing within ±2s.\n\n");
        sb.append("| instrument | pairs | median | p95 | max |\n");
        sb.append("|------------|-------|--------|-----|-----|\n");
        for (String inst : r.tickerSeries.keySet()) {
            List<PricePoint> refS = r.tickerSeries.get(inst);
            List<PricePoint> tgtS = t.tickerSeries.get(inst);
            if (refS == null || tgtS == null) continue;
            List<Double> drifts = drift(refS, tgtS, 2_000_000L);
            if (drifts.isEmpty()) {
                sb.append(String.format("| %s | 0 | — | — | — |%n", inst));
                continue;
            }
            drifts.sort(Double::compareTo);
            double median = percentile(drifts, 0.5);
            double p95 = percentile(drifts, 0.95);
            double max = drifts.get(drifts.size() - 1);
            sb.append(String.format("| %s | %d | %.4f%% | %.4f%% | %.4f%% |%n",
                    inst, drifts.size(), median * 100, p95 * 100, max * 100));
        }
        sb.append('\n');

        sb.append("## Adapter yield (parsed ÷ inbound frames)\n\n");
        sb.append("> Values >100% are normal for exchanges that batch multiple records per frame "
                + "(e.g. Bitget/Gate.io kline snapshots, Kraken OHLC pushes). Values <100% suggest "
                + "control frames, acks, or dropped events.\n\n");
        double refYield = r.rawInbound == 0 ? 0 : (double) r.totalParsed() / r.rawInbound;
        double tgtYield = t.rawInbound == 0 ? 0 : (double) t.totalParsed() / t.rawInbound;
        sb.append(String.format("- Reference: parsed=%d / inbound=%d = **%.1f%%**%n",
                r.totalParsed(), r.rawInbound, refYield * 100));
        sb.append(String.format("- Target:    parsed=%d / inbound=%d = **%.1f%%**%n",
                t.totalParsed(), t.rawInbound, tgtYield * 100));
        return sb.toString();
    }

    static List<Double> drift(List<PricePoint> ref, List<PricePoint> tgt, long windowMicros) {
        List<Double> out = new ArrayList<>();
        int j = 0;
        for (PricePoint r : ref) {
            // advance j while tgt[j].ts is too far behind r.ts - window
            while (j < tgt.size() - 1 && tgt.get(j + 1).ts <= r.ts + windowMicros) j++;
            PricePoint candidate = tgt.get(j);
            // pick the closer neighbour if possible
            if (j + 1 < tgt.size()
                    && Math.abs(tgt.get(j + 1).ts - r.ts) < Math.abs(candidate.ts - r.ts)) {
                candidate = tgt.get(j + 1);
            }
            if (Math.abs(candidate.ts - r.ts) > windowMicros) continue;
            if (r.price.signum() == 0) continue;
            double d = candidate.price.subtract(r.price).abs()
                    .divide(r.price, 10, java.math.RoundingMode.HALF_UP).doubleValue();
            out.add(d);
        }
        return out;
    }

    static double percentile(List<Double> sorted, double p) {
        if (sorted.isEmpty()) return 0;
        int idx = (int) Math.floor(p * (sorted.size() - 1));
        return sorted.get(Math.min(idx, sorted.size() - 1));
    }

    static String keyOf(String kind, String instrument) { return kind + "|" + instrument; }

    private static String windowLine(String label, CaptureStats s) {
        if (s.minTs == Long.MAX_VALUE) return "- " + label + ": (no events)\n";
        return String.format("- %s: %d events spanning %.1fs%n",
                label, s.totalParsed(), (s.maxTs - s.minTs) / 1_000_000d);
    }

    private static String required(Map<String, String> opts, String key) {
        String v = opts.get(key);
        if (v == null || v.isBlank()) throw new IllegalArgumentException("--" + key + " required");
        return v;
    }

    static final class CaptureStats {
        final Map<String, Long> parsedCount = new LinkedHashMap<>();
        final Map<String, List<PricePoint>> tickerSeries = new HashMap<>();
        long minTs = Long.MAX_VALUE, maxTs = Long.MIN_VALUE;
        long rawInbound, rawOutbound, rawLifecycle;

        void updateWindow(long ts) {
            if (ts < minTs) minTs = ts;
            if (ts > maxTs) maxTs = ts;
        }

        long totalParsed() { return parsedCount.values().stream().mapToLong(Long::longValue).sum(); }

        long durationMs() {
            if (minTs == Long.MAX_VALUE) return 0;
            return (maxTs - minTs) / 1_000L;
        }
    }

    record PricePoint(long ts, BigDecimal price) {}
}
