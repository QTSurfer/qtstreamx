package com.qtsurfer.qtstreamx.canary;

import com.qtsurfer.qtstreamx.core.client.StreamClient;
import com.qtsurfer.qtstreamx.core.client.StreamClientConfig;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.ws.WebSocketClient;
import com.qtsurfer.qtstreamx.exchange.binance.BinanceStreamClient;
import com.qtsurfer.qtstreamx.exchange.bitget.BitgetStreamClient;
import com.qtsurfer.qtstreamx.exchange.bybit.BybitStreamClient;
import com.qtsurfer.qtstreamx.exchange.gateio.GateioFuturesStreamClient;
import com.qtsurfer.qtstreamx.exchange.gateio.GateioSpotStreamClient;
import com.qtsurfer.qtstreamx.exchange.htx.HtxLinearSwapStreamClient;
import com.qtsurfer.qtstreamx.exchange.htx.HtxSpotStreamClient;
import com.qtsurfer.qtstreamx.exchange.kraken.KrakenFuturesStreamClient;
import com.qtsurfer.qtstreamx.exchange.kraken.KrakenSpotStreamClient;
import com.qtsurfer.qtstreamx.exchange.okx.OkxStreamClient;
import com.qtsurfer.qtstreamx.ws.jdk.GzipAwareJdkWebSocketClient;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Canary WS capture tool. Connects to an exchange's public WS, subscribes to ticker + kline +
 * funding-rate for a small set of symbols, and writes both raw frames and parsed records to
 * JSONL files. Runs for a fixed duration, then closes cleanly.
 *
 * <p>Zero QDB, zero NATS — pure offline capture. Safe to run from any host with internet.
 *
 * <pre>
 * --exchange   binance-spot|binance-futures|bybit-spot|bybit-linear|okx
 *              |kraken-spot|kraken-futures|bitget-spot|bitget-futures
 *              |gateio-spot|gateio-futures|htx-spot|htx-linear
 * --symbols    comma list of CCXT-style symbols, e.g. BTC/USDT,ETH/USDT,BTC/USDT:USDT
 * --duration   minutes (default 5)
 * --interval   kline interval string native to each exchange (default 1m/1min etc.)
 * --out        output directory (default /tmp/canary/&lt;exchange&gt;)
 * </pre>
 */
public class CaptureMain {

    private static final Logger log = LoggerFactory.getLogger(CaptureMain.class);

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = parseArgs(args);
        String exchange = require(opts, "exchange").toLowerCase(Locale.ROOT);
        String symbolsStr = require(opts, "symbols");
        int durationMin = Integer.parseInt(opts.getOrDefault("duration", "5"));
        String interval = opts.getOrDefault("interval", defaultInterval(exchange));
        Path outDir = Path.of(opts.getOrDefault("out", "/tmp/canary/" + exchange));

        List<Instrument> instruments = Arrays.stream(symbolsStr.split(","))
                .map(String::trim).filter(s -> !s.isEmpty())
                .map(Instrument::parse).toList();

        log.info("Capture starting: exchange={} symbols={} duration={}min interval={} out={}",
                exchange, instruments, durationMin, interval, outDir);

        try (FrameRecorder recorder = new FrameRecorder(outDir)) {
            Supplier<WebSocketClient> factory = () -> new RecordingWebSocketClient(
                    new GzipAwareJdkWebSocketClient(), recorder, exchange);
            StreamClientConfig config = StreamClientConfig.withDefaults(factory);

            StreamClient client = buildClient(exchange, config);

            for (Instrument inst : instruments) {
                // Ticker + kline always; funding rate only for perps.
                client.subscribeTicker(inst, recorder::recordTicker);
                try {
                    client.subscribeKline(inst, interval, recorder::recordKline);
                } catch (UnsupportedOperationException e) {
                    log.warn("subscribeKline not supported for {}: {}", exchange, e.getMessage());
                }
                if (inst.settle() != null) {
                    try {
                        client.subscribeFundingRate(inst, recorder::recordFundingRate);
                    } catch (UnsupportedOperationException e) {
                        log.warn("subscribeFundingRate not supported for {} / {}: {}",
                                exchange, inst, e.getMessage());
                    }
                }
            }

            AtomicBoolean shuttingDown = new AtomicBoolean(false);
            AtomicInteger reconnects = new AtomicInteger(0);
            ScheduledExecutorService reconnectExec = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "canary-reconnect");
                t.setDaemon(true);
                return t;
            });
            client.onDisconnect(() -> {
                if (shuttingDown.get()) return;
                int n = reconnects.incrementAndGet();
                long delay = Math.min(30, 3L * n); // 3s, 6s, 9s, ... cap 30s
                log.warn("{} WS disconnected, reconnect #{} in {}s", exchange, n, delay);
                reconnectExec.schedule(() -> {
                    if (shuttingDown.get()) return;
                    try {
                        client.connect();
                        log.info("{} WS reconnected (#{})", exchange, n);
                    } catch (Exception e) {
                        log.error("{} reconnect #{} failed: {}", exchange, n, e.getMessage());
                    }
                }, delay, TimeUnit.SECONDS);
            });
            client.connect();
            log.info("Connected; running for {} minutes", durationMin);

            long durationMs = durationMin * 60_000L;
            long end = System.currentTimeMillis() + durationMs;
            long lastReport = System.currentTimeMillis();
            while (System.currentTimeMillis() < end) {
                Thread.sleep(5_000);
                long now = System.currentTimeMillis();
                if (now - lastReport >= 30_000) {
                    log.info("Capture progress: raw={} parsed={} reconnects={} remainingSec={}",
                            recorder.rawLines(), recorder.parsedLines(),
                            reconnects.get(), (end - now) / 1000);
                    lastReport = now;
                }
            }
            shuttingDown.set(true);
            reconnectExec.shutdownNow();
            client.close();
            Thread.sleep(500); // allow trailing frames to flush via recorder
            log.info("Capture done: raw={} parsed={} reconnects={}",
                    recorder.rawLines(), recorder.parsedLines(), reconnects.get());
        }
    }

    static String defaultInterval(String exchange) {
        // Each exchange has its own kline-interval spelling for 1 minute.
        if (exchange.startsWith("htx-")) return "1min";
        if (exchange.startsWith("bybit-")) return "1";         // v5: just the number
        if (exchange.equals("okx")) return "1m";
        if (exchange.startsWith("kraken-")) return "1";        // minutes for both spot + futures
        if (exchange.startsWith("gateio-")) return "1m";
        if (exchange.startsWith("bitget-")) return "1m";
        return "1m";
    }

    static StreamClient buildClient(String exchange, StreamClientConfig config) {
        return switch (exchange) {
            case "binance-spot" -> BinanceStreamClient.spot(config);
            case "binance-futures" -> BinanceStreamClient.futures(config);
            case "bybit-spot" -> BybitStreamClient.spot(config);
            case "bybit-linear" -> BybitStreamClient.linear(config);
            case "okx" -> new OkxStreamClient(config);
            case "kraken-spot" -> new KrakenSpotStreamClient(config);
            case "kraken-futures" -> new KrakenFuturesStreamClient(config);
            case "bitget-spot" -> BitgetStreamClient.spot(config);
            case "bitget-futures" -> BitgetStreamClient.usdtFutures(config);
            case "gateio-spot" -> new GateioSpotStreamClient(config);
            case "gateio-futures" -> new GateioFuturesStreamClient(config);
            case "htx-spot" -> new HtxSpotStreamClient(config);
            case "htx-linear" -> new HtxLinearSwapStreamClient(config);
            default -> throw new IllegalArgumentException("Unknown exchange: " + exchange);
        };
    }

    static Map<String, String> parseArgs(String[] args) {
        Map<String, String> out = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (a.startsWith("--") && i + 1 < args.length) {
                out.put(a.substring(2), args[i + 1]);
                i++;
            }
        }
        return out;
    }

    private static String require(Map<String, String> opts, String key) {
        String v = opts.get(key);
        if (v == null || v.isBlank()) {
            throw new IllegalArgumentException("--" + key + " is required");
        }
        return v;
    }
}
