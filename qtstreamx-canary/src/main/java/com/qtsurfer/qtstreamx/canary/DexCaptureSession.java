package com.qtsurfer.qtstreamx.canary;

import com.qtsurfer.qtstreamx.aggregation.CandleInterval;
import com.qtsurfer.qtstreamx.aggregation.MarketDataAggregator;
import com.qtsurfer.qtstreamx.core.client.MarketTradeAcknowledgement;
import com.qtsurfer.qtstreamx.core.client.MarketTradeBatch;
import com.qtsurfer.qtstreamx.core.client.RecoverableMarketTradeStream;
import com.qtsurfer.qtstreamx.core.model.MarketTrade;
import com.qtsurfer.qtstreamx.core.model.MarketId;
import com.qtsurfer.qtstreamx.dex.capture.csv.CsvMarketTradeSink;
import java.nio.file.Path;
import java.util.Objects;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/** Assembles a normalized trade stream, aggregator, and artifact recorder. */
final class DexCaptureSession implements AutoCloseable {
    private final RecoverableMarketTradeStream marketDataStream;
    private final MarketDataAggregator aggregator;
    private final MarketDataAggregator validator;
    private final DexCaptureRecorder recorder;
    private final CsvMarketTradeSink csvSink;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    DexCaptureSession(
            RecoverableMarketTradeStream marketDataStream,
            CandleInterval interval,
            Path outputDirectory) throws Exception {
        this(marketDataStream, interval, outputDirectory, null, null, null);
    }

    DexCaptureSession(
            RecoverableMarketTradeStream marketDataStream,
            CandleInterval interval,
            Path outputDirectory,
            DiscoveryCaptureReport discoveryReport) throws Exception {
        this(marketDataStream, interval, outputDirectory, discoveryReport, null, null);
    }

    DexCaptureSession(
            RecoverableMarketTradeStream marketDataStream,
            CandleInterval interval,
            Path outputDirectory,
            Path csvOutput,
            MarketId csvMarket) throws Exception {
        this(marketDataStream, interval, outputDirectory, null, csvOutput, csvMarket);
    }

    private DexCaptureSession(
            RecoverableMarketTradeStream marketDataStream,
            CandleInterval interval,
            Path outputDirectory,
            DiscoveryCaptureReport discoveryReport,
            Path csvOutput,
            MarketId csvMarket) throws Exception {
        this.marketDataStream = Objects.requireNonNull(marketDataStream, "marketDataStream");
        Objects.requireNonNull(interval, "interval");
        recorder = new DexCaptureRecorder(
                Objects.requireNonNull(outputDirectory, "outputDirectory"), discoveryReport);
        csvSink = csvOutput == null ? null : new CsvMarketTradeSink(csvOutput, Objects.requireNonNull(csvMarket, "csvMarket"));
        aggregator = new MarketDataAggregator(
                interval,
                recorder::recordTicker,
                recorder::recordKline);
        validator = new MarketDataAggregator(interval, ignored -> {}, ignored -> {});
        for (var trade : recorder.existingTrades()) {
            validator.accept(trade);
            aggregator.accept(trade);
        }
        marketDataStream.onError(recorder::recordError);
    }

    void start() throws Exception {
        begin();
        marketDataStream.start(this::acceptTrade);
    }

    /** Starts capture with explicit acknowledgement after all batch effects complete. */
    void startRecoverable() throws Exception {
        begin();
        marketDataStream.startRecoverable(batch -> {
            var newTrades = batch.trades().stream()
                    .filter(trade -> !recorder.containsTrade(trade.eventId()))
                    .toList();
            for (var trade : newTrades) {
                validator.accept(trade);
            }
            for (var trade : newTrades) {
                if (recorder.recordTrade(trade)) {
                    aggregator.accept(trade);
                }
            }
            if (csvSink != null) {
                csvSink.handle(batch);
            }
            return MarketTradeAcknowledgement.ACKNOWLEDGED;
        });
    }

    boolean isConnected() {
        return marketDataStream.isConnected();
    }

    long tradeCount() {
        return recorder.tradeCount();
    }

    void advanceWatermark(long timestamp) {
        validator.advanceWatermark(timestamp);
        aggregator.advanceWatermark(timestamp);
    }

    private void acceptTrade(MarketTrade trade) {
        if (recorder.containsTrade(trade.eventId())) {
            return;
        }
        validator.accept(trade);
        if (recorder.recordTrade(trade)) {
            aggregator.accept(trade);
            if (csvSink != null) {
                try {
                    csvSink.handle(new MarketTradeBatch(List.of(trade)));
                } catch (Exception exception) {
                    throw new IllegalStateException("CSV capture write failed", exception);
                }
            }
        }
    }

    private void begin() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("capture session has already started");
        }
        recorder.recordLifecycle("started");
    }

    @Override
    public void close() throws Exception {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        Exception failure = null;
        try {
            marketDataStream.close();
        } catch (Exception exception) {
            failure = exception;
        }
        try {
            aggregator.advanceWatermark(Long.MAX_VALUE);
            recorder.recordLifecycle("completed");
            recorder.close();
            if (csvSink != null) {
                csvSink.close();
            }
        } catch (Exception exception) {
            if (failure == null) {
                failure = exception;
            } else {
                failure.addSuppressed(exception);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
