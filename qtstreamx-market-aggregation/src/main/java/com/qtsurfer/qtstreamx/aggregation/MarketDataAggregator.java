package com.qtsurfer.qtstreamx.aggregation;

import com.qtsurfer.qtstreamx.core.model.Kline;
import com.qtsurfer.qtstreamx.core.model.MarketId;
import com.qtsurfer.qtstreamx.core.model.MarketKline;
import com.qtsurfer.qtstreamx.core.model.MarketTicker;
import com.qtsurfer.qtstreamx.core.model.MarketTrade;
import com.qtsurfer.qtstreamx.core.model.Ticker;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

/** Projects ordered normalized trades into market-scoped ticker and candle output. */
public final class MarketDataAggregator {
    private final CandleInterval interval;
    private final Consumer<MarketTicker> tickerHandler;
    private final Consumer<MarketKline> klineHandler;
    private final Map<MarketId, CandleBucket> buckets = new HashMap<>();
    private long watermark = Long.MIN_VALUE;

    /**
     * Creates a deterministic event-time aggregator.
     *
     * @param interval fixed candle interval
     * @param tickerHandler receives one ticker per accepted trade
     * @param klineHandler receives live and closed candle states
     * @throws NullPointerException if any argument is null
     */
    public MarketDataAggregator(
            CandleInterval interval,
            Consumer<MarketTicker> tickerHandler,
            Consumer<MarketKline> klineHandler) {
        this.interval = Objects.requireNonNull(interval, "interval");
        this.tickerHandler = Objects.requireNonNull(tickerHandler, "tickerHandler");
        this.klineHandler = Objects.requireNonNull(klineHandler, "klineHandler");
    }

    /**
     * Accepts one ordered normalized trade.
     *
     * @param trade confirmed normalized trade
     * @throws NullPointerException if trade is null
     * @throws IllegalArgumentException if the trade is late, duplicate, or out of order
     */
    public void accept(MarketTrade trade) {
        Objects.requireNonNull(trade, "trade");
        if (trade.timestamp() <= watermark) {
            throw new IllegalArgumentException("market event is at or behind the watermark");
        }
        CandleBucket bucket = buckets.get(trade.market());
        if (bucket != null && bucket.eventIds.contains(trade.eventId())) {
            throw new IllegalArgumentException("duplicate market event " + trade.eventId());
        }
        if (bucket != null && trade.timestamp() < bucket.lastTimestamp) {
            throw new IllegalArgumentException("market event is out of order");
        }
        Ticker ticker = new Ticker(
                trade.market().instrument(),
                null,
                null,
                null,
                null,
                trade.price(),
                null,
                null,
                null,
                null,
                null,
                trade.timestamp());
        tickerHandler.accept(new MarketTicker(trade.market(), ticker));
        if (bucket == null) {
            bucket = CandleBucket.open(trade, interval);
            buckets.put(trade.market(), bucket);
        } else if (trade.timestamp() > bucket.closeTime) {
            klineHandler.accept(new MarketKline(
                    trade.market(), bucket.toKline(trade.market(), interval, true)));
            bucket = CandleBucket.open(trade, interval);
            buckets.put(trade.market(), bucket);
        } else {
            bucket.add(trade);
        }
        klineHandler.accept(new MarketKline(
                trade.market(), bucket.toKline(trade.market(), interval, false)));
    }

    /**
     * Advances the inclusive event-time watermark and closes elapsed quiet buckets.
     *
     * @param timestamp epoch-microsecond watermark
     * @throws IllegalArgumentException if the watermark would decrease
     */
    public void advanceWatermark(long timestamp) {
        if (timestamp < watermark) {
            throw new IllegalArgumentException("watermark cannot decrease");
        }
        watermark = timestamp;
        Iterator<Map.Entry<MarketId, CandleBucket>> iterator = buckets.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<MarketId, CandleBucket> entry = iterator.next();
            CandleBucket bucket = entry.getValue();
            if (bucket.closeTime <= watermark) {
                klineHandler.accept(new MarketKline(
                        entry.getKey(), bucket.toKline(entry.getKey(), interval, true)));
                iterator.remove();
            }
        }
    }

    private static final class CandleBucket {
        private final long openTime;
        private final long closeTime;
        private final BigDecimal open;
        private final Set<String> eventIds = new HashSet<>();
        private BigDecimal high;
        private BigDecimal low;
        private BigDecimal close;
        private BigDecimal baseVolume;
        private BigDecimal quoteVolume;
        private long numberOfTrades;
        private long lastTimestamp;

        private CandleBucket(long openTime, long closeTime, MarketTrade trade) {
            this.openTime = openTime;
            this.closeTime = closeTime;
            open = trade.price();
            high = trade.price();
            low = trade.price();
            close = trade.price();
            baseVolume = trade.baseAmount();
            quoteVolume = trade.quoteAmount();
            numberOfTrades = 1;
            lastTimestamp = trade.timestamp();
            eventIds.add(trade.eventId());
        }

        private static CandleBucket open(MarketTrade trade, CandleInterval interval) {
            long openTime = trade.timestamp() / interval.durationMicros() * interval.durationMicros();
            long closeTime = Math.addExact(openTime, interval.durationMicros() - 1);
            return new CandleBucket(openTime, closeTime, trade);
        }

        private void add(MarketTrade trade) {
            high = high.max(trade.price());
            low = low.min(trade.price());
            close = trade.price();
            baseVolume = baseVolume.add(trade.baseAmount());
            quoteVolume = quoteVolume.add(trade.quoteAmount());
            numberOfTrades++;
            lastTimestamp = trade.timestamp();
            eventIds.add(trade.eventId());
        }

        private Kline toKline(MarketId market, CandleInterval interval, boolean closed) {
            return new Kline(
                    market.instrument(),
                    interval.name(),
                    open,
                    high,
                    low,
                    close,
                    baseVolume,
                    quoteVolume,
                    numberOfTrades,
                    closed,
                    openTime,
                    closeTime);
        }
    }
}
