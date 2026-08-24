package com.qtsurfer.qtstreamx.exchange.binance;

import com.qtsurfer.qtstreamx.core.model.Instrument;

/**
 * Binance symbol conversion for WebSocket streams.
 *
 * <p>Only converts <i>to</i> the exchange-native symbol; the reverse direction
 * is not needed because {@link BinanceStreamClient} carries the originating
 * {@link Instrument} through the subscription registry and passes it directly
 * to {@link BinanceAdapters}. Parsing a concat'd symbol like {@code BTCUSDT}
 * back into base/quote requires either a quote-suffix heuristic (which
 * misfires on new fiat listings like {@code HEITRY}, {@code PEPEBRL}) or the
 * {@code exchangeInfo} REST endpoint (which is already the source of truth
 * feeding the subscription in the first place) — so we avoid that round trip
 * entirely.
 */
final class BinanceSymbols {

    private BinanceSymbols() {}

    /**
     * Convert a QTStreamX Instrument to a Binance WS symbol (lowercase).
     * <p>{@code BTC/USDT → btcusdt}, {@code BTC/USDT:USDT → btcusdt}
     */
    static String toStream(Instrument instrument) {
        return (instrument.base() + instrument.quote()).toLowerCase();
    }
}
