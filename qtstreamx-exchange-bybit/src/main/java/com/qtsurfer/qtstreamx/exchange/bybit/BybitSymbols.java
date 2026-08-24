package com.qtsurfer.qtstreamx.exchange.bybit;

import com.qtsurfer.qtstreamx.core.model.Instrument;

/**
 * Bybit symbol conversion for WebSocket and REST APIs.
 *
 * <p>Only converts <i>to</i> the exchange-native symbol; the reverse direction
 * is not needed because {@link BybitStreamClient} carries the originating
 * {@link Instrument} through the subscription registry. Same trade-off as
 * {@code BinanceSymbols}.
 *
 * <p>Bybit uses a single concatenated uppercase string for every category
 * (spot, linear, inverse): {@code BTCUSDT}. The category distinguishes spot
 * from linear perps at the URL/REST level, not in the symbol itself.
 */
final class BybitSymbols {

    private BybitSymbols() {}

    /** {@code BTC/USDT → BTCUSDT}, {@code BTC/USDT:USDT → BTCUSDT}. */
    static String toStream(Instrument instrument) {
        return (instrument.base() + instrument.quote()).toUpperCase();
    }
}
