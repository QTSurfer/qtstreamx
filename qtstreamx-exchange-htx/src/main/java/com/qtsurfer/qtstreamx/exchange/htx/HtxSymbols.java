package com.qtsurfer.qtstreamx.exchange.htx;

import com.qtsurfer.qtstreamx.core.model.Instrument;

/**
 * HTX (ex-Huobi) symbol conversion.
 *
 * <ul>
 *   <li>Spot v2 WS uses lowercase concat: {@code btcusdt}.
 *   <li>Linear swap WS uses dashed uppercase: {@code BTC-USDT}.
 * </ul>
 */
final class HtxSymbols {

    private HtxSymbols() {}

    /** {@code BTC/USDT → btcusdt}. */
    static String toSpotSymbol(Instrument instrument) {
        return (instrument.base() + instrument.quote()).toLowerCase();
    }

    /** {@code BTC/USDT:USDT → BTC-USDT}. */
    static String toLinearContract(Instrument instrument) {
        return (instrument.base() + "-" + instrument.quote()).toUpperCase();
    }
}
