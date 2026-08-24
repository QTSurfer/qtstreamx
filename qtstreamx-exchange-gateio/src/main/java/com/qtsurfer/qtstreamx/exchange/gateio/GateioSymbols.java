package com.qtsurfer.qtstreamx.exchange.gateio;

import com.qtsurfer.qtstreamx.core.model.Instrument;

/**
 * Gate.io symbol conversion. v4 API uses underscore-separated uppercase pairs ({@code BTC_USDT})
 * for both spot and USDⓈ-M futures. Futures tickers use the same {@code BTC_USDT} form (no
 * extra suffix); category split happens at the WS URL level.
 */
final class GateioSymbols {

    private GateioSymbols() {}

    /** {@code BTC/USDT → BTC_USDT}. Works for spot and futures alike. */
    static String toSymbol(Instrument instrument) {
        return (instrument.base() + "_" + instrument.quote()).toUpperCase();
    }
}
