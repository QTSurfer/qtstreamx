package com.qtsurfer.qtstreamx.exchange.bitget;

import com.qtsurfer.qtstreamx.core.model.Instrument;

/**
 * Bitget symbol conversion. v2 API uses a single compact uppercase concat for every instType:
 * {@code BTCUSDT} on SPOT, USDT-FUTURES and USDC-FUTURES alike. {@code instType} is the
 * disambiguator on the wire, not the symbol itself.
 */
final class BitgetSymbols {

    private BitgetSymbols() {}

    /** {@code BTC/USDT → BTCUSDT}; {@code BTC/USDT:USDT → BTCUSDT}. */
    static String toInstId(Instrument instrument) {
        return (instrument.base() + instrument.quote()).toUpperCase();
    }
}
