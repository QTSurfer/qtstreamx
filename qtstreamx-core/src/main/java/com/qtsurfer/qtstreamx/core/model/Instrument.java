package com.qtsurfer.qtstreamx.core.model;

/**
 * Normalized instrument identifier using CCXT unified format.
 *
 * <p>Examples:
 * <ul>
 *   <li>Spot: {@code BTC/USDT}
 *   <li>USDT-margined perpetual: {@code BTC/USDT:USDT}
 *   <li>Coin-margined perpetual: {@code BTC/USD:BTC}
 * </ul>
 *
 * @param base  base currency (e.g. "BTC")
 * @param quote quote currency (e.g. "USDT")
 * @param settle settlement currency for derivatives, null for spot (e.g. "USDT")
 */
public record Instrument(String base, String quote, String settle) {

    public Instrument(String base, String quote) {
        this(base, quote, null);
    }

    /** Returns the CCXT-style symbol string. */
    public String symbol() {
        if (settle == null) {
            return base + "/" + quote;
        }
        return base + "/" + quote + ":" + settle;
    }

    /** Parse a CCXT-style symbol string into an Instrument. */
    public static Instrument parse(String symbol) {
        int colonIdx = symbol.indexOf(':');
        if (colonIdx >= 0) {
            String pair = symbol.substring(0, colonIdx);
            String settle = symbol.substring(colonIdx + 1);
            int slashIdx = pair.indexOf('/');
            return new Instrument(pair.substring(0, slashIdx), pair.substring(slashIdx + 1), settle);
        }
        int slashIdx = symbol.indexOf('/');
        return new Instrument(symbol.substring(0, slashIdx), symbol.substring(slashIdx + 1));
    }

    public boolean isDerivative() {
        return settle != null;
    }

    @Override
    public String toString() {
        return symbol();
    }
}
