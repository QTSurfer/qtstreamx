package com.qtsurfer.qtstreamx.exchange.bybit;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

    /**
     * The exchange's own symbol for instruments where it is NOT base+quote, as listed by
     * {@code instruments-info}. Bybit names its USDC-settled linear perpetuals {@code <BASE>PERP}
     * ({@code BTCPERP}, not {@code BTCUSDC}): the base+quote topic {@code tickers.BTCUSDC} does not
     * exist, and because Bybit rejects the WHOLE subscribe frame that carries an unknown topic, every
     * other instrument sharing that frame went silent with it (2026-11-24: 68 USDC perps never
     * captured, and about half of each publisher's linear frames dead). Filled by
     * {@link BybitInstrumentsCache} from the REST listing; anything not listed keeps base+quote.
     */
    private static final Map<Instrument, String> NATIVE = new ConcurrentHashMap<>();

    private BybitSymbols() {}

    /** {@code BTC/USDT → BTCUSDT}, {@code BTC/USDT:USDT → BTCUSDT}, {@code BTC/USDC:USDC → BTCPERP} once listed. */
    static String toStream(Instrument instrument) {
        String listed = NATIVE.get(instrument);
        return listed != null ? listed : derived(instrument);
    }

    /** What the exchange would call the instrument if its symbol were base+quote. */
    static String derived(Instrument instrument) {
        return (instrument.base() + instrument.quote()).toUpperCase();
    }

    /** Record the listed symbol of an instrument whose symbol is not base+quote. */
    static void registerListed(Instrument instrument, String symbol) {
        if (symbol == null || symbol.isEmpty() || symbol.equals(derived(instrument))) return;
        NATIVE.put(instrument, symbol);
    }
}
