package com.qtsurfer.qtstreamx.exchange.kraken;

import com.qtsurfer.qtstreamx.core.model.Instrument;

/**
 * Kraken symbol conversion. Two distinct conventions split across the spot and futures APIs.
 *
 * <ul>
 *   <li><b>Spot v2</b> uses slash-separated pairs ({@code BTC/USD}, {@code ETH/USDT}). Happily
 *       the qtstreamx {@link Instrument} already is {@code base/quote}, so
 *       {@link #toSpotSymbol(Instrument)} is a cheap concat.
 *   <li><b>Futures v1 / v3</b> uses compact prefixed codes: {@code PF_XBTUSD} (perpetual
 *       future, BTC/USD), {@code PI_XBTUSD} (inverse perpetual), {@code FF_ETHUSD} (fixed-
 *       expiry future). {@link #toFuturesProductId(Instrument)} only handles linear USDⓈ-M
 *       perps today, which is the subset the NATS frate path needs. {@code XBT} is Kraken's
 *       alias for {@code BTC} and gets rewritten at the boundary.
 * </ul>
 */
final class KrakenSymbols {

    private KrakenSymbols() {}

    /** {@code BTC/USD → BTC/USD}; {@code BTC/USDT → BTC/USDT}. */
    static String toSpotSymbol(Instrument instrument) {
        return instrument.base() + "/" + instrument.quote();
    }

    /**
     * {@code BTC/USD:USD → PF_XBTUSD}; {@code ETH/USDT:USDT → PF_ETHUSDT}.
     *
     * <p>Kraken writes BTC as {@code XBT} everywhere on the futures side for legacy reasons;
     * the mapping happens here so the rest of the codebase can stay on the ISO-4217 form.
     */
    static String toFuturesProductId(Instrument instrument) {
        String base = "BTC".equals(instrument.base()) ? "XBT" : instrument.base();
        return "PF_" + base + instrument.quote();
    }
}
