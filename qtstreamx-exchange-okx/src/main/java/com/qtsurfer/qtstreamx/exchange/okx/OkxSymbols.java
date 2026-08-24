package com.qtsurfer.qtstreamx.exchange.okx;

import com.qtsurfer.qtstreamx.core.model.Instrument;

/**
 * OKX symbol conversion. OKX uses dashed uppercase symbols:
 *
 * <ul>
 *   <li>spot: {@code BTC-USDT}
 *   <li>USDⓈ-M perpetual swap: {@code BTC-USDT-SWAP}
 *   <li>coin-margined perpetual swap: {@code BTC-USD-SWAP}
 * </ul>
 *
 * <p>We infer the type from {@link Instrument#settle()}: {@code null} is spot, otherwise perp.
 * The rare OKX futures-with-expiry contracts ({@code BTC-USD-210625}) aren't supported by this
 * mapping — add them explicitly when a consumer asks for them.
 */
final class OkxSymbols {

    private OkxSymbols() {}

    /** {@code BTC/USDT → BTC-USDT}, {@code BTC/USDT:USDT → BTC-USDT-SWAP}. */
    static String toInstId(Instrument instrument) {
        String base = instrument.base();
        String quote = instrument.quote();
        if (instrument.settle() == null) {
            return base + "-" + quote;
        }
        return base + "-" + quote + "-SWAP";
    }
}
