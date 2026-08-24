package com.qtsurfer.qtstreamx.dex.core;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import java.util.Objects;

/**
 * Ordered EVM tokens and their independent logical spot-market orientation.
 *
 * <p>{@code token0} and {@code token1} follow ascending contract-address order.
 * The instrument may choose either token as its logical base.
 *
 * @param token0 lower-addressed token
 * @param token1 higher-addressed token
 * @param instrument normalized logical base/quote instrument
 */
public record EvmTokenPair(EvmToken token0, EvmToken token1, Instrument instrument) {

    /**
     * Validates token order and logical market orientation.
     *
     * @throws NullPointerException if a token or the instrument is null
     * @throws IllegalArgumentException if token order, symbols, or orientation are invalid
     */
    public EvmTokenPair {
        Objects.requireNonNull(token0, "token0");
        Objects.requireNonNull(token1, "token1");
        Objects.requireNonNull(instrument, "instrument");
        if (token0.address().compareTo(token1.address()) >= 0) {
            throw new IllegalArgumentException(
                    "token0/token1 must follow ascending contract address order");
        }
        if (token0.symbol().equals(token1.symbol())) {
            throw new IllegalArgumentException("token symbols must be distinct");
        }
        if (instrument.base() == null || instrument.base().isBlank()
                || instrument.quote() == null || instrument.quote().isBlank()) {
            throw new IllegalArgumentException("instrument base and quote must not be blank");
        }
        if (instrument.isDerivative()) {
            throw new IllegalArgumentException("instrument must be spot");
        }
        boolean token0Base = instrument.base().equals(token0.symbol())
                && instrument.quote().equals(token1.symbol());
        boolean token1Base = instrument.base().equals(token1.symbol())
                && instrument.quote().equals(token0.symbol());
        if (!token0Base && !token1Base) {
            throw new IllegalArgumentException(
                    "instrument base/quote must match token0/token1 symbols");
        }
    }

    /**
     * Returns the token selected as the logical base asset.
     *
     * @return base token
     */
    public EvmToken baseToken() {
        return token0IsBase() ? token0 : token1;
    }

    /**
     * Returns the token selected as the logical quote asset.
     *
     * @return quote token
     */
    public EvmToken quoteToken() {
        return token0IsBase() ? token1 : token0;
    }

    /**
     * Reports whether {@code token0} is the logical base asset.
     *
     * @return {@code true} when {@code token0} is the instrument base
     */
    public boolean token0IsBase() {
        return instrument.base().equals(token0.symbol());
    }
}
