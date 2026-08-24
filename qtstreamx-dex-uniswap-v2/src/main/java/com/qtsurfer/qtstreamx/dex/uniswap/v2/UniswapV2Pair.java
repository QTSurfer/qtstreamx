package com.qtsurfer.qtstreamx.dex.uniswap.v2;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.dex.core.EvmToken;
import com.qtsurfer.qtstreamx.dex.core.EvmTokenPair;
import java.util.Locale;
import java.util.Objects;

/**
 * Explicit descriptor for one Uniswap v2 pair.
 *
 * <p>{@code token0} and {@code token1} follow the pair contract's ascending
 * token-address order. The logical base and quote come from {@code instrument}
 * and may map to either token position.
 *
 * @param network stable EVM network identity
 * @param address pair contract address
 * @param tokens ordered tokens and logical base/quote orientation
 */
public record UniswapV2Pair(
        String network,
        String address,
        EvmTokenPair tokens
) {

    /**
     * Creates a pair from explicit ordered tokens and logical orientation.
     *
     * @param network stable EVM network identity
     * @param address pair contract address
     * @param token0 lower-addressed pair token
     * @param token1 higher-addressed pair token
     * @param instrument normalized base/quote instrument
     */
    public UniswapV2Pair(
            String network,
            String address,
            EvmToken token0,
            EvmToken token1,
            Instrument instrument) {
        this(network, address, new EvmTokenPair(token0, token1, instrument));
    }

    /**
     * Validates pair identity, token ordering, and instrument mapping.
     *
     * @throws NullPointerException if the token pair is null
     * @throws IllegalArgumentException if pair identity is invalid
     */
    public UniswapV2Pair {
        if (network == null || network.isBlank()) {
            throw new IllegalArgumentException("network must not be blank");
        }
        if (address == null || !address.matches("0x[0-9a-fA-F]{40}")) {
            throw new IllegalArgumentException("address must be a 20-byte hex value");
        }
        address = address.toLowerCase(Locale.ROOT);
        Objects.requireNonNull(tokens, "tokens");
    }

    /**
     * Returns the lower-addressed pair token.
     *
     * @return token0
     */
    public EvmToken token0() {
        return tokens.token0();
    }

    /**
     * Returns the higher-addressed pair token.
     *
     * @return token1
     */
    public EvmToken token1() {
        return tokens.token1();
    }

    /**
     * Returns the normalized logical instrument.
     *
     * @return base/quote instrument
     */
    public Instrument instrument() {
        return tokens.instrument();
    }
}
