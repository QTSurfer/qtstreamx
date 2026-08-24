package com.qtsurfer.qtstreamx.dex.uniswap.v3;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.dex.core.EvmToken;
import com.qtsurfer.qtstreamx.dex.core.EvmTokenPair;
import java.util.Locale;
import java.util.Objects;

/**
 * Explicit descriptor for one Uniswap v3 pool.
 *
 * <p>{@code token0} and {@code token1} must follow Uniswap's ascending contract-address
 * order. The logical base and quote are taken from {@code instrument}; they may map to
 * either pool-token position.
 *
 * @param network stable EVM network identity
 * @param address pool contract address
 * @param tokens ordered tokens and logical base/quote orientation
 * @param feeTier pool fee in hundredths of one basis point
 */
public record UniswapV3Pool(
        String network,
        String address,
        EvmTokenPair tokens,
        int feeTier
) {

    /**
     * Creates a pool from explicit ordered tokens and logical orientation.
     *
     * @param network stable EVM network identity
     * @param address pool contract address
     * @param token0 lower-addressed pool token
     * @param token1 higher-addressed pool token
     * @param instrument normalized base/quote instrument
     * @param feeTier pool fee in hundredths of one basis point
     */
    public UniswapV3Pool(
            String network,
            String address,
            EvmToken token0,
            EvmToken token1,
            Instrument instrument,
            int feeTier) {
        this(network, address, new EvmTokenPair(token0, token1, instrument), feeTier);
    }

    /**
     * Validates pool identity, token ordering, and instrument mapping.
     *
     * @throws NullPointerException if the token pair is null
     * @throws IllegalArgumentException if pool identity or fee is invalid
     */
    public UniswapV3Pool {
        if (network == null || network.isBlank()) {
            throw new IllegalArgumentException("network must not be blank");
        }
        if (address == null || !address.matches("0x[0-9a-fA-F]{40}")) {
            throw new IllegalArgumentException("address must be a 20-byte hex value");
        }
        address = address.toLowerCase(Locale.ROOT);
        Objects.requireNonNull(tokens, "tokens");
        if (feeTier <= 0 || feeTier >= 1_000_000) {
            throw new IllegalArgumentException("feeTier must be between 1 and 999999");
        }
    }

    /**
     * Returns the lower-addressed pool token.
     *
     * @return token0
     */
    public EvmToken token0() {
        return tokens.token0();
    }

    /**
     * Returns the higher-addressed pool token.
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
