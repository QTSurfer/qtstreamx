package com.qtsurfer.qtstreamx.dex.discovery.uniswap;

import java.math.BigInteger;
import java.util.Locale;
import java.util.Objects;

/**
 * Validated on-chain identity and current native reserves for one Uniswap V2 pair.
 *
 * <p>Native reserves are token-unit integers, not fiat TVL.
 *
 * @param network stable EVM network identity
 * @param address pair contract address
 * @param token0 lower-addressed pair token
 * @param token1 higher-addressed pair token
 * @param reserve0 current raw reserve of token0
 * @param reserve1 current raw reserve of token1
 */
public record UniswapV2PairInspection(
        String network,
        String address,
        Erc20TokenInspection token0,
        Erc20TokenInspection token1,
        BigInteger reserve0,
        BigInteger reserve1
) {

    /** Validates immutable pair inspection data. */
    public UniswapV2PairInspection {
        if (network == null || network.isBlank()) {
            throw new IllegalArgumentException("network must not be blank");
        }
        address = normalizeAddress(address);
        Objects.requireNonNull(token0, "token0");
        Objects.requireNonNull(token1, "token1");
        Objects.requireNonNull(reserve0, "reserve0");
        Objects.requireNonNull(reserve1, "reserve1");
        if (!network.equals(token0.network()) || !network.equals(token1.network())) {
            throw new IllegalArgumentException("token networks must match the pair network");
        }
        if (token0.address().compareTo(token1.address()) >= 0) {
            throw new IllegalArgumentException("token0 address must sort before token1 address");
        }
        if (reserve0.signum() < 0 || reserve1.signum() < 0) {
            throw new IllegalArgumentException("reserves must be non-negative");
        }
    }

    private static String normalizeAddress(String value) {
        if (value == null || !value.matches("0x[0-9a-fA-F]{40}")) {
            throw new IllegalArgumentException("address must be a 20-byte hex value");
        }
        return value.toLowerCase(Locale.ROOT);
    }
}
