package com.qtsurfer.qtstreamx.dex.discovery.uniswap;

import java.math.BigInteger;
import java.util.Locale;
import java.util.Objects;

/**
 * Validated on-chain identity and current native liquidity for one Uniswap V3 pool.
 *
 * <p>Current liquidity is the protocol's active-liquidity integer, not fiat TVL.
 *
 * @param network stable EVM network identity
 * @param address pool contract address
 * @param token0 lower-addressed pool token
 * @param token1 higher-addressed pool token
 * @param feeTier pool fee in hundredths of one basis point
 * @param currentLiquidity current protocol-native active liquidity
 */
public record UniswapV3PoolInspection(
        String network,
        String address,
        Erc20TokenInspection token0,
        Erc20TokenInspection token1,
        int feeTier,
        BigInteger currentLiquidity
) {

    /** Validates immutable pool inspection data. */
    public UniswapV3PoolInspection {
        if (network == null || network.isBlank()) {
            throw new IllegalArgumentException("network must not be blank");
        }
        address = normalizeAddress(address);
        Objects.requireNonNull(token0, "token0");
        Objects.requireNonNull(token1, "token1");
        Objects.requireNonNull(currentLiquidity, "currentLiquidity");
        if (!network.equals(token0.network()) || !network.equals(token1.network())) {
            throw new IllegalArgumentException("token networks must match the pool network");
        }
        if (token0.address().compareTo(token1.address()) >= 0) {
            throw new IllegalArgumentException("token0 address must sort before token1 address");
        }
        if (feeTier <= 0 || feeTier >= 1_000_000) {
            throw new IllegalArgumentException("feeTier must be between 1 and 999999");
        }
        if (currentLiquidity.signum() < 0) {
            throw new IllegalArgumentException("currentLiquidity must be non-negative");
        }
    }

    private static String normalizeAddress(String value) {
        if (value == null || !value.matches("0x[0-9a-fA-F]{40}")) {
            throw new IllegalArgumentException("address must be a 20-byte hex value");
        }
        return value.toLowerCase(Locale.ROOT);
    }
}
