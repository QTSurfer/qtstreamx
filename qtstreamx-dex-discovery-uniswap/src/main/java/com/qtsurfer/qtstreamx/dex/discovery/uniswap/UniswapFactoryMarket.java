package com.qtsurfer.qtstreamx.dex.discovery.uniswap;

import java.util.Locale;
import java.util.Objects;

/**
 * Trust-neutral market identity decoded from one canonical factory creation event.
 *
 * @param version factory event family
 * @param network stable EVM network identity
 * @param factoryAddress emitting factory address
 * @param marketAddress created pair or pool address
 * @param token0Address lower-addressed contract token
 * @param token1Address higher-addressed contract token
 * @param feeTier V3 fee tier, or zero for V2
 * @param blockNumber creation-event block number
 * @param transactionHash creation transaction hash
 * @param logIndex creation-event log index within the block
 */
public record UniswapFactoryMarket(
        UniswapDeployment.Version version,
        String network,
        String factoryAddress,
        String marketAddress,
        String token0Address,
        String token1Address,
        int feeTier,
        long blockNumber,
        String transactionHash,
        int logIndex
) {

    /** Validates and normalizes immutable factory provenance. */
    public UniswapFactoryMarket {
        Objects.requireNonNull(version, "version");
        if (network == null || network.isBlank()) {
            throw new IllegalArgumentException("network must not be blank");
        }
        factoryAddress = normalizeAddress(factoryAddress, "factoryAddress");
        marketAddress = normalizeAddress(marketAddress, "marketAddress");
        token0Address = normalizeAddress(token0Address, "token0Address");
        token1Address = normalizeAddress(token1Address, "token1Address");
        if (token0Address.compareTo(token1Address) >= 0) {
            throw new IllegalArgumentException("token0 address must sort before token1 address");
        }
        if (version == UniswapDeployment.Version.V2 && feeTier != 0
                || version == UniswapDeployment.Version.V3
                        && (feeTier <= 0 || feeTier >= 1_000_000)) {
            throw new IllegalArgumentException("feeTier does not match the protocol version");
        }
        if (blockNumber < 0 || logIndex < 0) {
            throw new IllegalArgumentException("event position must be non-negative");
        }
        if (transactionHash == null || transactionHash.isBlank()) {
            throw new IllegalArgumentException("transactionHash must not be blank");
        }
    }

    private static String normalizeAddress(String value, String field) {
        if (value == null || !value.matches("0x[0-9a-fA-F]{40}")) {
            throw new IllegalArgumentException(field + " must be a 20-byte hex value");
        }
        return value.toLowerCase(Locale.ROOT);
    }
}
