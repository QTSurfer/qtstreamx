package com.qtsurfer.qtstreamx.dex.discovery.uniswap;

import java.util.Locale;

/**
 * Identifies one Uniswap factory scan and its initial cursor.
 *
 * @param network stable EVM network identity
 * @param factoryAddress factory contract address
 * @param startBlock first block eligible for discovery
 */
public record UniswapFactoryScan(String network, String factoryAddress, long startBlock) {

    /** Validates and normalizes the scan identity. */
    public UniswapFactoryScan {
        if (network == null || network.isBlank()) {
            throw new IllegalArgumentException("network must not be blank");
        }
        if (factoryAddress == null || !factoryAddress.matches("0x[0-9a-fA-F]{40}")) {
            throw new IllegalArgumentException("factoryAddress must be a 20-byte hex value");
        }
        if (startBlock < 0) {
            throw new IllegalArgumentException("startBlock must be non-negative");
        }
        factoryAddress = factoryAddress.toLowerCase(Locale.ROOT);
    }

    /** Returns a diagnostic description with no RPC endpoint information. */
    @Override
    public String toString() {
        return "UniswapFactoryScan[network=" + network
                + ", factoryAddress=" + factoryAddress
                + ", startBlock=" + startBlock
                + "]";
    }
}
