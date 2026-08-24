package com.qtsurfer.qtstreamx.dex.core;

import java.util.Locale;

/**
 * Immutable metadata required to normalize an EVM token amount.
 *
 * @param symbol symbol used by normalized instruments
 * @param address EVM token contract address
 * @param decimals on-chain decimal precision
 */
public record EvmToken(String symbol, String address, int decimals) {

    /**
     * Validates token metadata and normalizes the contract address.
     *
     * @throws IllegalArgumentException if the symbol, address, or decimals are invalid
     */
    public EvmToken {
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("symbol must not be blank");
        }
        if (address == null || !address.matches("0x[0-9a-fA-F]{40}")) {
            throw new IllegalArgumentException("address must be a 20-byte hex value");
        }
        if (decimals < 0 || decimals > 255) {
            throw new IllegalArgumentException("decimals must be between 0 and 255");
        }
        address = address.toLowerCase(Locale.ROOT);
    }
}
