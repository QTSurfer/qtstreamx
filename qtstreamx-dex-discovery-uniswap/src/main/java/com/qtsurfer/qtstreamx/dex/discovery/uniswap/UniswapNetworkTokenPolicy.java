package com.qtsurfer.qtstreamx.dex.discovery.uniswap;

import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Canonical token-address policy for one EVM network.
 *
 * @param quoteTokenAddresses addresses permitted as logical quote assets
 * @param baseTokenAddresses addresses permitted as logical base assets
 */
public record UniswapNetworkTokenPolicy(
        Set<String> quoteTokenAddresses,
        Set<String> baseTokenAddresses
) {

    /** Validates, normalizes, and immutably copies the configured addresses. */
    public UniswapNetworkTokenPolicy {
        quoteTokenAddresses = normalize(quoteTokenAddresses, "quoteTokenAddresses");
        baseTokenAddresses = normalize(baseTokenAddresses, "baseTokenAddresses");
        if (quoteTokenAddresses.isEmpty()) {
            throw new IllegalArgumentException("quoteTokenAddresses must not be empty");
        }
        if (baseTokenAddresses.isEmpty()) {
            throw new IllegalArgumentException("baseTokenAddresses must not be empty");
        }
    }

    private static Set<String> normalize(Set<String> addresses, String name) {
        if (addresses == null) {
            throw new NullPointerException(name);
        }
        return addresses.stream()
                .map(address -> normalize(address, name))
                .collect(Collectors.toUnmodifiableSet());
    }

    private static String normalize(String address, String name) {
        if (address == null || !address.matches("0x[0-9a-fA-F]{40}")) {
            throw new IllegalArgumentException(name + " must contain only 20-byte hex addresses");
        }
        return address.toLowerCase(Locale.ROOT);
    }
}
