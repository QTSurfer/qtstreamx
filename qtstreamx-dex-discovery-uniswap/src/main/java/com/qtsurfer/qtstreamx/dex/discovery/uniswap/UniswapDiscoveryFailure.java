package com.qtsurfer.qtstreamx.dex.discovery.uniswap;

import java.util.Objects;

/**
 * Safe diagnostic for one factory event excluded from discovery.
 *
 * @param kind stable failure classification
 * @param blockNumber event block number
 * @param transactionHash event transaction hash
 */
public record UniswapDiscoveryFailure(
        Kind kind,
        long blockNumber,
        String transactionHash
) {
    /** Classifies failures without exposing provider-controlled messages. */
    public enum Kind {
        /** Event topics or data do not match the canonical factory ABI. */
        MALFORMED_EVENT,
        /** ERC-20 symbol or decimal metadata could not be resolved safely. */
        TOKEN_METADATA,
        /** The supplied orientation rejected or failed to orient the pair. */
        ORIENTATION,
        /** No canonical Swap was found in the configured recent activity window. */
        INACTIVE_MARKET,
        /** Existing descriptor invariants rejected the decoded market. */
        INVALID_DESCRIPTOR
    }

    /** Validates the safe diagnostic fields. */
    public UniswapDiscoveryFailure {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(transactionHash, "transactionHash");
        if (blockNumber < 0) {
            throw new IllegalArgumentException("blockNumber must be non-negative");
        }
    }
}
