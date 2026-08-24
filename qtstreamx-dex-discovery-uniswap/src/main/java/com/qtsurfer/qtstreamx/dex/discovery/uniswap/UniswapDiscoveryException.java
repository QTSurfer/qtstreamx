package com.qtsurfer.qtstreamx.dex.discovery.uniswap;

/** Reports a scan-level discovery failure without retaining provider-controlled details. */
public final class UniswapDiscoveryException extends RuntimeException {

    /** Stable scan-level failure classification. */
    public enum Kind {
        /** Provider-backed scan or call failed. */
        RPC,
        /** Requested refresh range exceeded the configured block bound. */
        SCAN_RANGE_LIMIT,
        /** ERC-20 metadata calls exceeded the configured budget. */
        METADATA_CALL_LIMIT,
        /** Factory candidates exceeded the configured retained-market bound. */
        DISCOVERED_MARKET_LIMIT,
        /** Selected descriptors exceeded the configured output bound. */
        OUTPUT_MARKET_LIMIT
    }

    /** Safe failure classification. */
    private final Kind kind;

    /** Creates the safe scan failure. */
    UniswapDiscoveryException() {
        this(Kind.RPC);
    }

    /** Creates a safe scan failure with a stable classification. */
    UniswapDiscoveryException(Kind kind) {
        super("Uniswap factory discovery scan failed: " + kind);
        this.kind = kind;
    }

    /**
     * Returns the safe failure classification.
     *
     * @return stable failure kind
     */
    public Kind kind() {
        return kind;
    }
}
