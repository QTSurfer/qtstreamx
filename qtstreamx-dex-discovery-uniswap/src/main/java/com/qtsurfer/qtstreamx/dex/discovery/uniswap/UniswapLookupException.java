package com.qtsurfer.qtstreamx.dex.discovery.uniswap;

import java.util.Objects;

/** Safe domain failure produced while validating a targeted on-chain lookup. */
public final class UniswapLookupException extends RuntimeException {

    /** Stable lookup failure categories suitable for CLI exit-code mapping. */
    public enum Kind {
        /** The configured lookup fan-out exceeds its fixed safety bound. */
        LIMIT,
        /** The requested deployment does not match the lookup operation. */
        VERSION,
        /** A returned contract does not expose the expected versioned pool ABI. */
        MALFORMED_CONTRACT,
        /** A factory log does not match the canonical versioned creation event. */
        MALFORMED_EVENT,
        /** The pool identity does not resolve back to the configured reviewed factory. */
        FACTORY_MISMATCH
    }

    /** Stable failure category retained without provider-controlled detail. */
    private final Kind kind;

    /**
     * Creates a failure without provider-controlled content.
     *
     * @param kind stable failure category
     */
    public UniswapLookupException(Kind kind) {
        super(message(Objects.requireNonNull(kind, "kind")));
        this.kind = kind;
    }

    /**
     * Returns the stable failure category.
     *
     * @return lookup failure kind
     */
    public Kind kind() {
        return kind;
    }

    private static String message(Kind kind) {
        return switch (kind) {
            case LIMIT -> "Uniswap lookup limit exceeded";
            case VERSION -> "Uniswap deployment version does not match lookup";
            case MALFORMED_CONTRACT -> "Contract does not match the expected Uniswap ABI";
            case MALFORMED_EVENT -> "Factory log does not match the expected Uniswap event";
            case FACTORY_MISMATCH -> "Market does not belong to the configured Uniswap factory";
        };
    }
}
