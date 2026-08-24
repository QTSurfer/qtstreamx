package com.qtsurfer.qtstreamx.dex.discovery.cli;

import java.util.Objects;

/** Protocol-neutral bounded lookup failure exposed to the CLI shell. */
final class CliLookupException extends RuntimeException {
    private final String reason;

    CliLookupException(String reason) {
        super("DEX discovery lookup failed");
        this.reason = Objects.requireNonNull(reason, "reason");
    }

    String reason() {
        return reason;
    }
}
