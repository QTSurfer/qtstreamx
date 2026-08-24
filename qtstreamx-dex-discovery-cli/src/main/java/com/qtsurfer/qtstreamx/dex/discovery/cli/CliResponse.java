package com.qtsurfer.qtstreamx.dex.discovery.cli;

import java.util.List;
import java.util.Objects;

/** Versioned application response rendered identically by every entry point. */
record CliResponse(
        int schemaVersion,
        String protocol,
        String command,
        String status,
        Object data,
        List<String> messages) {

    static final int SCHEMA_VERSION = 1;

    CliResponse {
        if (schemaVersion != SCHEMA_VERSION) {
            throw new IllegalArgumentException("unsupported schema version");
        }
        Objects.requireNonNull(command, "command");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(data, "data");
        messages = List.copyOf(Objects.requireNonNull(messages, "messages"));
    }

    static CliResponse ok(CliRequest request, Object data) {
        return new CliResponse(
                SCHEMA_VERSION, request.protocolName(), request.commandName(), "ok", data, List.of());
    }

    static CliResponse noSupportedMarket(CliRequest request, Object data) {
        return new CliResponse(
                SCHEMA_VERSION,
                request.protocolName(),
                request.commandName(),
                "no_supported_market",
                data,
                List.of("No market matched the exact supported deployment and query"));
    }

    static CliResponse error(String protocol, String command, String status, String message) {
        return new CliResponse(
                SCHEMA_VERSION, protocol, command, status, List.of(), List.of(message));
    }
}
