package com.qtsurfer.qtstreamx.dex.discovery.cli;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Immutable command request containing no RPC endpoint or credential data. */
record CliRequest(
        Protocol protocol,
        Command command,
        Output output,
        List<String> arguments,
        Map<String, List<String>> options) {

    /** Supported CLI commands. */
    enum Command {
        INTERACTIVE,
        PROTOCOLS,
        NETWORKS,
        MARKETS,
        TOKEN,
        POOL,
        CAPTURE,
        FORMAT,
        SCAN,
        SEARCH,
        HELP
    }

    /** Protocol owning a command, or the protocol-neutral CLI shell. */
    enum Protocol {
        CORE,
        UNISWAP
    }

    /** Supported output formats. */
    enum Output {
        HUMAN,
        JSON
    }

    CliRequest {
        Objects.requireNonNull(protocol, "protocol");
        Objects.requireNonNull(command, "command");
        Objects.requireNonNull(output, "output");
        arguments = List.copyOf(Objects.requireNonNull(arguments, "arguments"));
        options = Map.copyOf(Objects.requireNonNull(options, "options"));
    }

    String requiredOption(String name) {
        return option(name).orElseThrow(() -> new IllegalArgumentException(
                "--" + name + " is required"));
    }

    Optional<String> option(String name) {
        List<String> values = options.getOrDefault(name, List.of());
        if (values.size() > 1) {
            throw new IllegalArgumentException("--" + name + " may be specified only once");
        }
        return values.stream().findFirst();
    }

    int intOption(String name, int fallback) {
        return option(name).map(value -> parseInt(name, value)).orElse(fallback);
    }

    long requiredLongOption(String name) {
        String value = requiredOption(name);
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("--" + name + " must be an integer");
        }
    }

    String requiredAddressArgument() {
        if (arguments.size() != 1) {
            throw new IllegalArgumentException(commandName() + " requires one contract address");
        }
        return arguments.getFirst();
    }

    String commandName() {
        return command.name().toLowerCase(Locale.ROOT);
    }

    String protocolName() {
        return protocol == Protocol.CORE ? null : protocol.name().toLowerCase(Locale.ROOT);
    }

    void requireNoArguments() {
        if (!arguments.isEmpty()) {
            throw new IllegalArgumentException(commandName() + " accepts no positional arguments");
        }
    }

    void rejectUnknownOptions(Set<String> allowed) {
        options.keySet().stream()
                .filter(name -> !allowed.contains(name))
                .findFirst()
                .ifPresent(name -> {
                    throw new IllegalArgumentException("unsupported option: --" + name);
                });
    }

    private static int parseInt(String name, String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("--" + name + " must be an integer");
        }
    }
}
