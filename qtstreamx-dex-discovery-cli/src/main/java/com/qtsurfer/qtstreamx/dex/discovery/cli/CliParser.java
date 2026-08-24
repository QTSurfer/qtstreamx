package com.qtsurfer.qtstreamx.dex.discovery.cli;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Parses the deliberately small command grammar without a framework dependency. */
final class CliParser {

    private CliParser() {}

    static CliRequest parse(List<String> arguments) {
        if (arguments.isEmpty()) {
            return new CliRequest(
                    CliRequest.Protocol.CORE,
                    CliRequest.Command.INTERACTIVE,
                    CliRequest.Output.HUMAN,
                    List.of(),
                    Map.of());
        }
        boolean protocolCommand = "uniswap".equalsIgnoreCase(arguments.getFirst());
        if (protocolCommand && arguments.size() == 1) {
            throw new IllegalArgumentException("uniswap requires a command");
        }
        CliRequest.Protocol protocol = protocolCommand
                ? CliRequest.Protocol.UNISWAP
                : CliRequest.Protocol.CORE;
        int commandIndex = protocolCommand ? 1 : 0;
        CliRequest.Command command = parseCommand(arguments.get(commandIndex));
        validateScope(protocol, command);
        List<String> positionals = new ArrayList<>();
        Map<String, List<String>> options = new LinkedHashMap<>();
        for (int index = commandIndex + 1; index < arguments.size(); index++) {
            String argument = arguments.get(index);
            if (!argument.startsWith("--")) {
                positionals.add(argument);
                continue;
            }
            String name = argument.substring(2);
            if (name.isBlank() || index + 1 >= arguments.size()
                    || arguments.get(index + 1).startsWith("--")) {
                throw new IllegalArgumentException(argument + " requires a value");
            }
            options.computeIfAbsent(name, ignored -> new ArrayList<>())
                    .add(arguments.get(++index));
        }
        CliRequest.Output output = options.getOrDefault("output", List.of("human")).stream()
                .reduce((first, second) -> {
                    throw new IllegalArgumentException("--output may be specified only once");
                })
                .map(CliParser::parseOutput)
                .orElse(CliRequest.Output.HUMAN);
        options.remove("output");
        return new CliRequest(protocol, command, output, positionals, options);
    }

    private static void validateScope(
            CliRequest.Protocol protocol,
            CliRequest.Command command) {
        boolean coreCommand = command == CliRequest.Command.PROTOCOLS
                || command == CliRequest.Command.NETWORKS
                || command == CliRequest.Command.HELP;
        if (protocol == CliRequest.Protocol.CORE && !coreCommand) {
            throw new IllegalArgumentException("protocol command requires a protocol prefix");
        }
        if (protocol == CliRequest.Protocol.UNISWAP && coreCommand) {
            throw new IllegalArgumentException("command is not specific to uniswap");
        }
    }

    private static CliRequest.Command parseCommand(String value) {
        try {
            return CliRequest.Command.valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("unknown command: " + value);
        }
    }

    private static CliRequest.Output parseOutput(String value) {
        try {
            return CliRequest.Output.valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("--output must be human or json");
        }
    }
}
