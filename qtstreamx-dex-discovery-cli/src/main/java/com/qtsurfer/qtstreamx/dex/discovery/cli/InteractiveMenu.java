package com.qtsurfer.qtstreamx.dex.discovery.cli;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Builds ordinary CLI commands from a numbered keyboard-only menu. */
final class InteractiveMenu {
    private final CliTerminal terminal;

    InteractiveMenu(CliTerminal terminal) {
        this.terminal = terminal;
    }

    CliRequest request() throws IOException {
        terminal.write("QTStreamX DEX Discovery\n\n"
                + "Protocol\n"
                + "  1. Uniswap (V2/V3)\n"
                + "> ");
        if (!"1".equals(requiredLine())) {
            throw new IllegalArgumentException("protocol choice must be 1");
        }
        terminal.write("\nNetwork\n"
                + "  1. Ethereum mainnet\n"
                + "  2. Robinhood Chain mainnet\n"
                + "> ");
        String network = switch (requiredLine()) {
            case "1" -> "ethereum";
            case "2" -> "robinhood";
            default -> throw new IllegalArgumentException("network choice must be 1 or 2");
        };
        terminal.write("\nAction\n"
                + "  1. List reviewed markets\n"
                + "  2. Search discovered tokens by name or symbol\n"
                + "  3. Find pools for a token contract\n"
                + "  4. Inspect a pool contract\n"
                + "  5. Scan a bounded factory range\n"
                + "> ");
        return switch (requiredLine()) {
            case "1" -> parse("uniswap", "markets", "--network", network);
            case "2" -> rangeCommand("search", network, true);
            case "3" -> addressCommand("token", network);
            case "4" -> addressCommand("pool", network);
            case "5" -> rangeCommand("scan", network, false);
            default -> throw new IllegalArgumentException("action choice must be between 1 and 5");
        };
    }

    private CliRequest addressCommand(String command, String network) throws IOException {
        terminal.write("Contract address: ");
        return parse("uniswap", command, "--network", network, requiredLine());
    }

    private CliRequest rangeCommand(
            String command,
            String network,
            boolean search) throws IOException {
        terminal.write("Version (v2/v3): ");
        String version = requiredLine();
        terminal.write("From block: ");
        String from = requiredLine();
        terminal.write("To block: ");
        String to = requiredLine();
        List<String> arguments = new ArrayList<>(List.of(
                "uniswap",
                command,
                "--network", network,
                "--version", version,
                "--from", from,
                "--to", to));
        if (search) {
            terminal.write("Name or symbol query: ");
            arguments.addAll(List.of("--query", requiredLine()));
        }
        terminal.write("Factory address (blank for reviewed deployment): ");
        String factory = requiredLine();
        if (!factory.isBlank()) {
            arguments.addAll(List.of("--factory", factory));
        }
        return CliParser.parse(arguments);
    }

    private String requiredLine() throws IOException {
        String value = terminal.readLine();
        if (value == null) {
            throw new IllegalArgumentException("input ended before command was complete");
        }
        return value.strip();
    }

    private static CliRequest parse(String... arguments) {
        return CliParser.parse(List.of(arguments));
    }
}
