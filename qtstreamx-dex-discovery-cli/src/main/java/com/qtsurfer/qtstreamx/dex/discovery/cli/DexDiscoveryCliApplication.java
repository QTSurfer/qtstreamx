package com.qtsurfer.qtstreamx.dex.discovery.cli;

import com.qtsurfer.qtstreamx.dex.capture.csv.QtsurferTickerCsvFormatter;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Coordinates parsing, menus, typed discovery, rendering, and stable exit codes. */
final class DexDiscoveryCliApplication {
    static final int SUCCESS = 0;
    static final int INVALID_INPUT = 2;
    static final int PROVIDER_UNAVAILABLE = 3;
    static final int LOOKUP_FAILURE = 4;
    static final int NO_SUPPORTED_MARKET = 5;

    private static final List<String> HELP = List.of(
            "qtstreamx-dex-discovery protocols [--output json]",
            "qtstreamx-dex-discovery networks [--output json]",
            "qtstreamx-dex-discovery uniswap markets --network <name>",
            "qtstreamx-dex-discovery uniswap token --network <name> <token>",
            "qtstreamx-dex-discovery uniswap pool --network <name> <pool>",
            "qtstreamx-dex-discovery uniswap capture --network <name> --version <v2|v3> --start-block <n> --out <events.csv> <reviewed-contract>",
            "qtstreamx-dex-discovery uniswap format --source <events.csv> --out <ticks.csv>",
            "qtstreamx-dex-discovery uniswap scan --network <name> --version <v2|v3> --from <n> --to <n>",
            "qtstreamx-dex-discovery uniswap search --network <name> --version <v2|v3> --query <text> --from <n> --to <n>");

    private final CliTerminal terminal;
    private final ProtocolAdapterFactory adapterFactory;
    private final CliRenderer renderer = new CliRenderer();

    DexDiscoveryCliApplication(CliTerminal terminal) {
        this(terminal, new DefaultProtocolAdapterFactory());
    }

    DexDiscoveryCliApplication(CliTerminal terminal, ProtocolAdapterFactory adapterFactory) {
        this.terminal = terminal;
        this.adapterFactory = adapterFactory;
    }

    int run(String[] arguments, Map<String, String> environment) {
        CliRequest.Output output = requestedOutput(arguments);
        String protocol = null;
        String command = "cli";
        try {
            EndpointArguments endpointArguments = EndpointArguments.extract(arguments, environment);
            CliRequest request = CliParser.parse(endpointArguments.commandArguments());
            if (request.command() == CliRequest.Command.INTERACTIVE) {
                if (request.output() != CliRequest.Output.HUMAN) {
                    throw new IllegalArgumentException("interactive mode requires human output");
                }
                request = new InteractiveMenu(terminal).request();
            }
            protocol = request.protocolName();
            command = request.commandName();
            output = request.output();
            String providerMessage = preflightHttpProvider(request, endpointArguments);
            if (providerMessage != null && output == CliRequest.Output.HUMAN) {
                terminal.write(providerMessage + "\n");
            }
            CliResponse response = execute(request, endpointArguments);
            if (providerMessage != null && output == CliRequest.Output.JSON) {
                response = response.withMessage(providerMessage);
            }
            terminal.write(renderer.render(response, output));
            return "no_supported_market".equals(response.status())
                    ? NO_SUPPORTED_MARKET
                    : SUCCESS;
        } catch (IllegalArgumentException exception) {
            writeError(protocol, command, "invalid_input", safeMessage(exception.getMessage()), output);
            return INVALID_INPUT;
        } catch (CliLookupException exception) {
            writeError(protocol, command, "lookup_failure", exception.reason(), output);
            return LOOKUP_FAILURE;
        } catch (EvmRpcException | IllegalStateException exception) {
            if (Thread.currentThread().isInterrupted()) {
                writeError(protocol, command, "interrupted", "Operation interrupted", output);
                return 130;
            }
            writeError(protocol, command, "provider_unavailable", "RPC provider unavailable", output);
            return PROVIDER_UNAVAILABLE;
        } catch (IOException exception) {
            writeError(protocol, command, "invalid_input", "Terminal input unavailable", output);
            return INVALID_INPUT;
        }
    }

    private CliResponse execute(CliRequest request, EndpointArguments endpoints) {
        return switch (request.command()) {
            case HELP -> {
                request.requireNoArguments();
                request.rejectUnknownOptions(Set.of());
                yield CliResponse.ok(request, HELP);
            }
            case PROTOCOLS -> {
                request.requireNoArguments();
                request.rejectUnknownOptions(Set.of());
                yield CliResponse.ok(request, ProtocolCatalog.protocols());
            }
            case NETWORKS -> {
                request.requireNoArguments();
                request.rejectUnknownOptions(Set.of());
                yield CliResponse.ok(request, NetworkCatalog.networks());
            }
            case MARKETS -> {
                request.requireNoArguments();
                request.rejectUnknownOptions(Set.of("network"));
                String network = uniswapNetwork(request);
                yield CliResponse.ok(request, UniswapCliCatalog.markets(network));
            }
            case TOKEN, POOL, SCAN, SEARCH -> {
                validateOnChainOptions(request);
                String network = uniswapNetwork(request);
                String endpoint = endpoints.requireHttpUrl();
                yield adapterFactory.create(request.protocol(), network, endpoint).execute(request);
            }
            case CAPTURE -> {
                validateCaptureOptions(request);
                yield new UniswapCaptureCommand().capture(request, endpoints);
            }
            case FORMAT -> {
                request.requireNoArguments();
                request.rejectUnknownOptions(Set.of("source", "out"));
                try {
                    new QtsurferTickerCsvFormatter().format(
                            Path.of(request.requiredOption("source")), Path.of(request.requiredOption("out")));
                } catch (IOException exception) {
                    throw new IllegalArgumentException(exception.getMessage(), exception);
                }
                yield CliResponse.ok(request, List.of(request.requiredOption("out")));
            }
            case INTERACTIVE -> throw new IllegalStateException("interactive request was not resolved");
        };
    }

    private static String preflightHttpProvider(CliRequest request, EndpointArguments endpoints) {
        return switch (request.command()) {
            case TOKEN, POOL, SCAN, SEARCH -> {
                validateOnChainOptions(request);
                uniswapNetwork(request);
                endpoints.requireHttpUrl();
                yield endpoints.httpProviderMessage();
            }
            case CAPTURE -> {
                validateCaptureOptions(request);
                yield UniswapCaptureCommand.preflightProviders(request, endpoints);
            }
            default -> null;
        };
    }

    private static void validateCaptureOptions(CliRequest request) {
        request.rejectUnknownOptions(Set.of(
                "network", "version", "start-block", "start-date", "out", "checkpoint-dir", "duration-seconds",
                "confirmations", "max-block-range", "timeout-seconds", "retries", "overlap-blocks",
                "max-replay-blocks", "max-provider-lag-blocks"));
        request.requiredAddressArgument();
        request.requiredOption("version");
        boolean hasStartBlock = request.option("start-block").isPresent();
        boolean hasStartDate = request.option("start-date").isPresent();
        if (hasStartBlock == hasStartDate) {
            throw new IllegalArgumentException("exactly one of --start-block or --start-date is required");
        }
        request.requiredOption("out");
    }

    private static void validateOnChainOptions(CliRequest request) {
        Set<String> common = Set.of("network", "version", "factory", "factory-start", "block");
        switch (request.command()) {
            case TOKEN, POOL -> request.rejectUnknownOptions(common);
            case SCAN -> request.rejectUnknownOptions(
                    Set.of("network", "version", "factory", "factory-start", "from", "to", "limit"));
            case SEARCH -> request.rejectUnknownOptions(Set.of(
                    "network", "version", "factory", "factory-start", "from", "to", "limit",
                    "token-limit", "query"));
            default -> throw new IllegalArgumentException("command is not an on-chain operation");
        }
        if (request.command() == CliRequest.Command.SCAN
                || request.command() == CliRequest.Command.SEARCH) {
            request.requireNoArguments();
        }
    }

    private static String uniswapNetwork(CliRequest request) {
        return UniswapCliCatalog.resolveNetwork(request.requiredOption("network"));
    }

    private void writeError(
            String protocol,
            String command,
            String status,
            String message,
            CliRequest.Output output) {
        String rendered = renderer.render(
                CliResponse.error(protocol, command, status, message), output);
        terminal.writeError(rendered);
    }

    private static CliRequest.Output requestedOutput(String[] arguments) {
        for (int index = 0; index + 1 < arguments.length; index++) {
            if ("--output".equals(arguments[index])
                    && "json".equalsIgnoreCase(arguments[index + 1])) {
                return CliRequest.Output.JSON;
            }
        }
        return CliRequest.Output.HUMAN;
    }

    private static String safeMessage(String message) {
        if (message == null) {
            return "Invalid input";
        }
        String sanitized = message.codePoints()
                .filter(codePoint -> !Character.isISOControl(codePoint))
                .limit(200)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
        return sanitized.isBlank() ? "Invalid input" : sanitized;
    }

}
