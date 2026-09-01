package com.qtsurfer.qtstreamx.dex.discovery.cli;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Separates secret-shaped endpoint configuration from the command model. */
record EndpointArguments(
        List<String> commandArguments,
        Optional<String> httpUrl,
        Optional<String> wsUrl,
        Optional<String> passiveHttpUrl,
        Optional<String> passiveWsUrl,
        EndpointSource httpSource,
        EndpointSource wsSource,
        EndpointSource passiveHttpSource,
        EndpointSource passiveWsSource) {

    EndpointArguments {
        commandArguments = List.copyOf(Objects.requireNonNull(commandArguments, "commandArguments"));
        httpUrl = Objects.requireNonNull(httpUrl, "httpUrl");
        wsUrl = Objects.requireNonNull(wsUrl, "wsUrl");
        passiveHttpUrl = Objects.requireNonNull(passiveHttpUrl, "passiveHttpUrl");
        passiveWsUrl = Objects.requireNonNull(passiveWsUrl, "passiveWsUrl");
        httpSource = Objects.requireNonNull(httpSource, "httpSource");
        wsSource = Objects.requireNonNull(wsSource, "wsSource");
        passiveHttpSource = Objects.requireNonNull(passiveHttpSource, "passiveHttpSource");
        passiveWsSource = Objects.requireNonNull(passiveWsSource, "passiveWsSource");
    }

    static EndpointArguments extract(String[] arguments, Map<String, String> environment) {
        Objects.requireNonNull(arguments, "arguments");
        Objects.requireNonNull(environment, "environment");
        List<String> remaining = new ArrayList<>();
        String explicit = null;
        String explicitWs = null;
        String explicitPassive = null;
        String explicitPassiveWs = null;
        for (int index = 0; index < arguments.length; index++) {
            if (!"--http-url".equals(arguments[index]) && !"--ws-url".equals(arguments[index])
                    && !"--passive-http-url".equals(arguments[index])
                    && !"--passive-ws-url".equals(arguments[index])) {
                remaining.add(arguments[index]);
                continue;
            }
            String option = arguments[index];
            boolean websocket = "--ws-url".equals(option) || "--passive-ws-url".equals(option);
            boolean passive = option.startsWith("--passive-");
            if ((passive ? (websocket ? explicitPassiveWs : explicitPassive) : (websocket ? explicitWs : explicit)) != null) {
                throw new IllegalArgumentException(arguments[index] + " may be specified only once");
            }
            if (index + 1 >= arguments.length || arguments[index + 1].startsWith("--")) {
                throw new IllegalArgumentException(arguments[index] + " requires a value");
            }
            String value = arguments[++index];
            if (passive) {
                if (websocket) explicitPassiveWs = value; else explicitPassive = value;
            } else if (websocket) {
                explicitWs = value;
            } else {
                explicit = value;
            }
        }
        ResolvedEndpoint endpoint = resolve(explicit, environment.get("QTSTREAMX_EVM_HTTP_URL"));
        ResolvedEndpoint ws = resolve(explicitWs, environment.get("QTSTREAMX_EVM_WS_URL"));
        ResolvedEndpoint passiveHttp = resolve(
                explicitPassive, environment.get("QTSTREAMX_EVM_PASSIVE_HTTP_URL"));
        ResolvedEndpoint passiveWs = resolve(
                explicitPassiveWs, environment.get("QTSTREAMX_EVM_PASSIVE_WS_URL"));
        return new EndpointArguments(
                remaining,
                endpoint.value(), ws.value(), passiveHttp.value(), passiveWs.value(),
                endpoint.source(), ws.source(), passiveHttp.source(), passiveWs.source());
    }

    String requireHttpUrl() {
        return httpUrl.orElseThrow(() -> new IllegalArgumentException(
                "--http-url or QTSTREAMX_EVM_HTTP_URL is required"));
    }

    String httpProviderMessage() {
        return "RPC HTTP provider: " + httpSource.label("--http-url", "QTSTREAMX_EVM_HTTP_URL")
                + " (endpoint redacted)";
    }

    void requireCaptureProviders() {
        List<String> missing = new ArrayList<>();
        if (httpUrl.isEmpty()) missing.add("QTSTREAMX_EVM_HTTP_URL");
        if (wsUrl.isEmpty()) missing.add("QTSTREAMX_EVM_WS_URL");
        if (passiveHttpUrl.isEmpty()) {
            missing.add("QTSTREAMX_EVM_PASSIVE_HTTP_URL");
        }
        if (passiveWsUrl.isEmpty()) {
            missing.add("QTSTREAMX_EVM_PASSIVE_WS_URL");
        }
        if (!missing.isEmpty()) {
            throw new IllegalArgumentException(
                    "capture RPC providers missing before startup: "
                            + String.join(", ", missing));
        }
    }

    String captureProvidersMessage() {
        return "Capture RPC providers: active HTTP="
                + httpSource.label("--http-url", "QTSTREAMX_EVM_HTTP_URL")
                + ", active WebSocket=" + wsSource.label("--ws-url", "QTSTREAMX_EVM_WS_URL")
                + ", passive HTTP=" + passiveHttpSource.label(
                        "--passive-http-url", "QTSTREAMX_EVM_PASSIVE_HTTP_URL")
                + ", passive WebSocket=" + passiveWsSource.label(
                        "--passive-ws-url", "QTSTREAMX_EVM_PASSIVE_WS_URL")
                + " (endpoints redacted)";
    }

    private static ResolvedEndpoint resolve(String explicit, String environment) {
        if (explicit != null) {
            return new ResolvedEndpoint(optional(explicit), EndpointSource.COMMAND_LINE);
        }
        if (environment != null) {
            return new ResolvedEndpoint(optional(environment), EndpointSource.ENVIRONMENT);
        }
        return new ResolvedEndpoint(Optional.empty(), EndpointSource.ABSENT);
    }

    private static Optional<String> optional(String value) {
        return Optional.ofNullable(value).filter(candidate -> !candidate.isBlank());
    }

    private record ResolvedEndpoint(Optional<String> value, EndpointSource source) {}

    /** Returns a diagnostic form that cannot expose the endpoint. */
    @Override
    public String toString() {
        return "EndpointArguments[commandArguments=" + commandArguments + ", httpUrl=<redacted>]";
    }
}

/** Non-sensitive origin of one endpoint value. */
enum EndpointSource {
    COMMAND_LINE,
    ENVIRONMENT,
    ABSENT;

    String label(String option, String environment) {
        return switch (this) {
            case COMMAND_LINE -> option;
            case ENVIRONMENT -> environment;
            case ABSENT -> "not configured";
        };
    }
}
