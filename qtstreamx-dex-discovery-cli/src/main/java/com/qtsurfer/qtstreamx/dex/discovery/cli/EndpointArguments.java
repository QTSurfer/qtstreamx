package com.qtsurfer.qtstreamx.dex.discovery.cli;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Separates secret-shaped endpoint configuration from the command model. */
record EndpointArguments(List<String> commandArguments, Optional<String> httpUrl, Optional<String> wsUrl, Optional<String> passiveHttpUrl, Optional<String> passiveWsUrl) {

    EndpointArguments {
        commandArguments = List.copyOf(Objects.requireNonNull(commandArguments, "commandArguments"));
        httpUrl = Objects.requireNonNull(httpUrl, "httpUrl");
        wsUrl = Objects.requireNonNull(wsUrl, "wsUrl");
        passiveHttpUrl = Objects.requireNonNull(passiveHttpUrl, "passiveHttpUrl");
        passiveWsUrl = Objects.requireNonNull(passiveWsUrl, "passiveWsUrl");
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
        String endpoint = explicit == null ? environment.get("QTSTREAMX_EVM_HTTP_URL") : explicit;
        String ws = explicitWs == null ? environment.get("QTSTREAMX_EVM_WS_URL") : explicitWs;
        return new EndpointArguments(
                remaining,
                Optional.ofNullable(endpoint).filter(value -> !value.isBlank()),
                Optional.ofNullable(ws).filter(value -> !value.isBlank()),
                Optional.ofNullable(explicitPassive == null ? environment.get("QTSTREAMX_EVM_PASSIVE_HTTP_URL") : explicitPassive).filter(value -> !value.isBlank()),
                Optional.ofNullable(explicitPassiveWs == null ? environment.get("QTSTREAMX_EVM_PASSIVE_WS_URL") : explicitPassiveWs).filter(value -> !value.isBlank()));
    }

    /** Returns a diagnostic form that cannot expose the endpoint. */
    @Override
    public String toString() {
        return "EndpointArguments[commandArguments=" + commandArguments + ", httpUrl=<redacted>]";
    }
}
