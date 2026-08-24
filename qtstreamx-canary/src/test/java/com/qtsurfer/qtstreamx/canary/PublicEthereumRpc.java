package com.qtsurfer.qtstreamx.canary;

/** Credential-free Ethereum endpoints used only by opt-in live integration tests. */
final class PublicEthereumRpc {
    static final String HTTP_URL = "https://eth.drpc.org";
    static final String WS_URL = "wss://eth.drpc.org";

    private PublicEthereumRpc() {}

    static String activeHttpUrl() {
        return environmentOrDefault(
                "QTSTREAMX_EVM_ACTIVE_HTTP_URL",
                environmentOrDefault("QTSTREAMX_EVM_HTTP_URL", HTTP_URL));
    }

    static String activeWebSocketUrl() {
        return environmentOrDefault(
                "QTSTREAMX_EVM_ACTIVE_WS_URL",
                environmentOrDefault("QTSTREAMX_EVM_WS_URL", WS_URL));
    }

    static String passiveHttpUrl() {
        return requireEnvironment("QTSTREAMX_EVM_PASSIVE_HTTP_URL");
    }

    static String passiveWebSocketUrl() {
        return requireEnvironment("QTSTREAMX_EVM_PASSIVE_WS_URL");
    }

    private static String environmentOrDefault(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static String requireEnvironment(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required for active/passive live tests");
        }
        return value;
    }
}
