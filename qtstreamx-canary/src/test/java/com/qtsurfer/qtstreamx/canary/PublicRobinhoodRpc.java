package com.qtsurfer.qtstreamx.canary;

/** Credential-free archive endpoints used only by the opt-in Robinhood live test. */
final class PublicRobinhoodRpc {
    static final String HTTP_URL = "https://docs-demo.robinhood-mainnet.quiknode.pro/";
    static final String WS_URL = "wss://docs-demo.robinhood-mainnet.quiknode.pro/";

    private PublicRobinhoodRpc() {}

    static String activeHttpUrl() {
        return environmentOrDefault(
                "QTSTREAMX_ROBINHOOD_ACTIVE_HTTP_URL",
                environmentOrDefault("QTSTREAMX_ROBINHOOD_HTTP_URL", HTTP_URL));
    }

    static String activeWebSocketUrl() {
        return environmentOrDefault(
                "QTSTREAMX_ROBINHOOD_ACTIVE_WS_URL",
                environmentOrDefault("QTSTREAMX_ROBINHOOD_WS_URL", WS_URL));
    }

    static String passiveHttpUrl() {
        return requireEnvironment("QTSTREAMX_ROBINHOOD_PASSIVE_HTTP_URL");
    }

    static String passiveWebSocketUrl() {
        return requireEnvironment("QTSTREAMX_ROBINHOOD_PASSIVE_WS_URL");
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
