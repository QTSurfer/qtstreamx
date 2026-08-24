package com.qtsurfer.qtstreamx.dex.discovery.cli;

import java.util.List;

/** Protocol adapters currently installed in the DEX discovery CLI. */
final class ProtocolCatalog {
    private static final List<CliData.Protocol> PROTOCOLS = List.of(
            new CliData.Protocol("uniswap", "Uniswap", List.of("v2", "v3")));

    private ProtocolCatalog() {}

    static List<CliData.Protocol> protocols() {
        return PROTOCOLS;
    }
}
