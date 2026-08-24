package com.qtsurfer.qtstreamx.dex.discovery.cli;

import java.util.List;

/** Aggregates networks exposed by the installed protocol adapters. */
final class NetworkCatalog {

    private NetworkCatalog() {}

    static List<CliData.Network> networks() {
        return UniswapCliCatalog.networks();
    }
}
