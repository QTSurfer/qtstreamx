package com.qtsurfer.qtstreamx.dex.discovery.cli;

/** Creates an installed protocol adapter from runtime-only endpoint material. */
@FunctionalInterface
interface ProtocolAdapterFactory {

    CliProtocolAdapter create(
            CliRequest.Protocol protocol,
            String network,
            String httpUrl);
}
