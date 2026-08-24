package com.qtsurfer.qtstreamx.dex.discovery.cli;

/** Executes commands owned by one installed DEX discovery protocol. */
interface CliProtocolAdapter {

    CliResponse execute(CliRequest request);
}
