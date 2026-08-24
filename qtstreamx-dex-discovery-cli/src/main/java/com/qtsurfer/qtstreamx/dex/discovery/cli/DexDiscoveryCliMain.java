package com.qtsurfer.qtstreamx.dex.discovery.cli;

/** Entry point for the protocol-extensible, read-only QTStreamX DEX discovery CLI. */
public final class DexDiscoveryCliMain {

    private DexDiscoveryCliMain() {}

    /**
     * Runs the CLI against standard streams and runtime environment variables.
     *
     * @param args command and option arguments
     */
    public static void main(String[] args) {
        int exitCode = new DexDiscoveryCliApplication(new SystemCliTerminal())
                .run(args, System.getenv());
        if (exitCode != DexDiscoveryCliApplication.SUCCESS) {
            System.exit(exitCode);
        }
    }
}
