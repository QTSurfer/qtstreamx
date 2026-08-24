package com.qtsurfer.qtstreamx.dex.discovery.cli;

import java.io.IOException;

/** Minimal terminal port used by interactive mode and deterministic tests. */
interface CliTerminal {

    String readLine() throws IOException;

    void write(String text);

    void writeError(String text);
}
