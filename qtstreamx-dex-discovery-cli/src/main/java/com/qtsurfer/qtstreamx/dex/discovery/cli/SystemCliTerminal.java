package com.qtsurfer.qtstreamx.dex.discovery.cli;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/** UTF-8 terminal backed by standard input, output, and error. */
final class SystemCliTerminal implements CliTerminal {
    private final BufferedReader input = new BufferedReader(
            new InputStreamReader(System.in, StandardCharsets.UTF_8));

    @Override
    public String readLine() throws IOException {
        return input.readLine();
    }

    @Override
    public void write(String text) {
        System.out.print(text);
    }

    @Override
    public void writeError(String text) {
        System.err.print(text);
    }
}
