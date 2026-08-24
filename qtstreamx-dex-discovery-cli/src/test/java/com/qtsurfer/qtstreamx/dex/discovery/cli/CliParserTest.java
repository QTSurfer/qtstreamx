package com.qtsurfer.qtstreamx.dex.discovery.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

class CliParserTest {

    @Test
    void parsesScriptableCommandWithoutEndpointMaterial() {
        CliRequest request = CliParser.parse(List.of(
                "uniswap",
                "search",
                "--network", "robinhood",
                "--version", "v2",
                "--query", "RUBY",
                "--from", "30800000",
                "--to", "30899999",
                "--output", "json"));

        assertThat(request.command()).isEqualTo(CliRequest.Command.SEARCH);
        assertThat(request.protocol()).isEqualTo(CliRequest.Protocol.UNISWAP);
        assertThat(request.output()).isEqualTo(CliRequest.Output.JSON);
        assertThat(request.requiredOption("query")).isEqualTo("RUBY");
        assertThat(request.requiredLongOption("to")).isEqualTo(30_899_999L);
        assertThat(request.toString()).doesNotContain("http");
    }

    @Test
    void rejectsUnknownCommandsAndValuelessOptions() {
        assertThatThrownBy(() -> CliParser.parse(List.of("unknown")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("unknown command: unknown");
        assertThatThrownBy(() -> CliParser.parse(List.of("uniswap", "scan", "--network")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("--network requires a value");
        assertThatThrownBy(() -> CliParser.parse(List.of("search")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("protocol command requires a protocol prefix");
    }

    @Test
    void parsesTheSingleContractCaptureCommand() {
        CliRequest request = CliParser.parse(List.of(
                "uniswap", "capture", "--network", "ethereum", "--version", "v3",
                "--start-block", "123", "--out", "ticks.csv",
                "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8"));

        assertThat(request.command()).isEqualTo(CliRequest.Command.CAPTURE);
        assertThat(request.requiredAddressArgument()).isEqualTo("0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8");
    }
}
