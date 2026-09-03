package com.qtsurfer.qtstreamx.dex.capture.csv;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIOException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class QtsurferTickerCsvFormatterTest {

    @TempDir
    Path directory;

    @Test
    void mapsTradesWithoutRoundingOrLosingMicrosecondTimestamps() throws Exception {
        Path source = writeRaw("event-1,1700000000123456,2000.000000,1.2300,2460.000000,BUY\n");
        Path destination = directory.resolve("qtsurfer-ticks.csv");

        new QtsurferTickerCsvFormatter().format(source, destination);

        assertThat(Files.readString(destination, StandardCharsets.UTF_8)).isEqualTo(
                "timestamp,close,volume,quoteVolume\n"
                        + "1700000000123456,2000.000000,1.2300,2460.000000\n");
    }

    @Test
    void rejectsInvalidSourcesBeforeCreatingDestination() throws Exception {
        Path source = writeRaw("event-1,not-a-time,2000,1,2000,BUY\n");
        Path destination = directory.resolve("qtsurfer-ticks.csv");

        assertThatIOException().isThrownBy(() -> new QtsurferTickerCsvFormatter().format(source, destination));

        assertThat(Files.exists(destination)).isFalse();
    }

    @Test
    void neverReplacesAnExistingDestination() throws Exception {
        Path source = writeRaw("event-1,1700000000123456,2000,1,2000,BUY\n");
        Path destination = directory.resolve("qtsurfer-ticks.csv");
        Files.writeString(destination, "keep\n", StandardCharsets.UTF_8);

        assertThatIOException().isThrownBy(() -> new QtsurferTickerCsvFormatter().format(source, destination));

        assertThat(Files.readString(destination, StandardCharsets.UTF_8)).isEqualTo("keep\n");
    }

    private Path writeRaw(String rows) throws Exception {
        Path source = directory.resolve("trades.csv");
        Files.writeString(source, CsvMarketTradeSink.HEADER + "\n" + rows, StandardCharsets.UTF_8);
        return source;
    }
}
