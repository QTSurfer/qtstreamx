package com.qtsurfer.qtstreamx.dex.capture.csv;

import com.qtsurfer.qtstreamx.core.model.TradeSide;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Converts one durable normalized-trade capture into the current QTSurfer ticker CSV contract. */
public final class QtsurferTickerCsvFormatter {
    /** Exact header accepted by the QTSurfer ticker dataset ingest. */
    public static final String HEADER = "timestamp,close,volume,quoteVolume";

    private static final int RAW_COLUMN_COUNT = 6;

    /**
     * Validates a complete raw capture before creating a new QTSurfer ticker CSV.
     *
     * @param source raw capture written by {@link CsvMarketTradeSink}
     * @param destination new derived ticker CSV; it must not already exist
     * @throws IOException when either path is unsafe or the source capture is incompatible
     */
    public void format(Path source, Path destination) throws IOException {
        Path sourcePath = Objects.requireNonNull(source, "source").toAbsolutePath().normalize();
        Path destinationPath = Objects.requireNonNull(destination, "destination").toAbsolutePath().normalize();
        if (sourcePath.equals(destinationPath)) {
            throw new IOException("QTSurfer ticker destination must differ from the raw capture");
        }
        if (!Files.isRegularFile(sourcePath)) {
            throw new IOException("raw capture must be an existing file");
        }
        if (Files.exists(destinationPath)) {
            throw new IOException("QTSurfer ticker destination already exists");
        }

        List<List<String>> rows = parseRows(decodeUtf8(Files.readAllBytes(sourcePath)));
        validateHeader(rows);
        String output = formatRows(rows);

        Path parent = destinationPath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.writeString(destinationPath, output, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
    }

    private static void validateHeader(List<List<String>> rows) throws IOException {
        if (rows.size() < 2) {
            throw new IOException("raw capture contains no trade rows");
        }
        if (!rows.getFirst().equals(List.of(CsvMarketTradeSink.HEADER.split(",", -1)))) {
            throw new IOException("raw capture has an incompatible header");
        }
    }

    private static String formatRows(List<List<String>> rows) throws IOException {
        StringBuilder output = new StringBuilder(HEADER).append('\n');
        for (int index = 1; index < rows.size(); index++) {
            List<String> row = rows.get(index);
            validateRawRow(row, index + 1);
            output.append(row.get(1)).append(',')
                    .append(row.get(2)).append(',')
                    .append(row.get(3)).append(',')
                    .append(row.get(4)).append('\n');
        }
        return output.toString();
    }

    private static void validateRawRow(List<String> row, int lineNumber) throws IOException {
        if (row.size() != RAW_COLUMN_COUNT || row.stream().anyMatch(String::isBlank)) {
            throw new IOException("raw capture line " + lineNumber + " has an invalid column count or blank value");
        }
        try {
            if (Long.parseLong(row.get(1)) < 0) {
                throw new IllegalArgumentException("negative timestamp");
            }
            validatePositiveDecimal(row.get(2));
            validatePositiveDecimal(row.get(3));
            validatePositiveDecimal(row.get(4));
            TradeSide.valueOf(row.get(5));
        } catch (IllegalArgumentException exception) {
            throw new IOException("raw capture line " + lineNumber + " has an invalid normalized trade", exception);
        }
    }

    private static void validatePositiveDecimal(String value) {
        if (value.contains("e") || value.contains("E") || new BigDecimal(value).signum() <= 0) {
            throw new IllegalArgumentException("invalid decimal");
        }
    }

    private static String decodeUtf8(byte[] bytes) throws IOException {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException exception) {
            throw new IOException("raw capture is not valid UTF-8", exception);
        }
    }

    private static List<List<String>> parseRows(String content) throws IOException {
        List<List<String>> rows = new ArrayList<>();
        List<String> row = new ArrayList<>();
        StringBuilder field = new StringBuilder();
        boolean quoted = false;
        boolean atFieldStart = true;
        for (int index = 0; index < content.length(); index++) {
            char character = content.charAt(index);
            if (quoted) {
                if (character == '"' && index + 1 < content.length() && content.charAt(index + 1) == '"') {
                    field.append(character);
                    index++;
                } else if (character == '"') {
                    quoted = false;
                } else {
                    field.append(character);
                }
            } else if (character == '"') {
                if (!atFieldStart) {
                    throw new IOException("raw capture has an invalid quote");
                }
                quoted = true;
            } else if (character == ',') {
                row.add(field.toString());
                field.setLength(0);
                atFieldStart = true;
            } else if (character == '\n') {
                row.add(field.toString());
                rows.add(List.copyOf(row));
                row = new ArrayList<>();
                field.setLength(0);
                atFieldStart = true;
            } else if (character == '\r') {
                throw new IOException("raw capture must use LF line terminators");
            } else {
                field.append(character);
                atFieldStart = false;
            }
        }
        if (quoted) {
            throw new IOException("raw capture has an unterminated quoted field");
        }
        if (!row.isEmpty() || field.length() > 0) {
            throw new IOException("raw capture must end each record with LF");
        }
        return List.copyOf(rows);
    }
}
