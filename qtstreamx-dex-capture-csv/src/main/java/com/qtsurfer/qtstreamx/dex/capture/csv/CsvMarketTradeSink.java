package com.qtsurfer.qtstreamx.dex.capture.csv;

import com.qtsurfer.qtstreamx.core.client.MarketTradeAcknowledgement;
import com.qtsurfer.qtstreamx.core.client.MarketTradeBatch;
import com.qtsurfer.qtstreamx.core.client.MarketTradeBatchHandler;
import com.qtsurfer.qtstreamx.core.model.MarketTrade;
import com.qtsurfer.qtstreamx.core.model.MarketId;
import com.qtsurfer.qtstreamx.core.model.TradeSide;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Durable append-only CSV sink for normalized DEX market trades.
 *
 * <p>The sink writes UTF-8 rows using a fixed schema and only acknowledges a batch after the
 * appended bytes have been forced to disk. Event IDs are rebuilt from an existing file on reopen,
 * making source replay idempotent.
 */
public final class CsvMarketTradeSink implements MarketTradeBatchHandler, AutoCloseable {
    /** Exact header written to new capture files. */
    public static final String HEADER =
            "event_id,timestamp_us,price,base_amount,quote_amount,side";

    /** Exact header written to the sibling metadata file. */
    public static final String METADATA_HEADER =
            "venue,network,contract,instrument,date_from_us,date_to_us";

    private static final int COLUMN_COUNT = 6;

    private final Path outputPath;
    private final Path metadataPath;
    private final MarketId market;
    private final CsvFileAccess fileAccess;
    private final Set<String> eventIds;
    private boolean failed;
    private Long dateFrom;
    private Long dateTo;

    /**
     * Opens or creates a CSV capture at the explicit output path.
     *
     * @param outputPath target CSV file
     * @throws IOException when the existing capture is malformed or cannot be opened safely
     */
    public CsvMarketTradeSink(Path outputPath, MarketId market) throws IOException {
        this(outputPath, market, new JdkCsvFileAccess());
    }

    CsvMarketTradeSink(Path outputPath, MarketId market, CsvFileAccess fileAccess) throws IOException {
        this.outputPath = Objects.requireNonNull(outputPath, "outputPath").toAbsolutePath();
        this.metadataPath = this.outputPath.resolveSibling(this.outputPath.getFileName() + ".metadata.csv");
        this.market = Objects.requireNonNull(market, "market");
        this.fileAccess = Objects.requireNonNull(fileAccess, "fileAccess");
        this.eventIds = new LinkedHashSet<>();
        initialise();
    }

    /** Returns the file receiving this capture. */
    public Path outputPath() {
        return outputPath;
    }

    /** Returns the sibling file containing stable market and capture-range metadata. */
    public Path metadataPath() {
        return metadataPath;
    }

    /**
     * Appends previously unseen trades and forces their bytes to disk before acknowledgement.
     *
     * @param batch ordered normalized trades offered at one source acknowledgement boundary
     * @return {@link MarketTradeAcknowledgement#ACKNOWLEDGED} after durable persistence
     * @throws IOException when writing or syncing fails; future calls fail closed
     */
    @Override
    public synchronized MarketTradeAcknowledgement handle(MarketTradeBatch batch) throws IOException {
        Objects.requireNonNull(batch, "batch");
        if (failed) {
            throw new IOException("CSV capture is unavailable after a previous write failure");
        }

        List<MarketTrade> newTrades = new ArrayList<>();
        Set<String> batchEventIds = new LinkedHashSet<>();
        for (MarketTrade trade : batch.trades()) {
            if (!market.equals(trade.market())) {
                throw new IOException("CSV capture received a trade for a different market");
            }
            if (!eventIds.contains(trade.eventId()) && batchEventIds.add(trade.eventId())) {
                newTrades.add(trade);
            }
        }
        if (newTrades.isEmpty()) {
            return MarketTradeAcknowledgement.ACKNOWLEDGED;
        }

        StringBuilder rows = new StringBuilder();
        for (MarketTrade trade : newTrades) {
            appendRow(rows, trade);
        }
        try {
            fileAccess.appendAndForce(outputPath, rows.toString().getBytes(StandardCharsets.UTF_8));
        } catch (IOException exception) {
            failed = true;
            throw exception;
        }
        newTrades.forEach(trade -> {
            eventIds.add(trade.eventId());
            dateFrom = dateFrom == null ? trade.timestamp() : Math.min(dateFrom, trade.timestamp());
            dateTo = dateTo == null ? trade.timestamp() : Math.max(dateTo, trade.timestamp());
        });
        return MarketTradeAcknowledgement.ACKNOWLEDGED;
    }

    /** No resources are retained between writes. */
    @Override
    public synchronized void close() throws IOException {
        if (!failed) {
            String from = dateFrom == null ? "" : Long.toString(dateFrom);
            String to = dateTo == null ? "" : Long.toString(dateTo);
            String metadata = METADATA_HEADER + "\n"
                    + market.venue() + ',' + market.network() + ',' + market.nativeId() + ','
                    + market.instrument().symbol() + ',' + from + ',' + to + "\n";
            Files.writeString(metadataPath, metadata, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        }
    }

    private void initialise() throws IOException {
        if (fileAccess.exists(outputPath)) {
            if (fileAccess.isDirectory(outputPath)) {
                throw new IOException("CSV capture path must be a file");
            }
            loadExisting(fileAccess.readAllBytes(outputPath));
            return;
        }
        Path parent = outputPath.getParent();
        if (parent != null) {
            fileAccess.createDirectories(parent);
        }
        fileAccess.appendAndForce(outputPath, (HEADER + "\n").getBytes(StandardCharsets.UTF_8));
    }

    private void loadExisting(byte[] bytes) throws IOException {
        List<List<String>> rows = parseRows(decodeUtf8(bytes));
        if (rows.isEmpty() || !rows.getFirst().equals(List.of(HEADER.split(",", -1)))) {
            throw new IOException("CSV capture has an incompatible header");
        }
        for (int index = 1; index < rows.size(); index++) {
            List<String> row = rows.get(index);
            validateRow(row, index + 1);
            if (!eventIds.add(row.getFirst())) {
                throw new IOException("CSV capture contains a duplicate event ID");
            }
            long timestamp = Long.parseLong(row.get(1));
            dateFrom = dateFrom == null ? timestamp : Math.min(dateFrom, timestamp);
            dateTo = dateTo == null ? timestamp : Math.max(dateTo, timestamp);
        }
    }

    private static String decodeUtf8(byte[] bytes) throws IOException {
        try {
            return StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException exception) {
            throw new IOException("CSV capture is not valid UTF-8", exception);
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
                if (character == '"') {
                    if (index + 1 < content.length() && content.charAt(index + 1) == '"') {
                        field.append(character);
                        index++;
                    } else {
                        quoted = false;
                    }
                } else {
                    field.append(character);
                }
                continue;
            }
            if (character == '"') {
                if (!atFieldStart) {
                    throw new IOException("CSV capture has an invalid quote");
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
                throw new IOException("CSV capture must use LF line terminators");
            } else {
                field.append(character);
                atFieldStart = false;
            }
        }
        if (quoted) {
            throw new IOException("CSV capture has an unterminated quoted field");
        }
        if (!row.isEmpty() || field.length() > 0) {
            throw new IOException("CSV capture must end each record with LF");
        }
        return List.copyOf(rows);
    }

    private static void validateRow(List<String> row, int lineNumber) throws IOException {
        if (row.size() != COLUMN_COUNT) {
            throw new IOException("CSV capture line " + lineNumber + " has an invalid column count");
        }
        if (row.stream().anyMatch(String::isBlank)) {
            throw new IOException("CSV capture line " + lineNumber + " has a blank value");
        }
        try {
            long timestamp = Long.parseLong(row.get(1));
            if (timestamp < 0) {
                throw new IllegalArgumentException("negative timestamp");
            }
            validateDecimal(row.get(2));
            validateDecimal(row.get(3));
            validateDecimal(row.get(4));
            TradeSide.valueOf(row.get(5));
        } catch (IllegalArgumentException exception) {
            throw new IOException("CSV capture line " + lineNumber + " has an invalid normalized trade", exception);
        }
    }

    private static void validateDecimal(String value) {
        if (value.contains("e") || value.contains("E")) {
            throw new IllegalArgumentException("scientific notation");
        }
        if (new BigDecimal(value).signum() <= 0) {
            throw new IllegalArgumentException("non-positive decimal");
        }
    }

    private static void appendRow(StringBuilder target, MarketTrade trade) {
        List<String> fields = List.of(
                trade.eventId(),
                Long.toString(trade.timestamp()),
                trade.price().toPlainString(),
                trade.baseAmount().toPlainString(),
                trade.quoteAmount().toPlainString(),
                trade.side().name());
        for (int index = 0; index < fields.size(); index++) {
            if (index > 0) {
                target.append(',');
            }
            appendField(target, fields.get(index));
        }
        target.append('\n');
    }

    private static void appendField(StringBuilder target, String value) {
        boolean quote = value.indexOf(',') >= 0 || value.indexOf('"') >= 0 || value.indexOf('\n') >= 0;
        if (quote) {
            target.append('"');
            for (int index = 0; index < value.length(); index++) {
                char character = value.charAt(index);
                if (character == '"') {
                    target.append('"');
                }
                target.append(character);
            }
            target.append('"');
        } else {
            target.append(value);
        }
    }

    interface CsvFileAccess {
        boolean exists(Path path);

        boolean isDirectory(Path path);

        byte[] readAllBytes(Path path) throws IOException;

        void createDirectories(Path path) throws IOException;

        void appendAndForce(Path path, byte[] bytes) throws IOException;
    }

    private static final class JdkCsvFileAccess implements CsvFileAccess {
        @Override
        public boolean exists(Path path) {
            return Files.exists(path);
        }

        @Override
        public boolean isDirectory(Path path) {
            return Files.isDirectory(path);
        }

        @Override
        public byte[] readAllBytes(Path path) throws IOException {
            return Files.readAllBytes(path);
        }

        @Override
        public void createDirectories(Path path) throws IOException {
            Files.createDirectories(path);
        }

        @Override
        public void appendAndForce(Path path, byte[] bytes) throws IOException {
            try (FileChannel channel = FileChannel.open(
                    path,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND)) {
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                channel.force(true);
            }
        }
    }
}
