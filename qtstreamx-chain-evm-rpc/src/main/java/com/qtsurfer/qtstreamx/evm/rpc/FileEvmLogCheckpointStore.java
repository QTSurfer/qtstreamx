package com.qtsurfer.qtstreamx.evm.rpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/** Persists acknowledged EVM checkpoints as fsynced, atomically replaced JSON files. */
public final class FileEvmLogCheckpointStore implements EvmLogCheckpointStore {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    // Windows has no equivalent of opening a directory as a byte-stream file
    // descriptor; FileChannel.open on a directory throws there. NTFS logs
    // renames in its own journal, so the extra fsync is a POSIX-only step,
    // not a POSIX-only guarantee.
    private static final boolean CAN_FSYNC_DIRECTORY =
            !System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win");
    private final Path directory;

    /** Creates a store rooted at one explicit checkpoint directory. */
    public FileEvmLogCheckpointStore(Path directory) throws IOException {
        this.directory = Objects.requireNonNull(directory, "directory").toAbsolutePath().normalize();
        Files.createDirectories(this.directory);
    }

    @Override
    public synchronized Optional<EvmLogCheckpoint> load(EvmLogStreamId streamId) throws IOException {
        Path path = pathFor(Objects.requireNonNull(streamId, "streamId"));
        if (!Files.exists(path)) return Optional.empty();
        EvmLogCheckpoint checkpoint = MAPPER.readValue(path.toFile(), EvmLogCheckpoint.class);
        if (!streamId.equals(checkpoint.streamId())) throw new IOException("checkpoint identity does not match the requested stream");
        return Optional.of(checkpoint);
    }

    @Override
    public synchronized void save(EvmLogCheckpoint checkpoint) throws IOException {
        Path target = pathFor(Objects.requireNonNull(checkpoint, "checkpoint").streamId());
        Path temporary = Files.createTempFile(directory, target.getFileName().toString(), ".tmp");
        try {
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                ByteBuffer buffer = ByteBuffer.wrap(MAPPER.writeValueAsBytes(checkpoint));
                while (buffer.hasRemaining()) channel.write(buffer);
                channel.force(true);
            }
            try { Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING); }
            catch (AtomicMoveNotSupportedException exception) { throw new IOException("checkpoint store requires atomic file replacement", exception); }
            if (CAN_FSYNC_DIRECTORY) {
                try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) { channel.force(true); }
            }
        } finally {
            // The move already renamed the real file away; this is only reached if the
            // move itself failed. Swallow cleanup failures here so they cannot replace
            // the real exception from the try block above.
            try { Files.deleteIfExists(temporary); } catch (IOException ignored) { }
        }
    }

    private Path pathFor(EvmLogStreamId streamId) {
        try {
            String identity = streamId.network() + '\0' + streamId.streamKey();
            return directory.resolve(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(identity.getBytes(StandardCharsets.UTF_8))) + ".json");
        } catch (java.security.NoSuchAlgorithmException exception) { throw new IllegalStateException("SHA-256 is unavailable", exception); }
    }
}
