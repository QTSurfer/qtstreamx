package com.qtsurfer.qtstreamx.canary;

import com.qtsurfer.qtstreamx.evm.rpc.FileEvmLogCheckpointStore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.qtsurfer.qtstreamx.evm.rpc.EvmLogCheckpoint;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogStreamId;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileEvmLogCheckpointStoreTest {

    private static final EvmLogStreamId STREAM_ID =
            new EvmLogStreamId("eip155:1", "uniswap-v2-usdc-weth");

    @TempDir
    Path directory;

    @Test
    void persistsCheckpointAcrossStoreInstances() throws Exception {
        EvmLogCheckpoint expected = new EvmLogCheckpoint(
                STREAM_ID, 19_123_456, "0x" + "a".repeat(64));

        new FileEvmLogCheckpointStore(directory).save(expected);

        assertThat(new FileEvmLogCheckpointStore(directory).load(STREAM_ID))
                .contains(expected);
    }

    @Test
    void atomicallyReplacesAnOlderCheckpointWithoutTemporaryResidue() throws Exception {
        FileEvmLogCheckpointStore store = new FileEvmLogCheckpointStore(directory);
        store.save(new EvmLogCheckpoint(STREAM_ID, 100, "0x" + "a".repeat(64)));
        EvmLogCheckpoint replacement =
                new EvmLogCheckpoint(STREAM_ID, 101, "0x" + "b".repeat(64));

        store.save(replacement);

        assertThat(store.load(STREAM_ID)).contains(replacement);
        try (var files = Files.list(directory)) {
            assertThat(files.toList())
                    .singleElement()
                    .satisfies(path -> assertThat(path.getFileName().toString())
                            .matches("[0-9a-f]{64}\\.json"));
        }
    }

    @Test
    void failsClosedOnCorruptedCheckpointContent() throws Exception {
        FileEvmLogCheckpointStore store = new FileEvmLogCheckpointStore(directory);
        store.save(new EvmLogCheckpoint(STREAM_ID, 100, "0x" + "a".repeat(64)));
        Path checkpoint;
        try (var files = Files.list(directory)) {
            checkpoint = files.findFirst().orElseThrow();
        }
        Files.writeString(checkpoint, "{\"streamId\":");

        assertThatThrownBy(() -> store.load(STREAM_ID))
                .isInstanceOf(IOException.class);
    }
}
