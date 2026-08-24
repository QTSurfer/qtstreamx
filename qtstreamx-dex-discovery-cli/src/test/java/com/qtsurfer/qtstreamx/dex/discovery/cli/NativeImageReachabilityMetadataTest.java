package com.qtsurfer.qtstreamx.dex.discovery.cli;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogCheckpoint;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogStreamId;
import com.qtsurfer.qtstreamx.evm.rpc.FileEvmLogCheckpointStore;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assumptions.assumeFalse;

/** Guards the reviewed Jackson reflection surface required by the native CLI image. */
class NativeImageReachabilityMetadataTest {
    private static final String RESOURCE =
            "META-INF/native-image/com.qtsurfer.qtstreamx/"
                    + "qtstreamx-dex-discovery-cli/reflect-config.json";

    @Test
    void declaresEveryJacksonRecordCrossingTheCliOrCheckpointBoundary() throws Exception {
        assumeFalse("runtime".equals(System.getProperty("org.graalvm.nativeimage.imagecode")),
                "the versioned resource is verified by the JVM test suite");
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(RESOURCE)) {
            assertThat(input).as("native-image reflection metadata").isNotNull();
            List<Map<String, Object>> entries = new ObjectMapper().readValue(
                    input, new TypeReference<>() {});

            assertThat(entries)
                    .extracting(entry -> entry.get("name"))
                    .containsExactlyInAnyOrder(
                            CliResponse.class.getName(),
                            CliData.Protocol.class.getName(),
                            CliData.Network.class.getName(),
                            CliData.Token.class.getName(),
                            CliData.Market.class.getName(),
                            CliData.Pool.class.getName(),
                            CliData.FactoryMarket.class.getName(),
                            CliData.TokenLookup.class.getName(),
                            CliData.SearchMatch.class.getName(),
                            "com.qtsurfer.qtstreamx.evm.rpc.EvmLogCheckpoint",
                            "com.qtsurfer.qtstreamx.evm.rpc.EvmLogStreamId");
            assertThat(entries).allSatisfy(entry -> {
                assertThat(entry).containsEntry("allDeclaredConstructors", true)
                        .containsEntry("allDeclaredMethods", true)
                        .containsEntry("allDeclaredFields", true);
            });
        }
    }

    @Test
    void checkpointJacksonBoundaryRoundTripsThroughTheDurableStore() throws Exception {
        Path directory = Files.createTempDirectory("qtstreamx-native-checkpoint-");
        EvmLogStreamId streamId = new EvmLogStreamId("eip155:1", "uniswap-v3.pool");
        EvmLogCheckpoint expected = new EvmLogCheckpoint(streamId, 24_000_000, "0xcanonical");

        FileEvmLogCheckpointStore store = new FileEvmLogCheckpointStore(directory);
        store.save(expected);

        assertThat(store.load(streamId)).contains(expected);
    }
}
