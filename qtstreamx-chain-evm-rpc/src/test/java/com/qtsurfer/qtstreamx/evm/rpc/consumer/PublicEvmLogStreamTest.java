package com.qtsurfer.qtstreamx.evm.rpc.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.evm.rpc.EvmLogStream;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogStreamConfig;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcLogStream;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class PublicEvmLogStreamTest {

    @Test
    void constructsWithoutExposingHttpImplementation() throws Exception {
        EvmLogStreamConfig config = new EvmLogStreamConfig(
                "eip155:1",
                "wss://rpc.invalid/ws",
                "https://rpc.invalid/http",
                100,
                2,
                2_000,
                Duration.ofSeconds(5),
                3);

        try (EvmLogStream stream = new EvmRpcLogStream(config, () -> null)) {
            assertThat(stream.isConnected()).isFalse();
        }
    }
}
