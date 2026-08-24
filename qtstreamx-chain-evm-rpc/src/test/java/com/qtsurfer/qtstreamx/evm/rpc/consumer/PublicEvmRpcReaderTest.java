package com.qtsurfer.qtstreamx.evm.rpc.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.evm.rpc.EvmBlockTag;
import com.qtsurfer.qtstreamx.evm.rpc.EvmHttpRpcReader;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcReader;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcReaderConfig;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class PublicEvmRpcReaderTest {

    @Test
    void constructsThroughPublicTypesWithoutExposingHttpInternals() {
        EvmRpcReaderConfig config = new EvmRpcReaderConfig(
                "eip155:1",
                "https://rpc.invalid/private",
                2_000,
                Duration.ofSeconds(5),
                3);

        EvmRpcReader reader = new EvmHttpRpcReader(config);

        assertThat(reader).isInstanceOf(EvmHttpRpcReader.class);
        assertThat(EvmBlockTag.number(123)).hasToString("0x7b");
        assertThat(config.toString()).doesNotContain("rpc.invalid");
    }
}
