package com.qtsurfer.qtstreamx.evm.rpc;

import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.ws.jdk.JdkWebSocketClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Live opt-in smoke test for a caller-provided EVM network and contract event. */
@Tag("it")
class EvmRpcLiveIT {

    @Test
    void capturesConfiguredConfirmedLog() throws Exception {
        String network = environment("QTSTREAMX_EVM_NETWORK");
        String wsUrl = environment("QTSTREAMX_EVM_WS_URL");
        String httpUrl = environment("QTSTREAMX_EVM_HTTP_URL");
        String startBlock = environment("QTSTREAMX_EVM_START_BLOCK");
        String poolAddress = environment("QTSTREAMX_EVM_POOL_ADDRESS");
        String eventTopic = environment("QTSTREAMX_EVM_EVENT_TOPIC");
        Assumptions.assumeTrue(
                java.util.stream.Stream.of(network, wsUrl, httpUrl, startBlock, poolAddress, eventTopic)
                        .allMatch(value -> value != null && !value.isBlank()),
                "live EVM environment is not configured");

        EvmLogStreamConfig config = new EvmLogStreamConfig(
                network,
                wsUrl,
                httpUrl,
                Long.parseLong(startBlock),
                1,
                2_000,
                Duration.ofSeconds(15),
                3);
        CountDownLatch received = new CountDownLatch(1);
        List<Throwable> errors = new ArrayList<>();

        try (EvmLogStream stream = new EvmRpcLogStream(config, JdkWebSocketClient::new)) {
            stream.onError(errors::add);
            stream.start(
                    new EvmLogFilter(Set.of(poolAddress), Set.of(eventTopic)),
                    ignored -> received.countDown());
            assertThat(stream.isConnected()).isTrue();
            assertThat(received.await(60, TimeUnit.SECONDS)).isTrue();
        }

        assertThat(errors).isEmpty();
    }

    private static String environment(String name) {
        return System.getenv(name);
    }
}
