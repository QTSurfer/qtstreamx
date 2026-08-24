package com.qtsurfer.qtstreamx.evm.rpc;

import static com.qtsurfer.qtstreamx.evm.rpc.JsonRpcTestResponses.error;
import static com.qtsurfer.qtstreamx.evm.rpc.JsonRpcTestResponses.result;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class JsonRpcHttpClientTest {
    @Test
    void requestsChainId() {
        List<String> requests = new ArrayList<>();
        JsonRpcHttpTransport transport = transportReturning("\"0x1237\"", requests);
        JsonRpcHttpClient client = new JsonRpcHttpClient(config(), transport);

        assertThat(client.chainId()).isEqualTo(4663);
        assertThat(requests).singleElement().asString().contains("\"method\":\"eth_chainId\"");
    }

    @Test
    void requestsNamedBlocksForCapabilityEvidence() {
        List<String> requests = new ArrayList<>();
        JsonRpcHttpTransport transport = transportReturning("""
                {
                  "number":"0x64",
                  "hash":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                  "timestamp":"0x2"
                }
                """, requests);
        JsonRpcHttpClient client = new JsonRpcHttpClient(config(), transport);

        EvmBlock block = client.getBlock(EvmBlockTag.safe());

        assertThat(block.number()).isEqualTo(100);
        assertThat(block.timestamp()).isEqualTo(2_000_000);
        assertThat(requests).singleElement().asString().contains("[\"safe\",false]");
    }

    @Test
    void readsLatestBlockNumberFromHexResult() {
        List<String> requests = new ArrayList<>();
        JsonRpcHttpTransport transport = transportReturning("\"0x10f\"", requests);
        JsonRpcHttpClient client = new JsonRpcHttpClient(config(), transport);

        long latestBlock = client.latestBlockNumber();

        assertThat(latestBlock).isEqualTo(271);
        assertThat(requests).singleElement().asString().contains("\"method\":\"eth_blockNumber\"");
    }

    @Test
    void readsCanonicalBlockTimestampAsEpochMicroseconds() {
        List<String> requests = new ArrayList<>();
        JsonRpcHttpTransport transport = transportReturning("""
                {"number":"0x64","hash":"0xblock100","timestamp":"0x6553f100"}
                """, requests);
        JsonRpcHttpClient client = new JsonRpcHttpClient(config(), transport);

        EvmBlock block = client.getBlock(100);

        assertThat(block).isEqualTo(new EvmBlock(100, "0xblock100", 1_700_000_000_000_000L));
        assertThat(requests).singleElement().asString()
                .contains("\"method\":\"eth_getBlockByNumber\"")
                .contains("\"params\":[\"0x64\",false]");
    }

    @Test
    void readsLogsForConfiguredContractsAndEventTopics() {
        List<String> requests = new ArrayList<>();
        JsonRpcHttpTransport transport = transportReturning("""
                [{
                  "address":"0xpool","topics":["0xswap","0xsender"],"data":"0xdata",
                  "blockNumber":"0x64","blockHash":"0xblock100","transactionHash":"0xtx",
                  "transactionIndex":"0x3","logIndex":"0x7","removed":false
                }]
                """, requests);
        JsonRpcHttpClient client = new JsonRpcHttpClient(config(), transport);

        List<EvmRpcLog> logs = client.getLogs(
                new EvmLogFilter(Set.of("0xpool"), Set.of("0xswap")), 100, 120);

        assertThat(logs).containsExactly(new EvmRpcLog(
                "0xpool",
                List.of("0xswap", "0xsender"),
                "0xdata",
                100,
                "0xblock100",
                "0xtx",
                3,
                7,
                false));
        assertThat(requests).singleElement().asString()
                .contains("\"method\":\"eth_getLogs\"")
                .contains("\"fromBlock\":\"0x64\"")
                .contains("\"toBlock\":\"0x78\"")
                .contains("\"address\":[\"0xpool\"]")
                .contains("\"topics\":[[\"0xswap\"]]");
    }

    @Test
    void retriesTransientTransportFailuresWithExponentialBackoff() {
        AtomicInteger attempts = new AtomicInteger();
        List<Duration> delays = new ArrayList<>();
        JsonRpcHttpTransport transport = (request, timeout) -> {
            if (attempts.incrementAndGet() < 3) {
                throw new IOException("temporary provider failure");
            }
            return result("\"0x10f\"");
        };
        JsonRpcHttpClient client = new JsonRpcHttpClient(config(), transport, delays::add);

        long latestBlock = client.latestBlockNumber();

        assertThat(latestBlock).isEqualTo(271);
        assertThat(attempts).hasValue(3);
        assertThat(delays).containsExactly(Duration.ofMillis(100), Duration.ofMillis(200));
    }

    @Test
    void doesNotRetryJsonRpcProtocolError() {
        AtomicInteger attempts = new AtomicInteger();
        List<Duration> delays = new ArrayList<>();
        JsonRpcHttpTransport transport = (request, timeout) -> {
            attempts.incrementAndGet();
            return error(-32602, "invalid filter");
        };
        JsonRpcHttpClient client = new JsonRpcHttpClient(config(), transport, delays::add);

        assertThatThrownBy(client::latestBlockNumber)
                .isInstanceOf(EvmRpcException.class)
                .hasMessage("JSON-RPC request failed with code -32602")
                .hasMessageNotContaining("invalid filter");
        assertThat(attempts).hasValue(1);
        assertThat(delays).isEmpty();
    }

    private static JsonRpcHttpTransport transportReturning(String rawResult, List<String> requests) {
        return (request, timeout) -> {
            requests.add(request);
            return result(rawResult);
        };
    }

    private static EvmLogStreamConfig config() {
        return new EvmLogStreamConfig(
                "eip155:1",
                "wss://rpc.invalid/ws",
                "https://rpc.invalid/http",
                100,
                2,
                2_000,
                Duration.ofSeconds(5),
                3);
    }
}
