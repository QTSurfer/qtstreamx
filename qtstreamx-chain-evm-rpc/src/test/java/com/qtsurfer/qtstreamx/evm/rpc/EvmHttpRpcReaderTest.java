package com.qtsurfer.qtstreamx.evm.rpc;

import static com.qtsurfer.qtstreamx.evm.rpc.JsonRpcTestResponses.error;
import static com.qtsurfer.qtstreamx.evm.rpc.JsonRpcTestResponses.result;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class EvmHttpRpcReaderTest {

    @Test
    void readsLatestBlockNumberThroughThePublicReader() {
        List<String> requests = new ArrayList<>();
        JsonRpcHttpTransport transport = (request, timeout) -> {
            requests.add(request);
            return result("\"0x10f\"");
        };
        EvmRpcReader reader = new EvmHttpRpcReader(config(), transport, ignored -> {});

        long latestBlock = reader.latestBlockNumber();

        assertThat(latestBlock).isEqualTo(271);
        assertThat(requests).singleElement().asString()
                .contains("\"method\":\"eth_blockNumber\"");
    }

    @Test
    void paginatesBoundedLogRangesAndReturnsChainOrder() {
        List<String> requests = new ArrayList<>();
        JsonRpcHttpTransport transport = (request, timeout) -> {
            requests.add(request);
            if (request.contains("\"fromBlock\":\"0x64\"")) {
                return logResponse("0x65", "0x2", "0x3");
            }
            if (request.contains("\"fromBlock\":\"0x66\"")) {
                return logResponse("0x66", "0x1", "0x4");
            }
            return result(3, "[]");
        };
        EvmRpcReader reader = new EvmHttpRpcReader(
                new EvmRpcReaderConfig(
                        "eip155:1",
                        "https://rpc.invalid/private",
                        2,
                        Duration.ofSeconds(5),
                        3),
                transport,
                ignored -> {});

        List<EvmRpcLog> logs = reader.logs(
                new EvmLogFilter(Set.of("0xpool"), Set.of("0xcreated")),
                100,
                104);

        assertThat(logs).extracting(EvmRpcLog::blockNumber).containsExactly(101L, 102L);
        assertThat(requests).hasSize(3);
        assertThat(requests.get(0)).contains("\"fromBlock\":\"0x64\"")
                .contains("\"toBlock\":\"0x65\"");
        assertThat(requests.get(1)).contains("\"fromBlock\":\"0x66\"")
                .contains("\"toBlock\":\"0x67\"");
        assertThat(requests.get(2)).contains("\"fromBlock\":\"0x68\"")
                .contains("\"toBlock\":\"0x68\"");
    }

    @Test
    void bisectsProviderRejectedLogRanges() {
        List<String> requests = new ArrayList<>();
        JsonRpcHttpTransport transport = (request, timeout) -> {
            requests.add(request);
            boolean singleBlock = request.contains("\"fromBlock\":\"0x64\"")
                            && request.contains("\"toBlock\":\"0x64\"")
                    || request.contains("\"fromBlock\":\"0x65\"")
                            && request.contains("\"toBlock\":\"0x65\"")
                    || request.contains("\"fromBlock\":\"0x66\"")
                            && request.contains("\"toBlock\":\"0x66\"")
                    || request.contains("\"fromBlock\":\"0x67\"")
                            && request.contains("\"toBlock\":\"0x67\"");
            if (singleBlock) {
                return result("[]");
            }
            return error(-32005, "range limit");
        };
        EvmRpcReader reader = new EvmHttpRpcReader(
                new EvmRpcReaderConfig(
                        "eip155:1",
                        "https://rpc.invalid/private",
                        4,
                        Duration.ofSeconds(5),
                        3),
                transport,
                ignored -> {});

        List<EvmRpcLog> logs = reader.logs(
                new EvmLogFilter(Set.of("0xpool"), Set.of("0xcreated")),
                100,
                103);

        assertThat(logs).isEmpty();
        assertThat(requests).hasSize(7);
    }

    @Test
    void callsAContractAtAnExplicitBlockAndReturnsOpaqueBytes() {
        List<String> requests = new ArrayList<>();
        JsonRpcHttpTransport transport = (request, timeout) -> {
            requests.add(request);
            return result("\"0x000102ff\"");
        };
        EvmRpcReader reader = new EvmHttpRpcReader(config(), transport, ignored -> {});

        byte[] result = reader.call(
                "0x1111111111111111111111111111111111111111",
                new byte[] {(byte) 0xa9, 0x05, (byte) 0x9c, (byte) 0xbb},
                EvmBlockTag.number(123));

        assertThat(result).containsExactly(0x00, 0x01, 0x02, (byte) 0xff);
        assertThat(requests).singleElement().asString()
                .contains("\"method\":\"eth_call\"")
                .contains("\"to\":\"0x1111111111111111111111111111111111111111\"")
                .contains("\"data\":\"0xa9059cbb\"")
                .contains("\"0x7b\"");
    }

    @Test
    void exposesProtocolErrorCodeWithoutProviderErrorBody() {
        JsonRpcHttpTransport transport = (request, timeout) ->
                error(-32000, "execution reverted: provider detail");
        EvmRpcReader reader = new EvmHttpRpcReader(config(), transport, ignored -> {});

        Throwable thrown = catchThrowable(() -> reader.call(
                "0x1111111111111111111111111111111111111111",
                new byte[0],
                EvmBlockTag.latest()));

        assertThat(thrown).isInstanceOf(EvmRpcException.class)
                .hasMessage("JSON-RPC request failed with code -32000")
                .hasMessageNotContaining("provider detail");
        assertThat(((EvmRpcException) thrown).code()).isEqualTo(-32000);
    }

    @Test
    void returnsEmptyBytesForAnEmptyContractResult() {
        JsonRpcHttpTransport transport = (request, timeout) -> result("\"0x\"");
        EvmRpcReader reader = new EvmHttpRpcReader(config(), transport, ignored -> {});

        byte[] result = reader.call(
                "0x1111111111111111111111111111111111111111",
                new byte[0],
                EvmBlockTag.safe());

        assertThat(result).isEmpty();
    }

    @Test
    void readsContractBytecodeAtAnExplicitBlock() {
        List<String> requests = new ArrayList<>();
        JsonRpcHttpTransport transport = (request, timeout) -> {
            requests.add(request);
            return result("\"0x60016002ff\"");
        };
        EvmRpcReader reader = new EvmHttpRpcReader(config(), transport, ignored -> {});

        byte[] code = reader.code(
                "0xABCDEFabcdefABCDEFabcdefABCDEFabcdefABCD",
                EvmBlockTag.number(321));

        assertThat(code).containsExactly(0x60, 0x01, 0x60, 0x02, (byte) 0xff);
        assertThat(requests).singleElement().asString()
                .contains("\"method\":\"eth_getCode\"")
                .contains("\"0xabcdefabcdefabcdefabcdefabcdefabcdefabcd\"")
                .contains("\"0x141\"");
    }

    private static String logResponse(String block, String transactionIndex, String logIndex) {
        return result("""
                [{
                  "address":"0xpool","topics":["0xcreated"],"data":"0xdata",
                  "blockNumber":"%s","blockHash":"0xblock","transactionHash":"0xtx%s",
                  "transactionIndex":"%s","logIndex":"%s","removed":false
                }]
                """.formatted(block, logIndex, transactionIndex, logIndex));
    }

    private static EvmRpcReaderConfig config() {
        return new EvmRpcReaderConfig(
                "eip155:1",
                "https://rpc.invalid/private",
                2_000,
                Duration.ofSeconds(5),
                3);
    }
}
