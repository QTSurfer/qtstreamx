package com.qtsurfer.qtstreamx.dex.discovery.uniswap;

import static com.qtsurfer.qtstreamx.dex.discovery.uniswap.DiscoveryTestData.dynamicString;
import static com.qtsurfer.qtstreamx.dex.discovery.uniswap.DiscoveryTestData.uintResult;

import com.qtsurfer.qtstreamx.evm.rpc.EvmBlockTag;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogFilter;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcLog;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcReader;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

final class RecordingEvmRpcReader implements EvmRpcReader {

    private final LogSource logSource;
    private final Map<String, TokenMetadata> metadata;
    final List<EvmLogFilter> filters = new ArrayList<>();
    final List<String> ranges = new ArrayList<>();
    final List<String> calls = new ArrayList<>();

    RecordingEvmRpcReader(List<EvmRpcLog> logs, Map<String, TokenMetadata> metadata) {
        this((filter, fromBlock, toBlock) -> List.copyOf(logs), metadata);
    }

    RecordingEvmRpcReader(
            BiFunction<Long, Long, List<EvmRpcLog>> logSource,
            Map<String, TokenMetadata> metadata) {
        this((filter, fromBlock, toBlock) -> logSource.apply(fromBlock, toBlock), metadata);
    }

    RecordingEvmRpcReader(LogSource logSource, Map<String, TokenMetadata> metadata) {
        this.logSource = logSource;
        this.metadata = Map.copyOf(metadata);
    }

    @Override
    public long latestBlockNumber() {
        return 120;
    }

    @Override
    public List<EvmRpcLog> logs(EvmLogFilter filter, long fromBlock, long toBlock) {
        filters.add(filter);
        ranges.add(fromBlock + "-" + toBlock);
        return List.copyOf(logSource.logs(filter, fromBlock, toBlock));
    }

    @Override
    public byte[] call(String contractAddress, byte[] data, EvmBlockTag blockTag) {
        String selector = HexFormat.of().formatHex(data);
        calls.add(contractAddress + ":" + selector + ":" + blockTag);
        TokenMetadata token = metadata.get(contractAddress);
        if (token == null) {
            throw new IllegalStateException("No metadata for token " + contractAddress);
        }
        return switch (selector) {
            case "95d89b41" -> dynamicString(token.symbol());
            case "313ce567" -> uintResult(token.decimals());
            default -> throw new AssertionError("Unexpected selector " + selector);
        };
    }

    record TokenMetadata(String symbol, int decimals) {}

    @FunctionalInterface
    interface LogSource {
        List<EvmRpcLog> logs(EvmLogFilter filter, long fromBlock, long toBlock);
    }
}
