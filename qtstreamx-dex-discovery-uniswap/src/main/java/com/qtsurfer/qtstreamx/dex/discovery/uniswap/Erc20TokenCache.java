package com.qtsurfer.qtstreamx.dex.discovery.uniswap;

import com.qtsurfer.qtstreamx.dex.core.EvmToken;
import com.qtsurfer.qtstreamx.evm.rpc.EvmBlockTag;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcReader;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;

final class Erc20TokenCache {

    private static final byte[] SYMBOL_CALL = HexFormat.of().parseHex("95d89b41");
    private static final byte[] DECIMALS_CALL = HexFormat.of().parseHex("313ce567");

    private final EvmRpcReader reader;
    private final Map<String, EvmToken> tokens = new LinkedHashMap<>();

    Erc20TokenCache(EvmRpcReader reader) {
        this.reader = reader;
    }

    EvmToken resolve(String address, long safeHead) {
        return tokens.computeIfAbsent(address, ignored -> new EvmToken(
                EvmAbi.textResult(reader.call(address, SYMBOL_CALL, EvmBlockTag.number(safeHead))),
                address,
                EvmAbi.uint8Result(reader.call(address, DECIMALS_CALL, EvmBlockTag.number(safeHead)))));
    }

    boolean contains(String address) {
        return tokens.containsKey(address);
    }
}
