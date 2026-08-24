package com.qtsurfer.qtstreamx.dex.discovery.uniswap;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcLog;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;

final class DiscoveryTestData {

    static final String PAIR_CREATED_TOPIC =
            "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9";
    static final String POOL_CREATED_TOPIC =
            "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118";
    static final String V2_SWAP_TOPIC =
            "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822";
    static final String V3_SWAP_TOPIC =
            "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";

    private DiscoveryTestData() {}

    static String addressWord(String address) {
        return "0x" + "0".repeat(24) + address.substring(2);
    }

    static String uintWord(int value) {
        return "%064x".formatted(value);
    }

    static byte[] dynamicString(String value) {
        byte[] text = value.getBytes(StandardCharsets.UTF_8);
        int paddedLength = (text.length + 31) / 32 * 32;
        return HexFormat.of().parseHex(
                uintWord(32) + uintWord(text.length)
                        + HexFormat.of().formatHex(text)
                        + "00".repeat(paddedLength - text.length));
    }

    static byte[] uintResult(int value) {
        return HexFormat.of().parseHex(uintWord(value));
    }

    static UniswapPairOrientation exactOrientation(
            String network,
            String token0Address,
            String token1Address,
            Instrument instrument) {
        return (candidateNetwork, token0, token1) -> candidateNetwork.equals(network)
                        && token0.address().equals(token0Address)
                        && token1.address().equals(token1Address)
                ? Optional.of(instrument)
                : Optional.empty();
    }

    static EvmRpcLog v2Event(
            String factory,
            String token0,
            String token1,
            String pair,
            long blockNumber,
            String transactionHash,
            int logIndex) {
        return new EvmRpcLog(
                factory,
                List.of(PAIR_CREATED_TOPIC, addressWord(token0), addressWord(token1)),
                "0x" + addressWord(pair).substring(2) + uintWord(1),
                blockNumber,
                "0xblock" + blockNumber,
                transactionHash,
                0,
                logIndex,
                false);
    }

    static EvmRpcLog v3Event(
            String factory,
            String token0,
            String token1,
            int feeTier,
            String pool,
            long blockNumber,
            String transactionHash,
            int logIndex) {
        return new EvmRpcLog(
                factory,
                List.of(
                        POOL_CREATED_TOPIC,
                        addressWord(token0),
                        addressWord(token1),
                        "0x" + uintWord(feeTier)),
                "0x" + uintWord(10) + addressWord(pool).substring(2),
                blockNumber,
                "0xblock" + blockNumber,
                transactionHash,
                0,
                logIndex,
                false);
    }

    static EvmRpcLog swapEvent(String market, String topic, long blockNumber) {
        return new EvmRpcLog(
                market,
                List.of(topic),
                "0x",
                blockNumber,
                "0xblock" + blockNumber,
                "0xswap" + blockNumber,
                0,
                0,
                false);
    }
}
