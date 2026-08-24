package com.qtsurfer.qtstreamx.dex.discovery.uniswap;

import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcLog;

final class UniswapFactoryEvents {

    static final String V2_CREATED_TOPIC =
            "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9";
    static final String V3_CREATED_TOPIC =
            "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118";

    private UniswapFactoryEvents() {}

    static UniswapFactoryEvent decodeV2(EvmRpcLog log) {
        if (log.topics().size() != 3
                || !V2_CREATED_TOPIC.equalsIgnoreCase(log.topics().getFirst())) {
            throw new IllegalArgumentException("log is not a canonical PairCreated event");
        }
        EvmAbi.requireDataWords(log.data(), 2);
        return new UniswapFactoryEvent(
                EvmAbi.addressTopic(log.topics().get(1)),
                EvmAbi.addressTopic(log.topics().get(2)),
                EvmAbi.addressDataWord(log.data(), 0),
                0);
    }

    static UniswapFactoryEvent decodeV3(EvmRpcLog log) {
        if (log.topics().size() != 4
                || !V3_CREATED_TOPIC.equalsIgnoreCase(log.topics().getFirst())) {
            throw new IllegalArgumentException("log is not a canonical PoolCreated event");
        }
        EvmAbi.requireDataWords(log.data(), 2);
        EvmAbi.int24DataWord(log.data(), 0);
        return new UniswapFactoryEvent(
                EvmAbi.addressTopic(log.topics().get(1)),
                EvmAbi.addressTopic(log.topics().get(2)),
                EvmAbi.addressDataWord(log.data(), 1),
                EvmAbi.uint24Topic(log.topics().get(3)));
    }
}
