package com.qtsurfer.qtstreamx.dex.discovery.uniswap;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.dex.core.EvmToken;
import com.qtsurfer.qtstreamx.dex.uniswap.v2.UniswapV2Pair;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcLog;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcReader;

/** Discovers validated Uniswap v2 pair descriptors from one configured factory. */
public final class UniswapV2MarketDiscovery extends AbstractUniswapMarketDiscovery<UniswapV2Pair> {

    private static final String PAIR_CREATED_TOPIC = UniswapFactoryEvents.V2_CREATED_TOPIC;
    private static final String SWAP_TOPIC =
            "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822";

    /**
     * Creates discovery with an explicit orientation rule and safe default limits.
     *
     * @param scan factory identity and initial cursor
     * @param reader typed reader for the configured network
     * @param orientation explicit address-backed logical orientation
     */
    public UniswapV2MarketDiscovery(
            UniswapFactoryScan scan,
            EvmRpcReader reader,
            UniswapPairOrientation orientation) {
        this(scan, reader, orientation, UniswapDiscoveryListener.ignoring());
    }

    /**
     * Creates bounded discovery with an immutable selection policy.
     *
     * @param scan factory identity and initial cursor
     * @param reader typed reader for the configured network
     * @param policy address-backed selection and resource policy
     */
    public UniswapV2MarketDiscovery(
            UniswapFactoryScan scan,
            EvmRpcReader reader,
            UniswapDiscoveryPolicy policy) {
        this(scan, reader, policy, UniswapDiscoveryListener.ignoring());
    }

    /**
     * Creates discovery with safe default limits and excluded-event diagnostics.
     *
     * @param scan factory identity and initial cursor
     * @param reader typed reader for the configured network
     * @param orientation explicit address-backed logical orientation
     * @param listener excluded-event listener
     */
    public UniswapV2MarketDiscovery(
            UniswapFactoryScan scan,
            EvmRpcReader reader,
            UniswapPairOrientation orientation,
            UniswapDiscoveryListener listener) {
        super(scan, reader, orientation, listener, PAIR_CREATED_TOPIC, SWAP_TOPIC);
    }

    /**
     * Creates bounded discovery with safe excluded-event diagnostics.
     *
     * @param scan factory identity and initial cursor
     * @param reader typed reader for the configured network
     * @param policy address-backed selection and resource policy
     * @param listener excluded-event listener
     */
    public UniswapV2MarketDiscovery(
            UniswapFactoryScan scan,
            EvmRpcReader reader,
            UniswapDiscoveryPolicy policy,
            UniswapDiscoveryListener listener) {
        super(scan, reader, policy, listener, PAIR_CREATED_TOPIC, SWAP_TOPIC);
    }

    @Override
    UniswapFactoryEvent decode(EvmRpcLog log) {
        return UniswapFactoryEvents.decodeV2(log);
    }

    @Override
    UniswapV2Pair create(
            UniswapFactoryEvent event,
            EvmToken token0,
            EvmToken token1,
            Instrument instrument) {
        return new UniswapV2Pair(
                scan().network(), event.marketAddress(), token0, token1, instrument);
    }
}
