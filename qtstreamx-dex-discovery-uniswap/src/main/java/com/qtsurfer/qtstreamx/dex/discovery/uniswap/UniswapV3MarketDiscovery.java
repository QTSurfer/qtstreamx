package com.qtsurfer.qtstreamx.dex.discovery.uniswap;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.dex.core.EvmToken;
import com.qtsurfer.qtstreamx.dex.uniswap.v3.UniswapV3Pool;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcLog;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcReader;

/** Discovers validated Uniswap v3 pool descriptors from one configured factory. */
public final class UniswapV3MarketDiscovery extends AbstractUniswapMarketDiscovery<UniswapV3Pool> {

    private static final String POOL_CREATED_TOPIC = UniswapFactoryEvents.V3_CREATED_TOPIC;
    private static final String SWAP_TOPIC =
            "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";

    /**
     * Creates discovery with an explicit orientation rule and safe default limits.
     *
     * @param scan factory identity and initial cursor
     * @param reader typed reader for the configured network
     * @param orientation explicit address-backed logical orientation
     */
    public UniswapV3MarketDiscovery(
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
    public UniswapV3MarketDiscovery(
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
    public UniswapV3MarketDiscovery(
            UniswapFactoryScan scan,
            EvmRpcReader reader,
            UniswapPairOrientation orientation,
            UniswapDiscoveryListener listener) {
        super(scan, reader, orientation, listener, POOL_CREATED_TOPIC, SWAP_TOPIC);
    }

    /**
     * Creates bounded discovery with safe excluded-event diagnostics.
     *
     * @param scan factory identity and initial cursor
     * @param reader typed reader for the configured network
     * @param policy address-backed selection and resource policy
     * @param listener excluded-event listener
     */
    public UniswapV3MarketDiscovery(
            UniswapFactoryScan scan,
            EvmRpcReader reader,
            UniswapDiscoveryPolicy policy,
            UniswapDiscoveryListener listener) {
        super(scan, reader, policy, listener, POOL_CREATED_TOPIC, SWAP_TOPIC);
    }

    @Override
    UniswapFactoryEvent decode(EvmRpcLog log) {
        return UniswapFactoryEvents.decodeV3(log);
    }

    @Override
    UniswapV3Pool create(
            UniswapFactoryEvent event,
            EvmToken token0,
            EvmToken token1,
            Instrument instrument) {
        return new UniswapV3Pool(
                scan().network(),
                event.marketAddress(),
                token0,
                token1,
                instrument,
                event.feeTier());
    }
}
