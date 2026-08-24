package com.qtsurfer.qtstreamx.dex.discovery.uniswap;

record UniswapFactoryEvent(
        String token0Address,
        String token1Address,
        String marketAddress,
        int feeTier
) {}
