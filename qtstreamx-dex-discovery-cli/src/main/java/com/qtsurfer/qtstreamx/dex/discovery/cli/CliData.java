package com.qtsurfer.qtstreamx.dex.discovery.cli;

import java.util.List;

/** Internal stable output vocabulary shared by human and JSON renderers. */
final class CliData {

    private CliData() {}

    record Protocol(String alias, String label, List<String> versions) {}

    record Network(String alias, String label, String network) {}

    record Token(
            String network,
            String address,
            boolean contract,
            String name,
            String symbol,
            Integer decimals,
            List<String> unavailableFields) {}

    record Market(
            String version,
            String network,
            String factoryAddress,
            String address,
            String instrument,
            String token0Address,
            String token1Address,
            Integer feeTier,
            boolean reviewed) {}

    record Pool(
            String version,
            String network,
            String factoryAddress,
            String address,
            Token token0,
            Token token1,
            String instrument,
            String orientation,
            Integer feeTier,
            String reserve0,
            String reserve1,
            String currentLiquidity,
            boolean reviewed) {}

    record FactoryMarket(
            String version,
            String network,
            String factoryAddress,
            String address,
            String token0Address,
            String token1Address,
            Integer feeTier,
            long blockNumber,
            String transactionHash,
            int logIndex,
            boolean reviewed) {}

    record TokenLookup(Token token, List<Pool> pools) {}

    record SearchMatch(Token token, FactoryMarket market) {}
}
