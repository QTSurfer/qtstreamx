package com.qtsurfer.qtstreamx.dex.discovery.cli;

import com.qtsurfer.qtstreamx.dex.discovery.uniswap.KnownUniswapDeployments;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapDeployment;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapNetworkTokenPolicy;
import com.qtsurfer.qtstreamx.dex.uniswap.v2.KnownUniswapV2Markets;
import com.qtsurfer.qtstreamx.dex.uniswap.v2.UniswapV2Pair;
import com.qtsurfer.qtstreamx.dex.uniswap.v3.KnownUniswapV3Markets;
import com.qtsurfer.qtstreamx.dex.uniswap.v3.UniswapV3Pool;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

/** Reviewed network and example-market catalogue used only for CLI navigation. */
final class UniswapCliCatalog {
    private static final List<CliData.Network> NETWORKS = List.of(
            new CliData.Network("ethereum", "Ethereum mainnet", "eip155:1"),
            new CliData.Network("robinhood", "Robinhood Chain mainnet", "eip155:4663"));

    private UniswapCliCatalog() {}

    static List<CliData.Network> networks() {
        return NETWORKS;
    }

    static String resolveNetwork(String value) {
        String normalized = value.toLowerCase(Locale.ROOT);
        return NETWORKS.stream()
                .filter(network -> network.alias().equals(normalized)
                        || network.network().equals(normalized))
                .map(CliData.Network::network)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("unsupported network: " + value));
    }

    static List<UniswapDeployment> deployments(String network) {
        return KnownUniswapDeployments.all().stream()
                .filter(deployment -> deployment.network().equals(network))
                .toList();
    }

    static UniswapNetworkTokenPolicy tokenPolicy(String network) {
        return deployments(network).stream()
                .findFirst()
                .map(UniswapDeployment::tokenPolicy)
                .orElseThrow(() -> new IllegalArgumentException(
                        "network has no reviewed token policy"));
    }

    static List<CliData.Market> markets(String network) {
        List<CliData.Market> v2 = KnownUniswapV2Markets.all().stream()
                .filter(pair -> pair.network().equals(network))
                .map(UniswapCliCatalog::market)
                .toList();
        List<CliData.Market> v3 = KnownUniswapV3Markets.all().stream()
                .filter(pool -> pool.network().equals(network))
                .map(UniswapCliCatalog::market)
                .toList();
        return Stream.concat(v2.stream(), v3.stream()).toList();
    }

    static boolean reviewed(UniswapDeployment deployment) {
        return KnownUniswapDeployments.all().stream().anyMatch(candidate ->
                candidate.version() == deployment.version()
                        && candidate.network().equals(deployment.network())
                        && candidate.factoryScan().factoryAddress()
                                .equals(deployment.factoryScan().factoryAddress()));
    }

    private static CliData.Market market(UniswapV2Pair pair) {
        String factory = factory(pair.network(), UniswapDeployment.Version.V2);
        return new CliData.Market(
                "v2",
                pair.network(),
                factory,
                pair.address(),
                pair.instrument().symbol(),
                pair.token0().address(),
                pair.token1().address(),
                null,
                true);
    }

    private static CliData.Market market(UniswapV3Pool pool) {
        String factory = factory(pool.network(), UniswapDeployment.Version.V3);
        return new CliData.Market(
                "v3",
                pool.network(),
                factory,
                pool.address(),
                pool.instrument().symbol(),
                pool.token0().address(),
                pool.token1().address(),
                pool.feeTier(),
                true);
    }

    private static String factory(String network, UniswapDeployment.Version version) {
        return deployments(network).stream()
                .filter(deployment -> deployment.version() == version)
                .findFirst()
                .orElseThrow()
                .factoryScan().factoryAddress();
    }
}
