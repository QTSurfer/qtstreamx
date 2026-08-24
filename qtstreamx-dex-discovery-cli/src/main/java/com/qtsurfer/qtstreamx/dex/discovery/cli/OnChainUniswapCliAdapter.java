package com.qtsurfer.qtstreamx.dex.discovery.cli;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.dex.core.EvmToken;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.Erc20TokenInspection;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapDeployment;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapFactoryMarket;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapFactoryScan;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapLookupException;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapOnChainLookup;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapV2PairInspection;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapV3PoolInspection;
import com.qtsurfer.qtstreamx.evm.rpc.EvmBlockTag;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Uniswap adapter composed exclusively from public typed discovery APIs. */
final class OnChainUniswapCliAdapter implements CliProtocolAdapter {
    private static final Set<Integer> STANDARD_V3_FEES = Set.of(100, 500, 3_000, 10_000);

    private final String network;
    private final UniswapOnChainLookup lookup;

    OnChainUniswapCliAdapter(String network, UniswapOnChainLookup lookup) {
        this.network = network;
        this.lookup = lookup;
    }

    @Override
    public CliResponse execute(CliRequest request) {
        try {
            return switch (request.command()) {
                case TOKEN -> token(request);
                case POOL -> pool(request);
                case SCAN -> scan(request);
                case SEARCH -> search(request);
                default -> throw new IllegalArgumentException(
                        "command does not use the Uniswap adapter");
            };
        } catch (UniswapLookupException exception) {
            throw new CliLookupException(exception.kind().name());
        }
    }

    private CliResponse token(CliRequest request) {
        EvmBlockTag blockTag = blockTag(request);
        String address = request.requiredAddressArgument();
        Erc20TokenInspection token = lookup.inspectToken(network, address, blockTag);
        List<CliData.Pool> pools = new ArrayList<>();
        for (UniswapDeployment deployment : selectedDeployments(request)) {
            Set<String> counterparties = new LinkedHashSet<>();
            counterparties.addAll(deployment.tokenPolicy().baseTokenAddresses());
            counterparties.addAll(deployment.tokenPolicy().quoteTokenAddresses());
            counterparties.removeIf(candidate -> candidate.equalsIgnoreCase(address));
            if (counterparties.isEmpty()) {
                continue;
            }
            if (deployment.version() == UniswapDeployment.Version.V2) {
                lookup.findV2Pairs(deployment, address, counterparties, blockTag).stream()
                        .map(pair -> pool(deployment, pair))
                        .forEach(pools::add);
            } else {
                lookup.findV3Pools(
                                deployment,
                                address,
                                counterparties,
                                STANDARD_V3_FEES,
                                blockTag)
                        .stream()
                        .map(pool -> pool(deployment, pool))
                        .forEach(pools::add);
            }
        }
        CliData.TokenLookup data = new CliData.TokenLookup(token(token), List.copyOf(pools));
        return pools.isEmpty()
                ? CliResponse.noSupportedMarket(request, data)
                : CliResponse.ok(request, data);
    }

    private CliResponse pool(CliRequest request) {
        EvmBlockTag blockTag = blockTag(request);
        String address = request.requiredAddressArgument();
        List<UniswapDeployment> deployments;
        if (request.option("factory").isPresent()) {
            deployments = List.of(deployment(request));
        } else if (request.option("version").isPresent()) {
            deployments = selectedDeployments(request);
        } else {
            String factory = lookup.marketFactory(address, blockTag);
            deployments = List.of(
                    deployment(UniswapDeployment.Version.V2, factory, 0),
                    deployment(UniswapDeployment.Version.V3, factory, 0));
        }
        List<CliData.Pool> matches = new ArrayList<>();
        for (int index = 0; index < deployments.size(); index++) {
            UniswapDeployment deployment = deployments.get(index);
            try {
                if (deployment.version() == UniswapDeployment.Version.V2) {
                    matches.add(pool(deployment, lookup.inspectV2Pair(deployment, address, blockTag)));
                } else {
                    matches.add(pool(deployment, lookup.inspectV3Pool(deployment, address, blockTag)));
                }
                break;
            } catch (EvmRpcException | UniswapLookupException exception) {
                boolean hasFallbackProbe = request.option("factory").isEmpty()
                        && request.option("version").isEmpty()
                        && index + 1 < deployments.size();
                if (!hasFallbackProbe) {
                    throw exception;
                }
            }
        }
        return matches.isEmpty()
                ? CliResponse.noSupportedMarket(request, List.of())
                : CliResponse.ok(request, List.copyOf(matches));
    }

    private CliResponse scan(CliRequest request) {
        UniswapDeployment deployment = deployment(request);
        List<CliData.FactoryMarket> markets = lookup.listFactoryMarkets(
                        deployment,
                        request.requiredLongOption("from"),
                        request.requiredLongOption("to"),
                        resultLimit(request))
                .stream()
                .map(market -> market(deployment, market))
                .toList();
        return CliResponse.ok(request, markets);
    }

    private CliResponse search(CliRequest request) {
        String query = request.requiredOption("query").toLowerCase(Locale.ROOT);
        if (query.isBlank()) {
            throw new IllegalArgumentException("--query must not be blank");
        }
        UniswapDeployment deployment = deployment(request);
        EvmBlockTag blockTag = blockTag(request);
        List<UniswapFactoryMarket> markets = lookup.listFactoryMarkets(
                deployment,
                request.requiredLongOption("from"),
                request.requiredLongOption("to"),
                resultLimit(request));
        Set<String> tokenAddresses = new LinkedHashSet<>();
        markets.forEach(market -> {
            tokenAddresses.add(market.token0Address());
            tokenAddresses.add(market.token1Address());
        });
        if (tokenAddresses.size() > tokenLimit(request)) {
            throw new UniswapLookupException(UniswapLookupException.Kind.LIMIT);
        }
        Map<String, Erc20TokenInspection> tokens = new LinkedHashMap<>();
        for (String address : tokenAddresses) {
            tokens.put(address, lookup.inspectToken(network, address, blockTag));
        }
        List<CliData.SearchMatch> matches = new ArrayList<>();
        for (UniswapFactoryMarket market : markets) {
            addSearchMatch(matches, deployment, market, tokens.get(market.token0Address()), query);
            addSearchMatch(matches, deployment, market, tokens.get(market.token1Address()), query);
        }
        return matches.isEmpty()
                ? CliResponse.noSupportedMarket(request, List.of())
                : CliResponse.ok(request, List.copyOf(matches));
    }

    private void addSearchMatch(
            List<CliData.SearchMatch> matches,
            UniswapDeployment deployment,
            UniswapFactoryMarket market,
            Erc20TokenInspection token,
            String query) {
        boolean matchesName = token.name()
                .map(value -> value.toLowerCase(Locale.ROOT).contains(query))
                .orElse(false);
        boolean matchesSymbol = token.symbol()
                .map(value -> value.toLowerCase(Locale.ROOT).contains(query))
                .orElse(false);
        if (matchesName || matchesSymbol) {
            matches.add(new CliData.SearchMatch(token(token), market(deployment, market)));
        }
    }

    private List<UniswapDeployment> selectedDeployments(CliRequest request) {
        if (request.option("factory").isPresent()) {
            return List.of(deployment(request));
        }
        List<UniswapDeployment> deployments = UniswapCliCatalog.deployments(network);
        if (request.option("version").isEmpty()) {
            return deployments;
        }
        UniswapDeployment.Version version = version(request.requiredOption("version"));
        List<UniswapDeployment> selected = deployments.stream()
                .filter(deployment -> deployment.version() == version)
                .toList();
        if (selected.isEmpty()) {
            throw new IllegalArgumentException(
                    "network has no reviewed " + version.name().toLowerCase(Locale.ROOT)
                            + " deployment; provide --factory for an unreviewed lookup");
        }
        return selected;
    }

    private UniswapDeployment deployment(CliRequest request) {
        UniswapDeployment.Version version = version(request.requiredOption("version"));
        return request.option("factory")
                .map(factory -> deployment(
                        version,
                        factory,
                        request.intOption("factory-start", 0)))
                .orElseGet(() -> selectedDeployment(version));
    }

    private UniswapDeployment selectedDeployment(UniswapDeployment.Version version) {
        List<UniswapDeployment> matches = UniswapCliCatalog.deployments(network).stream()
                .filter(deployment -> deployment.version() == version)
                .toList();
        if (matches.size() != 1) {
            throw new IllegalArgumentException(
                    "network requires an explicit --factory for this version");
        }
        return matches.getFirst();
    }

    private UniswapDeployment deployment(
            UniswapDeployment.Version version,
            String factory,
            long startBlock) {
        return new UniswapDeployment(
                version,
                new UniswapFactoryScan(network, factory, startBlock),
                UniswapCliCatalog.tokenPolicy(network));
    }

    private static UniswapDeployment.Version version(String value) {
        return switch (value.toLowerCase(Locale.ROOT)) {
            case "v2" -> UniswapDeployment.Version.V2;
            case "v3" -> UniswapDeployment.Version.V3;
            default -> throw new IllegalArgumentException("--version must be v2 or v3");
        };
    }

    private static int resultLimit(CliRequest request) {
        int limit = request.intOption("limit", 100);
        if (limit <= 0 || limit > 1_000) {
            throw new IllegalArgumentException("--limit must be between 1 and 1000");
        }
        return limit;
    }

    private static int tokenLimit(CliRequest request) {
        int limit = request.intOption("token-limit", 200);
        if (limit <= 0 || limit > 2_000) {
            throw new IllegalArgumentException("--token-limit must be between 1 and 2000");
        }
        return limit;
    }

    private static EvmBlockTag blockTag(CliRequest request) {
        return request.option("block").map(value -> switch (value.toLowerCase(Locale.ROOT)) {
            case "latest" -> EvmBlockTag.latest();
            case "safe" -> EvmBlockTag.safe();
            case "finalized" -> EvmBlockTag.finalized();
            default -> {
                try {
                    yield EvmBlockTag.number(Long.parseLong(value));
                } catch (NumberFormatException exception) {
                    throw new IllegalArgumentException(
                            "--block must be latest, safe, finalized, or an integer");
                }
            }
        }).orElse(EvmBlockTag.latest());
    }

    private static CliData.Token token(Erc20TokenInspection token) {
        return new CliData.Token(
                token.network(),
                token.address(),
                token.contract(),
                token.name().orElse(null),
                token.symbol().orElse(null),
                token.decimals().isPresent() ? token.decimals().getAsInt() : null,
                token.unavailableFields().stream().map(Enum::name).sorted().toList());
    }

    private static CliData.Pool pool(
            UniswapDeployment deployment,
            UniswapV2PairInspection pair) {
        Optional<Instrument> instrument = orient(deployment, pair.token0(), pair.token1());
        return new CliData.Pool(
                "v2",
                pair.network(),
                deployment.factoryScan().factoryAddress(),
                pair.address(),
                token(pair.token0()),
                token(pair.token1()),
                instrument.map(Instrument::symbol).orElse(null),
                instrument.isPresent() ? "address_policy" : "unavailable",
                null,
                pair.reserve0().toString(),
                pair.reserve1().toString(),
                null,
                UniswapCliCatalog.reviewed(deployment));
    }

    private static CliData.Pool pool(
            UniswapDeployment deployment,
            UniswapV3PoolInspection pool) {
        Optional<Instrument> instrument = orient(deployment, pool.token0(), pool.token1());
        return new CliData.Pool(
                "v3",
                pool.network(),
                deployment.factoryScan().factoryAddress(),
                pool.address(),
                token(pool.token0()),
                token(pool.token1()),
                instrument.map(Instrument::symbol).orElse(null),
                instrument.isPresent() ? "address_policy" : "unavailable",
                pool.feeTier(),
                null,
                null,
                pool.currentLiquidity().toString(),
                UniswapCliCatalog.reviewed(deployment));
    }

    private static Optional<Instrument> orient(
            UniswapDeployment deployment,
            Erc20TokenInspection token0,
            Erc20TokenInspection token1) {
        Optional<EvmToken> first = evmToken(token0);
        Optional<EvmToken> second = evmToken(token1);
        if (first.isEmpty() || second.isEmpty()) {
            return Optional.empty();
        }
        return deployment.orientation()
                .orient(deployment.network(), first.orElseThrow(), second.orElseThrow());
    }

    private static Optional<EvmToken> evmToken(Erc20TokenInspection token) {
        if (token.symbol().isEmpty() || token.decimals().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new EvmToken(
                token.symbol().orElseThrow(),
                token.address(),
                token.decimals().orElseThrow()));
    }

    private static CliData.FactoryMarket market(
            UniswapDeployment deployment,
            UniswapFactoryMarket market) {
        return new CliData.FactoryMarket(
                market.version().name().toLowerCase(Locale.ROOT),
                market.network(),
                market.factoryAddress(),
                market.marketAddress(),
                market.token0Address(),
                market.token1Address(),
                market.version() == UniswapDeployment.Version.V3 ? market.feeTier() : null,
                market.blockNumber(),
                market.transactionHash(),
                market.logIndex(),
                UniswapCliCatalog.reviewed(deployment));
    }
}
