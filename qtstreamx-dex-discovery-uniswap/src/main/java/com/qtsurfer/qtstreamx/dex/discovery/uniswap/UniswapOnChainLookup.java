package com.qtsurfer.qtstreamx.dex.discovery.uniswap;

import com.qtsurfer.qtstreamx.evm.rpc.EvmBlockTag;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogFilter;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcException;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcLog;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcReader;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

/**
 * Performs bounded, trust-neutral token and Uniswap V2/V3 contract lookup.
 *
 * <p>The lookup uses only typed EVM JSON-RPC reads. It does not authenticate
 * arbitrary token metadata, query explorer/indexer APIs, or infer logical
 * base/quote orientation.
 */
public final class UniswapOnChainLookup {

    private static final int MAX_V2_COUNTERPARTIES = 32;
    private static final int MAX_V3_COUNTERPARTIES = 16;
    private static final int MAX_V3_FEE_TIERS = 8;
    private static final int MAX_V3_COMBINATIONS = 64;
    private static final long MAX_FACTORY_SCAN_BLOCKS = 100_000;
    private static final int MAX_FACTORY_RESULTS = 1_000;
    private static final String ZERO_ADDRESS = "0x0000000000000000000000000000000000000000";

    private static final byte[] NAME_CALL = EvmAbi.call("06fdde03");
    private static final byte[] SYMBOL_CALL = EvmAbi.call("95d89b41");
    private static final byte[] DECIMALS_CALL = EvmAbi.call("313ce567");
    private static final byte[] FACTORY_CALL = EvmAbi.call("c45a0155");
    private static final byte[] TOKEN0_CALL = EvmAbi.call("0dfe1681");
    private static final byte[] TOKEN1_CALL = EvmAbi.call("d21220a7");
    private static final byte[] V2_RESERVES_CALL = EvmAbi.call("0902f1ac");
    private static final byte[] V3_FEE_CALL = EvmAbi.call("ddca3f43");
    private static final byte[] V3_LIQUIDITY_CALL = EvmAbi.call("1a686502");

    private final EvmRpcReader reader;

    /**
     * Creates lookup operations over one typed EVM reader.
     *
     * @param reader bounded reader configured for the requested network
     */
    public UniswapOnChainLookup(EvmRpcReader reader) {
        this.reader = Objects.requireNonNull(reader, "reader");
    }

    /**
     * Inspects bytecode and ERC-20 display metadata at one block state.
     *
     * <p>Individual metadata reverts or malformed return values are isolated in
     * {@link Erc20TokenInspection#unavailableFields()}. Transport exhaustion is
     * propagated so callers can distinguish provider failure from token shape.
     *
     * @param network stable EVM network identity
     * @param address token contract address
     * @param blockTag exact or named block state
     * @return immutable trust-neutral token inspection
     */
    public Erc20TokenInspection inspectToken(
            String network,
            String address,
            EvmBlockTag blockTag) {
        return inspectToken(network, normalizeAddress(address), blockTag, new LinkedHashMap<>());
    }

    /**
     * Reads the factory claimed by a V2 pair or V3 pool contract.
     *
     * <p>The returned address is trust-neutral. Callers must validate the
     * market back through that factory and compare it with a reviewed deployment
     * before presenting it as trusted.
     *
     * @param marketAddress pair or pool contract address
     * @param blockTag exact or named block state
     * @return normalized factory address claimed by the market
     * @throws UniswapLookupException if the address has no code or factory result is malformed
     */
    public String marketFactory(String marketAddress, EvmBlockTag blockTag) {
        String market = normalizeAddress(marketAddress);
        Objects.requireNonNull(blockTag, "blockTag");
        if (reader.code(market, blockTag).length == 0) {
            throw new UniswapLookupException(UniswapLookupException.Kind.MALFORMED_CONTRACT);
        }
        try {
            return EvmAbi.addressResult(reader.call(market, FACTORY_CALL, blockTag));
        } catch (IllegalArgumentException exception) {
            throw new UniswapLookupException(UniswapLookupException.Kind.MALFORMED_CONTRACT);
        }
    }

    /**
     * Lists canonical creation events over one explicit bounded factory range.
     *
     * <p>The result is intentionally trust-neutral and does not call token
     * contracts, orient instruments, or imply that a created market is active.
     *
     * @param deployment reviewed V2 or V3 factory deployment
     * @param fromBlock first block, inclusive
     * @param toBlock last block, inclusive
     * @param maximumResults positive result cap, at most 1,000
     * @return immutable markets in chain order with creation provenance
     * @throws UniswapLookupException if range, result cap, or decoded event exceeds bounds
     */
    public List<UniswapFactoryMarket> listFactoryMarkets(
            UniswapDeployment deployment,
            long fromBlock,
            long toBlock,
            int maximumResults) {
        Objects.requireNonNull(deployment, "deployment");
        if (fromBlock < 0 || toBlock < fromBlock
                || toBlock - fromBlock >= MAX_FACTORY_SCAN_BLOCKS
                || maximumResults <= 0
                || maximumResults > MAX_FACTORY_RESULTS) {
            throw new UniswapLookupException(UniswapLookupException.Kind.LIMIT);
        }
        String topic = deployment.version() == UniswapDeployment.Version.V2
                ? UniswapFactoryEvents.V2_CREATED_TOPIC
                : UniswapFactoryEvents.V3_CREATED_TOPIC;
        List<EvmRpcLog> logs = reader.logs(
                new EvmLogFilter(
                        Set.of(deployment.factoryScan().factoryAddress()),
                        Set.of(topic)),
                fromBlock,
                toBlock);
        if (logs.size() > maximumResults) {
            throw new UniswapLookupException(UniswapLookupException.Kind.LIMIT);
        }
        List<UniswapFactoryMarket> markets = new ArrayList<>(logs.size());
        for (EvmRpcLog log : logs) {
            if (log.removed()) {
                continue;
            }
            if (!deployment.factoryScan().factoryAddress().equalsIgnoreCase(log.address())) {
                throw new UniswapLookupException(UniswapLookupException.Kind.MALFORMED_EVENT);
            }
            try {
                UniswapFactoryEvent event = deployment.version() == UniswapDeployment.Version.V2
                        ? UniswapFactoryEvents.decodeV2(log)
                        : UniswapFactoryEvents.decodeV3(log);
                markets.add(new UniswapFactoryMarket(
                        deployment.version(),
                        deployment.network(),
                        deployment.factoryScan().factoryAddress(),
                        event.marketAddress(),
                        event.token0Address(),
                        event.token1Address(),
                        event.feeTier(),
                        log.blockNumber(),
                        log.transactionHash(),
                        log.logIndex()));
            } catch (IllegalArgumentException exception) {
                throw new UniswapLookupException(UniswapLookupException.Kind.MALFORMED_EVENT);
            }
        }
        return List.copyOf(markets);
    }

    /**
     * Finds validated V2 pairs for one token and explicit counterparties.
     *
     * @param deployment reviewed V2 factory deployment
     * @param tokenAddress token to search for
     * @param counterpartyAddresses explicit counterparties, bounded to 32
     * @param blockTag exact or named block state
     * @return immutable pairs ordered by address
     * @throws UniswapLookupException if version or lookup bounds are invalid
     */
    public List<UniswapV2PairInspection> findV2Pairs(
            UniswapDeployment deployment,
            String tokenAddress,
            Set<String> counterpartyAddresses,
            EvmBlockTag blockTag) {
        requireVersion(deployment, UniswapDeployment.Version.V2);
        List<String> counterparties = counterparties(
                tokenAddress, counterpartyAddresses, MAX_V2_COUNTERPARTIES);
        Map<String, Erc20TokenInspection> tokens = new LinkedHashMap<>();
        Map<String, UniswapV2PairInspection> pairs = new LinkedHashMap<>();
        for (String counterparty : counterparties) {
            String pair = v2PairAddress(deployment, tokenAddress, counterparty, blockTag);
            if (!ZERO_ADDRESS.equals(pair)) {
                pairs.putIfAbsent(pair, inspectV2Pair(deployment, pair, blockTag, tokens));
            }
        }
        return pairs.values().stream()
                .sorted(Comparator.comparing(UniswapV2PairInspection::address))
                .toList();
    }

    /**
     * Finds validated V3 pools for one token, explicit counterparties, and fee tiers.
     *
     * @param deployment reviewed V3 factory deployment
     * @param tokenAddress token to search for
     * @param counterpartyAddresses explicit counterparties, bounded to 16
     * @param feeTiers explicit fee tiers, bounded to 8 and 64 total combinations
     * @param blockTag exact or named block state
     * @return immutable pools ordered by address
     * @throws UniswapLookupException if version or lookup bounds are invalid
     */
    public List<UniswapV3PoolInspection> findV3Pools(
            UniswapDeployment deployment,
            String tokenAddress,
            Set<String> counterpartyAddresses,
            Set<Integer> feeTiers,
            EvmBlockTag blockTag) {
        requireVersion(deployment, UniswapDeployment.Version.V3);
        List<String> counterparties = counterparties(
                tokenAddress, counterpartyAddresses, MAX_V3_COUNTERPARTIES);
        List<Integer> fees = feeTiers(feeTiers);
        if (Math.multiplyExact(counterparties.size(), fees.size()) > MAX_V3_COMBINATIONS) {
            throw new UniswapLookupException(UniswapLookupException.Kind.LIMIT);
        }
        Map<String, Erc20TokenInspection> tokens = new LinkedHashMap<>();
        Map<String, UniswapV3PoolInspection> pools = new LinkedHashMap<>();
        for (String counterparty : counterparties) {
            for (int feeTier : fees) {
                String pool = v3PoolAddress(
                        deployment, tokenAddress, counterparty, feeTier, blockTag);
                if (!ZERO_ADDRESS.equals(pool)) {
                    pools.putIfAbsent(pool, inspectV3Pool(deployment, pool, blockTag, tokens));
                }
            }
        }
        return pools.values().stream()
                .sorted(Comparator.comparing(UniswapV3PoolInspection::address))
                .toList();
    }

    /**
     * Inspects a V2 pair and verifies that it belongs to the configured factory.
     *
     * @param deployment reviewed V2 factory deployment
     * @param pairAddress pair contract address
     * @param blockTag exact or named block state
     * @return validated pair identity, token metadata, and native reserves
     * @throws UniswapLookupException if version, ABI, or factory identity is invalid
     */
    public UniswapV2PairInspection inspectV2Pair(
            UniswapDeployment deployment,
            String pairAddress,
            EvmBlockTag blockTag) {
        requireVersion(deployment, UniswapDeployment.Version.V2);
        return inspectV2Pair(
                deployment,
                normalizeAddress(pairAddress),
                blockTag,
                new LinkedHashMap<>());
    }

    /**
     * Inspects a V3 pool and verifies that it belongs to the configured factory.
     *
     * @param deployment reviewed V3 factory deployment
     * @param poolAddress pool contract address
     * @param blockTag exact or named block state
     * @return validated pool identity, token metadata, fee, and native liquidity
     * @throws UniswapLookupException if version, ABI, or factory identity is invalid
     */
    public UniswapV3PoolInspection inspectV3Pool(
            UniswapDeployment deployment,
            String poolAddress,
            EvmBlockTag blockTag) {
        requireVersion(deployment, UniswapDeployment.Version.V3);
        return inspectV3Pool(
                deployment,
                normalizeAddress(poolAddress),
                blockTag,
                new LinkedHashMap<>());
    }

    private Erc20TokenInspection inspectToken(
            String network,
            String address,
            EvmBlockTag blockTag,
            Map<String, Erc20TokenInspection> cache) {
        Objects.requireNonNull(network, "network");
        if (network.isBlank()) {
            throw new IllegalArgumentException("network must not be blank");
        }
        Objects.requireNonNull(blockTag, "blockTag");
        return cache.computeIfAbsent(address, ignored -> readToken(network, address, blockTag));
    }

    private Erc20TokenInspection readToken(
            String network,
            String address,
            EvmBlockTag blockTag) {
        if (reader.code(address, blockTag).length == 0) {
            return new Erc20TokenInspection(
                    network,
                    address,
                    false,
                    Optional.empty(),
                    Optional.empty(),
                    OptionalInt.empty(),
                    EnumSet.allOf(Erc20TokenInspection.Field.class));
        }
        EnumSet<Erc20TokenInspection.Field> unavailable =
                EnumSet.noneOf(Erc20TokenInspection.Field.class);
        Optional<String> name = readText(address, NAME_CALL, blockTag,
                Erc20TokenInspection.Field.NAME, unavailable);
        Optional<String> symbol = readText(address, SYMBOL_CALL, blockTag,
                Erc20TokenInspection.Field.SYMBOL, unavailable);
        OptionalInt decimals = readDecimals(address, blockTag, unavailable);
        return new Erc20TokenInspection(
                network, address, true, name, symbol, decimals, unavailable);
    }

    private Optional<String> readText(
            String address,
            byte[] call,
            EvmBlockTag blockTag,
            Erc20TokenInspection.Field field,
            Set<Erc20TokenInspection.Field> unavailable) {
        try {
            return Optional.of(EvmAbi.textResult(reader.call(address, call, blockTag)));
        } catch (EvmRpcException | IllegalArgumentException exception) {
            unavailable.add(field);
            return Optional.empty();
        }
    }

    private OptionalInt readDecimals(
            String address,
            EvmBlockTag blockTag,
            Set<Erc20TokenInspection.Field> unavailable) {
        try {
            return OptionalInt.of(EvmAbi.uint8Result(reader.call(address, DECIMALS_CALL, blockTag)));
        } catch (EvmRpcException | IllegalArgumentException exception) {
            unavailable.add(Erc20TokenInspection.Field.DECIMALS);
            return OptionalInt.empty();
        }
    }

    private UniswapV2PairInspection inspectV2Pair(
            UniswapDeployment deployment,
            String pairAddress,
            EvmBlockTag blockTag,
            Map<String, Erc20TokenInspection> tokens) {
        try {
            String token0Address = EvmAbi.addressResult(reader.call(pairAddress, TOKEN0_CALL, blockTag));
            String token1Address = EvmAbi.addressResult(reader.call(pairAddress, TOKEN1_CALL, blockTag));
            String expected = v2PairAddress(deployment, token0Address, token1Address, blockTag);
            if (!pairAddress.equals(expected)) {
                throw new UniswapLookupException(UniswapLookupException.Kind.FACTORY_MISMATCH);
            }
            byte[] reserves = reader.call(pairAddress, V2_RESERVES_CALL, blockTag);
            return new UniswapV2PairInspection(
                    deployment.network(),
                    pairAddress,
                    inspectToken(deployment.network(), token0Address, blockTag, tokens),
                    inspectToken(deployment.network(), token1Address, blockTag, tokens),
                    EvmAbi.uintDataWord(reserves, 0, 3),
                    EvmAbi.uintDataWord(reserves, 1, 3));
        } catch (UniswapLookupException exception) {
            throw exception;
        } catch (IllegalArgumentException exception) {
            throw new UniswapLookupException(UniswapLookupException.Kind.MALFORMED_CONTRACT);
        }
    }

    private UniswapV3PoolInspection inspectV3Pool(
            UniswapDeployment deployment,
            String poolAddress,
            EvmBlockTag blockTag,
            Map<String, Erc20TokenInspection> tokens) {
        try {
            String token0Address = EvmAbi.addressResult(reader.call(poolAddress, TOKEN0_CALL, blockTag));
            String token1Address = EvmAbi.addressResult(reader.call(poolAddress, TOKEN1_CALL, blockTag));
            int feeTier = EvmAbi.uint24Result(reader.call(poolAddress, V3_FEE_CALL, blockTag));
            String expected = v3PoolAddress(
                    deployment, token0Address, token1Address, feeTier, blockTag);
            if (!poolAddress.equals(expected)) {
                throw new UniswapLookupException(UniswapLookupException.Kind.FACTORY_MISMATCH);
            }
            BigInteger liquidity = EvmAbi.uintResult(
                    reader.call(poolAddress, V3_LIQUIDITY_CALL, blockTag));
            return new UniswapV3PoolInspection(
                    deployment.network(),
                    poolAddress,
                    inspectToken(deployment.network(), token0Address, blockTag, tokens),
                    inspectToken(deployment.network(), token1Address, blockTag, tokens),
                    feeTier,
                    liquidity);
        } catch (UniswapLookupException exception) {
            throw exception;
        } catch (IllegalArgumentException exception) {
            throw new UniswapLookupException(UniswapLookupException.Kind.MALFORMED_CONTRACT);
        }
    }

    private String v2PairAddress(
            UniswapDeployment deployment,
            String tokenAddress,
            String counterpartyAddress,
            EvmBlockTag blockTag) {
        byte[] call = EvmAbi.call(
                "e6a43905",
                EvmAbi.addressArgument(tokenAddress),
                EvmAbi.addressArgument(counterpartyAddress));
        return EvmAbi.addressResult(reader.call(
                deployment.factoryScan().factoryAddress(), call, blockTag));
    }

    private String v3PoolAddress(
            UniswapDeployment deployment,
            String tokenAddress,
            String counterpartyAddress,
            int feeTier,
            EvmBlockTag blockTag) {
        byte[] call = EvmAbi.call(
                "1698ee82",
                EvmAbi.addressArgument(tokenAddress),
                EvmAbi.addressArgument(counterpartyAddress),
                EvmAbi.uintArgument(feeTier));
        return EvmAbi.addressResult(reader.call(
                deployment.factoryScan().factoryAddress(), call, blockTag));
    }

    private static void requireVersion(
            UniswapDeployment deployment,
            UniswapDeployment.Version expected) {
        Objects.requireNonNull(deployment, "deployment");
        if (deployment.version() != expected) {
            throw new UniswapLookupException(UniswapLookupException.Kind.VERSION);
        }
    }

    private static List<String> counterparties(
            String tokenAddress,
            Set<String> counterpartyAddresses,
            int maximum) {
        String token = normalizeAddress(tokenAddress);
        Objects.requireNonNull(counterpartyAddresses, "counterpartyAddresses");
        if (counterpartyAddresses.isEmpty() || counterpartyAddresses.size() > maximum) {
            throw new UniswapLookupException(UniswapLookupException.Kind.LIMIT);
        }
        List<String> result = new ArrayList<>();
        for (String counterpartyAddress : counterpartyAddresses) {
            String counterparty = normalizeAddress(counterpartyAddress);
            if (token.equals(counterparty)) {
                throw new IllegalArgumentException("token and counterparty must be different");
            }
            result.add(counterparty);
        }
        return result.stream().distinct().sorted().toList();
    }

    private static List<Integer> feeTiers(Set<Integer> feeTiers) {
        Objects.requireNonNull(feeTiers, "feeTiers");
        if (feeTiers.isEmpty() || feeTiers.size() > MAX_V3_FEE_TIERS) {
            throw new UniswapLookupException(UniswapLookupException.Kind.LIMIT);
        }
        feeTiers.forEach(feeTier -> {
            if (feeTier == null || feeTier <= 0 || feeTier >= 1_000_000) {
                throw new IllegalArgumentException("feeTier must be between 1 and 999999");
            }
        });
        return feeTiers.stream().sorted().toList();
    }

    private static String normalizeAddress(String value) {
        if (value == null || !value.matches("0x[0-9a-fA-F]{40}")) {
            throw new IllegalArgumentException("address must be a 20-byte hex value");
        }
        return value.toLowerCase(Locale.ROOT);
    }
}
