package com.qtsurfer.qtstreamx.dex.discovery.uniswap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.qtsurfer.qtstreamx.evm.rpc.EvmBlockTag;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogFilter;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcLog;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcReader;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class UniswapOnChainLookupTest {

    private static final String WETH = "0x0bd7d308f8e1639fab988df18a8011f41eacad73";
    private static final String RUBY = "0xc0a9531cae8bea6268bd19efec1dd205830cae2a";
    private static final String V3_POOL = "0x1111111111111111111111111111111111111111";
    private static final String V2_PAIR = "0x2222222222222222222222222222222222222222";
    private static final String NO_CODE = "0x3333333333333333333333333333333333333333";

    @Test
    void inspectsTokenOnChainAndIsolatesMalformedFields() {
        LookupReader reader = new LookupReader(false);
        UniswapOnChainLookup lookup = new UniswapOnChainLookup(reader);

        Erc20TokenInspection token = lookup.inspectToken(
                "eip155:4663", RUBY, EvmBlockTag.latest());

        assertThat(token.contract()).isTrue();
        assertThat(token.name()).isEmpty();
        assertThat(token.symbol()).contains("RUBY");
        assertThat(token.decimals()).hasValue(9);
        assertThat(token.unavailableFields()).containsExactly(Erc20TokenInspection.Field.NAME);
        assertThat(token.token()).get().extracting(value -> value.symbol()).isEqualTo("RUBY");
    }

    @Test
    void reportsNoContractWithoutIssuingMetadataCalls() {
        LookupReader reader = new LookupReader(false);
        UniswapOnChainLookup lookup = new UniswapOnChainLookup(reader);

        Erc20TokenInspection token = lookup.inspectToken(
                "eip155:4663", NO_CODE, EvmBlockTag.safe());

        assertThat(token.contract()).isFalse();
        assertThat(token.unavailableFields())
                .containsExactlyInAnyOrder(Erc20TokenInspection.Field.values());
        assertThat(reader.calls).noneMatch(call -> call.startsWith(NO_CODE + ":"));
    }

    @Test
    void findsAndValidatesV3PoolWithFixedByteSymbol() {
        LookupReader reader = new LookupReader(false);
        UniswapOnChainLookup lookup = new UniswapOnChainLookup(reader);

        List<UniswapV3PoolInspection> pools = lookup.findV3Pools(
                KnownUniswapDeployments.ROBINHOOD_MAINNET_V3,
                RUBY,
                Set.of(WETH),
                Set.of(10_000),
                EvmBlockTag.latest());

        assertThat(pools).singleElement().satisfies(pool -> {
            assertThat(pool.address()).isEqualTo(V3_POOL);
            assertThat(pool.token0().address()).isEqualTo(WETH);
            assertThat(pool.token1().symbol()).contains("RUBY");
            assertThat(pool.feeTier()).isEqualTo(10_000);
            assertThat(pool.currentLiquidity()).isEqualTo(BigInteger.valueOf(42));
        });
        assertThat(reader.calls.stream().filter(call -> call.contains(":95d89b41:")))
                .hasSize(2);
    }

    @Test
    void listsTrustNeutralFactoryEventsWithProvenance() {
        UniswapOnChainLookup lookup = new UniswapOnChainLookup(new LookupReader(false));

        List<UniswapFactoryMarket> markets = lookup.listFactoryMarkets(
                KnownUniswapDeployments.ROBINHOOD_MAINNET_V3,
                100,
                100,
                1);

        assertThat(markets).singleElement().satisfies(market -> {
            assertThat(market.version()).isEqualTo(UniswapDeployment.Version.V3);
            assertThat(market.marketAddress()).isEqualTo(V3_POOL);
            assertThat(market.token0Address()).isEqualTo(WETH);
            assertThat(market.token1Address()).isEqualTo(RUBY);
            assertThat(market.feeTier()).isEqualTo(10_000);
            assertThat(market.blockNumber()).isEqualTo(100);
            assertThat(market.transactionHash()).isEqualTo("0xfactory-tx");
        });
    }

    @Test
    void findsAndValidatesV2PairWithNativeReserves() {
        LookupReader reader = new LookupReader(false);
        UniswapOnChainLookup lookup = new UniswapOnChainLookup(reader);

        List<UniswapV2PairInspection> pairs = lookup.findV2Pairs(
                KnownUniswapDeployments.ETHEREUM_MAINNET_V2,
                RUBY,
                Set.of(WETH),
                EvmBlockTag.number(123));

        assertThat(pairs).singleElement().satisfies(pair -> {
            assertThat(pair.address()).isEqualTo(V2_PAIR);
            assertThat(pair.reserve0()).isEqualTo(BigInteger.valueOf(7));
            assertThat(pair.reserve1()).isEqualTo(BigInteger.valueOf(11));
        });
    }

    @Test
    void readsTrustNeutralFactoryClaimForProtocolInference() {
        UniswapOnChainLookup lookup = new UniswapOnChainLookup(new LookupReader(false));

        String factory = lookup.marketFactory(V2_PAIR, EvmBlockTag.latest());

        assertThat(factory).isEqualTo(
                KnownUniswapDeployments.ETHEREUM_MAINNET_V2.factoryScan().factoryAddress());
    }

    @Test
    void rejectsPoolThatDoesNotResolveBackToReviewedFactory() {
        UniswapOnChainLookup lookup = new UniswapOnChainLookup(new LookupReader(true));

        assertThatThrownBy(() -> lookup.inspectV3Pool(
                        KnownUniswapDeployments.ROBINHOOD_MAINNET_V3,
                        V3_POOL,
                        EvmBlockTag.latest()))
                .isInstanceOf(UniswapLookupException.class)
                .extracting("kind")
                .isEqualTo(UniswapLookupException.Kind.FACTORY_MISMATCH);
    }

    @Test
    void failsClosedBeforeUnsafeV3FanOut() {
        Set<String> counterparties = IntStream.rangeClosed(1, 9)
                .mapToObj(value -> "0x%040x".formatted(value))
                .collect(Collectors.toSet());
        UniswapOnChainLookup lookup = new UniswapOnChainLookup(new LookupReader(false));

        assertThatThrownBy(() -> lookup.findV3Pools(
                        KnownUniswapDeployments.ROBINHOOD_MAINNET_V3,
                        RUBY,
                        counterparties,
                        Set.of(100, 500, 3_000, 10_000, 20_000, 30_000, 40_000, 50_000),
                        EvmBlockTag.latest()))
                .isInstanceOf(UniswapLookupException.class)
                .extracting("kind")
                .isEqualTo(UniswapLookupException.Kind.LIMIT);
    }

    private static final class LookupReader implements EvmRpcReader {
        private final boolean mismatch;
        private final List<String> calls = new ArrayList<>();

        private LookupReader(boolean mismatch) {
            this.mismatch = mismatch;
        }

        @Override
        public long latestBlockNumber() {
            return 123;
        }

        @Override
        public List<EvmRpcLog> logs(EvmLogFilter filter, long fromBlock, long toBlock) {
            return List.of(new EvmRpcLog(
                    KnownUniswapDeployments.ROBINHOOD_MAINNET_V3
                            .factoryScan().factoryAddress(),
                    List.of(
                            UniswapFactoryEvents.V3_CREATED_TOPIC,
                            topic(WETH),
                            topic(RUBY),
                            "0x" + "%064x".formatted(10_000)),
                    "0x" + "%064x".formatted(200)
                            + "0".repeat(24) + V3_POOL.substring(2),
                    100,
                    "0xblock",
                    "0xfactory-tx",
                    0,
                    0,
                    false));
        }

        @Override
        public byte[] code(String contractAddress, EvmBlockTag blockTag) {
            return contractAddress.equals(NO_CODE) ? new byte[0] : new byte[] {1};
        }

        @Override
        public byte[] call(String contractAddress, byte[] data, EvmBlockTag blockTag) {
            String selector = HexFormat.of().formatHex(Arrays.copyOf(data, 4));
            calls.add(contractAddress + ":" + selector + ":" + blockTag);
            if (contractAddress.equals(
                    KnownUniswapDeployments.ROBINHOOD_MAINNET_V3.factoryScan().factoryAddress())) {
                return address(mismatch ? V2_PAIR : V3_POOL);
            }
            if (contractAddress.equals(
                    KnownUniswapDeployments.ETHEREUM_MAINNET_V2.factoryScan().factoryAddress())) {
                return address(V2_PAIR);
            }
            if (contractAddress.equals(V3_POOL)) {
                return switch (selector) {
                    case "0dfe1681" -> address(WETH);
                    case "d21220a7" -> address(RUBY);
                    case "ddca3f43" -> uint(10_000);
                    case "1a686502" -> uint(42);
                    default -> throw new AssertionError("unexpected pool selector " + selector);
                };
            }
            if (contractAddress.equals(V2_PAIR)) {
                return switch (selector) {
                    case "c45a0155" -> address(KnownUniswapDeployments.ETHEREUM_MAINNET_V2
                            .factoryScan().factoryAddress());
                    case "0dfe1681" -> address(WETH);
                    case "d21220a7" -> address(RUBY);
                    case "0902f1ac" -> words(7, 11, 123);
                    default -> throw new AssertionError("unexpected pair selector " + selector);
                };
            }
            if (contractAddress.equals(WETH)) {
                return metadata(selector, "Wrapped Ether", "WETH", 18, false);
            }
            if (contractAddress.equals(RUBY)) {
                return metadata(selector, "The Reddit Dog", "RUBY", 9, true);
            }
            throw new AssertionError("unexpected contract " + contractAddress);
        }
    }

    private static byte[] metadata(
            String selector,
            String name,
            String symbol,
            int decimals,
            boolean malformedName) {
        return switch (selector) {
            case "06fdde03" -> malformedName ? new byte[] {1} : dynamicText(name);
            case "95d89b41" -> fixedText(symbol);
            case "313ce567" -> uint(decimals);
            default -> throw new AssertionError("unexpected token selector " + selector);
        };
    }

    private static byte[] address(String address) {
        return HexFormat.of().parseHex("00".repeat(12) + address.substring(2));
    }

    private static String topic(String address) {
        return "0x" + "0".repeat(24) + address.substring(2);
    }

    private static byte[] uint(long value) {
        return HexFormat.of().parseHex("%064x".formatted(value));
    }

    private static byte[] words(long... values) {
        StringBuilder encoded = new StringBuilder();
        for (long value : values) {
            encoded.append("%064x".formatted(value));
        }
        return HexFormat.of().parseHex(encoded.toString());
    }

    private static byte[] fixedText(String value) {
        byte[] text = value.getBytes(StandardCharsets.UTF_8);
        return HexFormat.of().parseHex(
                HexFormat.of().formatHex(text) + "00".repeat(32 - text.length));
    }

    private static byte[] dynamicText(String value) {
        byte[] text = value.getBytes(StandardCharsets.UTF_8);
        int paddedLength = (text.length + 31) / 32 * 32;
        return HexFormat.of().parseHex(
                "%064x".formatted(32)
                        + "%064x".formatted(text.length)
                        + HexFormat.of().formatHex(text)
                        + "00".repeat(paddedLength - text.length));
    }
}
