package com.qtsurfer.qtstreamx.dex.discovery.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapOnChainLookup;
import com.qtsurfer.qtstreamx.evm.rpc.EvmBlockTag;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogFilter;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcLog;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcReader;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.Test;

class OnChainUniswapCliAdapterTest {
    private static final String NETWORK = "eip155:4663";
    private static final String TOKEN0 = "0x0000000000000000000000000000000000000010";
    private static final String RUBY = "0x0000000000000000000000000000000000000020";
    private static final String PAIR = "0x0000000000000000000000000000000000000030";
    private static final String FACTORY = "0x0000000000000000000000000000000000000040";
    private static final String V2_TOPIC =
            "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9";

    @Test
    void infersAndValidatesAnUnreviewedV2Pair() {
        OnChainUniswapCliAdapter backend = backend();
        CliRequest request = CliParser.parse(List.of(
                "uniswap",
                "pool", "--network", "robinhood", PAIR));

        CliResponse response = backend.execute(request);

        assertThat(response.status()).isEqualTo("ok");
        assertThat((List<?>) response.data()).singleElement().satisfies(value -> {
            CliData.Pool pool = (CliData.Pool) value;
            assertThat(pool.version()).isEqualTo("v2");
            assertThat(pool.factoryAddress()).isEqualTo(FACTORY);
            assertThat(pool.token1().symbol()).isEqualTo("RUBY");
            assertThat(pool.instrument()).isNull();
            assertThat(pool.orientation()).isEqualTo("unavailable");
            assertThat(pool.reviewed()).isFalse();
            assertThat(pool.reserve0()).isEqualTo("11");
            assertThat(pool.reserve1()).isEqualTo("22");
        });
    }

    @Test
    void rejectsSearchBeforeMetadataCallsWhenTheTokenLimitIsExceeded() {
        OnChainUniswapCliAdapter backend = backend();
        CliRequest request = CliParser.parse(List.of(
                "uniswap",
                "search",
                "--network", "robinhood",
                "--version", "v2",
                "--factory", FACTORY,
                "--query", "ruby",
                "--from", "100",
                "--to", "100",
                "--token-limit", "1"));

        CliLookupException exception = assertThrows(
                CliLookupException.class, () -> backend.execute(request));

        assertThat(exception.reason()).isEqualTo("LIMIT");
    }

    @Test
    void searchesMetadataOnlyInsideTheBoundedFactoryResult() {
        OnChainUniswapCliAdapter backend = backend();
        CliRequest request = CliParser.parse(List.of(
                "uniswap",
                "search",
                "--network", "robinhood",
                "--version", "v2",
                "--factory", FACTORY,
                "--query", "ruby",
                "--from", "100",
                "--to", "100"));

        CliResponse response = backend.execute(request);

        assertThat(response.status()).isEqualTo("ok");
        assertThat((List<?>) response.data()).singleElement().satisfies(value -> {
            CliData.SearchMatch match = (CliData.SearchMatch) value;
            assertThat(match.token().name()).isEqualTo("The Reddit Dog");
            assertThat(match.token().address()).isEqualTo(RUBY);
            assertThat(match.market().address()).isEqualTo(PAIR);
            assertThat(match.market().reviewed()).isFalse();
        });
    }

    private static OnChainUniswapCliAdapter backend() {
        return new OnChainUniswapCliAdapter(
                NETWORK,
                new UniswapOnChainLookup(new PairReader()));
    }

    private static final class PairReader implements EvmRpcReader {
        @Override
        public long latestBlockNumber() {
            return 100;
        }

        @Override
        public List<EvmRpcLog> logs(EvmLogFilter filter, long fromBlock, long toBlock) {
            return List.of(new EvmRpcLog(
                    FACTORY,
                    List.of(V2_TOPIC, topicAddress(TOKEN0), topicAddress(RUBY)),
                    "0x" + wordAddress(PAIR) + word(1),
                    100,
                    "0xblock",
                    "0xtransaction",
                    0,
                    0,
                    false));
        }

        @Override
        public byte[] call(String contractAddress, byte[] data, EvmBlockTag blockTag) {
            String selector = HexFormat.of().formatHex(Arrays.copyOf(data, 4));
            return switch (selector) {
                case "c45a0155" -> bytes(wordAddress(FACTORY));
                case "0dfe1681" -> bytes(wordAddress(TOKEN0));
                case "d21220a7" -> bytes(wordAddress(RUBY));
                case "e6a43905" -> bytes(wordAddress(PAIR));
                case "0902f1ac" -> bytes(word(11) + word(22) + word(100));
                case "06fdde03" -> text(contractAddress.equals(RUBY)
                        ? "The Reddit Dog"
                        : "Wrapped Ether");
                case "95d89b41" -> text(contractAddress.equals(RUBY) ? "RUBY" : "WETH");
                case "313ce567" -> bytes(word(contractAddress.equals(RUBY) ? 9 : 18));
                default -> throw new IllegalArgumentException("unsupported test selector");
            };
        }

        @Override
        public byte[] code(String contractAddress, EvmBlockTag blockTag) {
            return new byte[] {1};
        }
    }

    private static String topicAddress(String address) {
        return "0x" + wordAddress(address);
    }

    private static String wordAddress(String address) {
        return "0".repeat(24) + address.substring(2).toLowerCase();
    }

    private static String word(long value) {
        return "%064x".formatted(BigInteger.valueOf(value));
    }

    private static byte[] text(String value) {
        byte[] text = value.getBytes(StandardCharsets.UTF_8);
        int padded = (text.length + 31) / 32 * 32;
        ByteBuffer buffer = ByteBuffer.allocate(64 + padded);
        buffer.put(bytes(word(32)));
        buffer.put(bytes(word(text.length)));
        buffer.put(text);
        return buffer.array();
    }

    private static byte[] bytes(String hex) {
        return HexFormat.of().parseHex(hex);
    }
}
