package com.qtsurfer.qtstreamx.dex.discovery.uniswap.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.dex.discovery.uniswap.Erc20TokenInspection;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.KnownUniswapDeployments;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapOnChainLookup;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapV3PoolInspection;
import com.qtsurfer.qtstreamx.evm.rpc.EvmBlockTag;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogFilter;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcLog;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class PublicUniswapOnChainLookupTest {

    private static final String TOKEN = "0xc0a9531cae8bea6268bd19efec1dd205830cae2a";
    private static final String WETH = "0x0bd7d308f8e1639fab988df18a8011f41eacad73";

    @Test
    void consumesTokenAndPoolLookupThroughPublicPackages() {
        UniswapOnChainLookup lookup = new UniswapOnChainLookup(new PublicReader());

        Erc20TokenInspection token = lookup.inspectToken(
                "eip155:4663", TOKEN, EvmBlockTag.latest());
        List<UniswapV3PoolInspection> pools = lookup.findV3Pools(
                KnownUniswapDeployments.ROBINHOOD_MAINNET_V3,
                TOKEN,
                Set.of(WETH),
                Set.of(100, 500, 3_000, 10_000),
                EvmBlockTag.latest());

        assertThat(token.name()).contains("The Reddit Dog");
        assertThat(token.symbol()).contains("RUBY");
        assertThat(token.decimals()).hasValue(9);
        assertThat(pools).isEmpty();
        assertThat(lookup.marketFactory(TOKEN, EvmBlockTag.latest()))
                .isEqualTo(KnownUniswapDeployments.ROBINHOOD_MAINNET_V3
                        .factoryScan().factoryAddress());
    }

    private static final class PublicReader implements EvmRpcReader {

        @Override
        public long latestBlockNumber() {
            return 1;
        }

        @Override
        public List<EvmRpcLog> logs(EvmLogFilter filter, long fromBlock, long toBlock) {
            return List.of();
        }

        @Override
        public byte[] code(String contractAddress, EvmBlockTag blockTag) {
            return new byte[] {1};
        }

        @Override
        public byte[] call(String contractAddress, byte[] data, EvmBlockTag blockTag) {
            String selector = HexFormat.of().formatHex(Arrays.copyOf(data, 4));
            return switch (selector) {
                case "06fdde03" -> dynamicText("The Reddit Dog");
                case "95d89b41" -> dynamicText("RUBY");
                case "313ce567" -> uint(9);
                case "1698ee82" -> new byte[32];
                case "c45a0155" -> address(KnownUniswapDeployments.ROBINHOOD_MAINNET_V3
                        .factoryScan().factoryAddress());
                default -> throw new AssertionError("unexpected selector " + selector);
            };
        }
    }

    private static byte[] uint(int value) {
        return HexFormat.of().parseHex("%064x".formatted(value));
    }

    private static byte[] address(String value) {
        return HexFormat.of().parseHex("0".repeat(24) + value.substring(2));
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
