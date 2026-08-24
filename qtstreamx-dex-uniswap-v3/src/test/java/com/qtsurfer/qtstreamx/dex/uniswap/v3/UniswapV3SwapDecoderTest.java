package com.qtsurfer.qtstreamx.dex.uniswap.v3;

import com.qtsurfer.qtstreamx.dex.core.EvmToken;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.model.MarketTrade;
import com.qtsurfer.qtstreamx.core.model.TradeSide;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLog;
import java.math.BigInteger;
import java.util.List;
import org.junit.jupiter.api.Test;

class UniswapV3SwapDecoderTest {

    private static final String POOL = "0x00000000000000000000000000000000000000ab";
    private static final String TOKEN0_ADDRESS = "0x0000000000000000000000000000000000000001";
    private static final String TOKEN1_ADDRESS = "0x0000000000000000000000000000000000000002";
    private static final BigInteger Q96 = BigInteger.ONE.shiftLeft(96);
    private static final UniswapV3Pool TOKEN0_BASE = new UniswapV3Pool(
            "eip155:1",
            POOL,
            new EvmToken("BASE", TOKEN0_ADDRESS, 6),
            new EvmToken("QUOTE", TOKEN1_ADDRESS, 18),
            new Instrument("BASE", "QUOTE"),
            3_000);

    @Test
    void usesCanonicalSwapEventTopic() {
        assertThat(UniswapV3SwapDecoder.SWAP_TOPIC)
                .isEqualTo("0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67")
                .hasSize(66);
    }

    @Test
    void decodesToken0SaleWithDecimalAsymmetry() {
        EvmLog log = swapLog(
                BigInteger.valueOf(1_000_000),
                BigInteger.valueOf(-1_000_000),
                Q96);

        MarketTrade trade = UniswapV3SwapDecoder.decode(TOKEN0_BASE, log);

        assertThat(trade.market().nativeId()).isEqualTo(POOL);
        assertThat(trade.market().instrument()).isEqualTo(new Instrument("BASE", "QUOTE"));
        assertThat(trade.eventId()).isEqualTo("eip155:1:0xblock:0xtx:7");
        assertThat(trade.price()).isEqualByComparingTo("0.000000000001");
        assertThat(trade.baseAmount()).isEqualByComparingTo("1");
        assertThat(trade.quoteAmount()).isEqualByComparingTo("0.000000000001");
        assertThat(trade.side()).isEqualTo(TradeSide.SELL);
        assertThat(trade.timestamp()).isEqualTo(1_700_000_000_123_456L);
    }

    @Test
    void decodesOppositeDirectionAsBaseBuy() {
        MarketTrade trade = UniswapV3SwapDecoder.decode(
                TOKEN0_BASE,
                swapLog(
                        BigInteger.valueOf(-2_000_000),
                        BigInteger.valueOf(2_000_000),
                        Q96));

        assertThat(trade.baseAmount()).isEqualByComparingTo("2");
        assertThat(trade.quoteAmount()).isEqualByComparingTo("0.000000000002");
        assertThat(trade.side()).isEqualTo(TradeSide.BUY);
    }

    @Test
    void invertsPoolPriceWhenToken1IsLogicalBase() {
        UniswapV3Pool token1Base = new UniswapV3Pool(
                "eip155:1",
                POOL,
                new EvmToken("QUOTE", TOKEN0_ADDRESS, 6),
                new EvmToken("BASE", TOKEN1_ADDRESS, 18),
                new Instrument("BASE", "QUOTE"),
                3_000);

        MarketTrade trade = UniswapV3SwapDecoder.decode(
                token1Base,
                swapLog(
                        new BigInteger("-1000000000000000000"),
                        new BigInteger("1000000000000000000"),
                        Q96));

        assertThat(trade.price()).isEqualByComparingTo("1000000000000");
        assertThat(trade.baseAmount()).isEqualByComparingTo("1");
        assertThat(trade.quoteAmount()).isEqualByComparingTo("1000000000000");
        assertThat(trade.side()).isEqualTo(TradeSide.SELL);
    }

    @Test
    void rejectsUnrelatedOrMalformedEvents() {
        EvmLog unrelated = new EvmLog(
                "eip155:1", POOL, List.of("0xdeadbeef"), "0x", 100,
                "0xblock", "0xtx", 0, 7, 1L);
        EvmLog malformed = new EvmLog(
                "eip155:1", POOL,
                List.of(UniswapV3SwapDecoder.SWAP_TOPIC, "0xsender", "0xrecipient"),
                "0x1234", 100, "0xblock", "0xtx", 0, 7, 1L);

        assertThatThrownBy(() -> UniswapV3SwapDecoder.decode(TOKEN0_BASE, unrelated))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("topic");
        assertThatThrownBy(() -> UniswapV3SwapDecoder.decode(TOKEN0_BASE, malformed))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("data length");
    }

    static EvmLog swapLog(BigInteger amount0, BigInteger amount1, BigInteger sqrtPriceX96) {
        String data = "0x"
                + signedWord(amount0)
                + signedWord(amount1)
                + unsignedWord(sqrtPriceX96)
                + unsignedWord(BigInteger.ONE)
                + signedWord(BigInteger.ZERO);
        return new EvmLog(
                "eip155:1",
                POOL,
                List.of(UniswapV3SwapDecoder.SWAP_TOPIC, "0xsender", "0xrecipient"),
                data,
                100,
                "0xblock",
                "0xtx",
                3,
                7,
                1_700_000_000_123_456L);
    }

    private static String signedWord(BigInteger value) {
        BigInteger encoded = value.signum() < 0 ? value.add(BigInteger.ONE.shiftLeft(256)) : value;
        return unsignedWord(encoded);
    }

    private static String unsignedWord(BigInteger value) {
        return "%064x".formatted(value);
    }
}
