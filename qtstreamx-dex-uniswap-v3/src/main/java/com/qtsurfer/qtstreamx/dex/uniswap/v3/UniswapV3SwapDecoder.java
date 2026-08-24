package com.qtsurfer.qtstreamx.dex.uniswap.v3;

import com.qtsurfer.qtstreamx.core.model.MarketId;
import com.qtsurfer.qtstreamx.core.model.MarketTrade;
import com.qtsurfer.qtstreamx.core.model.TradeSide;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLog;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.Locale;

final class UniswapV3SwapDecoder {
    static final String SWAP_TOPIC =
            "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";

    private static final int ABI_WORD_HEX_LENGTH = 64;
    private static final int SWAP_WORDS = 5;
    private static final BigInteger TWO_256 = BigInteger.ONE.shiftLeft(256);
    private static final BigDecimal Q192 = new BigDecimal(BigInteger.ONE.shiftLeft(192));
    private static final MathContext PRICE_CONTEXT = MathContext.DECIMAL128;

    private UniswapV3SwapDecoder() {}

    static MarketTrade decode(UniswapV3Pool pool, EvmLog log) {
        validateEnvelope(pool, log);
        String data = log.data().substring(2);
        BigInteger amount0 = signedWord(data, 0);
        BigInteger amount1 = signedWord(data, 1);
        BigInteger sqrtPriceX96 = unsignedWord(data, 2);
        BigInteger liquidity = unsignedWord(data, 3);
        BigInteger tick = signedWord(data, 4);
        validateValues(amount0, amount1, sqrtPriceX96, liquidity, tick);

        boolean token0Base = pool.tokens().token0IsBase();
        BigInteger baseRaw = token0Base ? amount0 : amount1;
        BigInteger quoteRaw = token0Base ? amount1 : amount0;
        int baseDecimals = pool.tokens().baseToken().decimals();
        int quoteDecimals = pool.tokens().quoteToken().decimals();

        MarketId market = new MarketId(
                "uniswap-v3", pool.network(), pool.address(), pool.instrument());
        return new MarketTrade(
                market,
                eventId(log),
                price(pool, sqrtPriceX96),
                decimalAmount(baseRaw.abs(), baseDecimals),
                decimalAmount(quoteRaw.abs(), quoteDecimals),
                baseRaw.signum() < 0 ? TradeSide.BUY : TradeSide.SELL,
                log.timestamp());
    }

    private static void validateEnvelope(UniswapV3Pool pool, EvmLog log) {
        if (!pool.network().equals(log.network())) {
            throw new IllegalArgumentException("log network does not match pool");
        }
        if (!pool.address().equals(log.address().toLowerCase(Locale.ROOT))) {
            throw new IllegalArgumentException("log address does not match pool");
        }
        if (log.topics().size() != 3 || !SWAP_TOPIC.equalsIgnoreCase(log.topics().getFirst())) {
            throw new IllegalArgumentException("log topic is not Uniswap v3 Swap");
        }
        int expectedLength = 2 + ABI_WORD_HEX_LENGTH * SWAP_WORDS;
        if (!log.data().startsWith("0x") || log.data().length() != expectedLength) {
            throw new IllegalArgumentException("invalid Swap data length");
        }
        for (int index = 2; index < log.data().length(); index++) {
            if (Character.digit(log.data().charAt(index), 16) < 0) {
                throw new IllegalArgumentException("Swap data is not hexadecimal");
            }
        }
    }

    private static void validateValues(
            BigInteger amount0,
            BigInteger amount1,
            BigInteger sqrtPriceX96,
            BigInteger liquidity,
            BigInteger tick) {
        if (amount0.signum() == 0 || amount1.signum() == 0
                || amount0.signum() == amount1.signum()) {
            throw new IllegalArgumentException("Swap amounts must be non-zero with opposite signs");
        }
        if (sqrtPriceX96.signum() <= 0 || sqrtPriceX96.bitLength() > 160) {
            throw new IllegalArgumentException("sqrtPriceX96 is outside uint160 range");
        }
        if (liquidity.bitLength() > 128) {
            throw new IllegalArgumentException("liquidity is outside uint128 range");
        }
        if (tick.compareTo(BigInteger.valueOf(-887_272)) < 0
                || tick.compareTo(BigInteger.valueOf(887_272)) > 0) {
            throw new IllegalArgumentException("tick is outside Uniswap v3 range");
        }
    }

    private static BigDecimal price(UniswapV3Pool pool, BigInteger sqrtPriceX96) {
        BigDecimal squared = new BigDecimal(sqrtPriceX96.multiply(sqrtPriceX96));
        if (pool.tokens().token0IsBase()) {
            return squared
                    .scaleByPowerOfTen(pool.token0().decimals() - pool.token1().decimals())
                    .divide(Q192, PRICE_CONTEXT);
        }
        return Q192
                .scaleByPowerOfTen(pool.token1().decimals() - pool.token0().decimals())
                .divide(squared, PRICE_CONTEXT);
    }

    private static BigDecimal decimalAmount(BigInteger value, int decimals) {
        return new BigDecimal(value, decimals);
    }

    private static String eventId(EvmLog log) {
        return log.network() + ":" + log.blockHash() + ":"
                + log.transactionHash() + ":" + log.logIndex();
    }

    private static BigInteger unsignedWord(String data, int index) {
        int start = index * ABI_WORD_HEX_LENGTH;
        return new BigInteger(data.substring(start, start + ABI_WORD_HEX_LENGTH), 16);
    }

    private static BigInteger signedWord(String data, int index) {
        BigInteger value = unsignedWord(data, index);
        return value.testBit(255) ? value.subtract(TWO_256) : value;
    }
}
