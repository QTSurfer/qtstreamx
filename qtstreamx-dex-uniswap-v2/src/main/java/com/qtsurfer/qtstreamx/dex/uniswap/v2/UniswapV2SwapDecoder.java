package com.qtsurfer.qtstreamx.dex.uniswap.v2;

import com.qtsurfer.qtstreamx.core.model.MarketId;
import com.qtsurfer.qtstreamx.core.model.MarketTrade;
import com.qtsurfer.qtstreamx.core.model.TradeSide;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLog;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.Locale;

final class UniswapV2SwapDecoder {
    static final String SWAP_TOPIC =
            "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822";

    private static final int ABI_WORD_HEX_LENGTH = 64;
    private static final int SWAP_WORDS = 4;

    private UniswapV2SwapDecoder() {}

    static MarketTrade decode(UniswapV2Pair pair, EvmLog log) {
        validateEnvelope(pair, log);
        String data = log.data().substring(2);
        BigInteger amount0In = word(data, 0);
        BigInteger amount1In = word(data, 1);
        BigInteger amount0Out = word(data, 2);
        BigInteger amount1Out = word(data, 3);
        validateDirection(amount0In, amount1In, amount0Out, amount1Out);

        boolean token0Base = pair.tokens().token0IsBase();
        BigInteger baseRaw;
        BigInteger quoteRaw;
        TradeSide side;
        if (token0Base) {
            baseRaw = amount0In.signum() > 0 ? amount0In : amount0Out;
            quoteRaw = amount1In.signum() > 0 ? amount1In : amount1Out;
            side = amount0In.signum() > 0 ? TradeSide.SELL : TradeSide.BUY;
        } else {
            baseRaw = amount1In.signum() > 0 ? amount1In : amount1Out;
            quoteRaw = amount0In.signum() > 0 ? amount0In : amount0Out;
            side = amount1In.signum() > 0 ? TradeSide.SELL : TradeSide.BUY;
        }

        BigDecimal baseAmount = new BigDecimal(baseRaw, pair.tokens().baseToken().decimals());
        BigDecimal quoteAmount = new BigDecimal(quoteRaw, pair.tokens().quoteToken().decimals());
        return new MarketTrade(
                new MarketId("uniswap-v2", pair.network(), pair.address(), pair.instrument()),
                eventId(log),
                quoteAmount.divide(baseAmount, MathContext.DECIMAL128),
                baseAmount,
                quoteAmount,
                side,
                log.timestamp());
    }

    private static void validateEnvelope(UniswapV2Pair pair, EvmLog log) {
        if (!pair.network().equals(log.network())) {
            throw new IllegalArgumentException("log network does not match pair");
        }
        if (!pair.address().equals(log.address().toLowerCase(Locale.ROOT))) {
            throw new IllegalArgumentException("log address does not match pair");
        }
        if (log.topics().size() != 3 || !SWAP_TOPIC.equalsIgnoreCase(log.topics().getFirst())) {
            throw new IllegalArgumentException("log topic is not Uniswap v2 Swap");
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

    private static void validateDirection(
            BigInteger amount0In,
            BigInteger amount1In,
            BigInteger amount0Out,
            BigInteger amount1Out) {
        boolean token0ToToken1 = amount0In.signum() > 0
                && amount1In.signum() == 0
                && amount0Out.signum() == 0
                && amount1Out.signum() > 0;
        boolean token1ToToken0 = amount0In.signum() == 0
                && amount1In.signum() > 0
                && amount0Out.signum() > 0
                && amount1Out.signum() == 0;
        if (!token0ToToken1 && !token1ToToken0) {
            throw new IllegalArgumentException(
                    "Swap amounts must describe exactly one input and opposite output");
        }
    }

    private static BigInteger word(String data, int index) {
        int start = index * ABI_WORD_HEX_LENGTH;
        return new BigInteger(data.substring(start, start + ABI_WORD_HEX_LENGTH), 16);
    }

    private static String eventId(EvmLog log) {
        return log.network() + ":" + log.blockHash() + ":"
                + log.transactionHash() + ":" + log.logIndex();
    }
}
