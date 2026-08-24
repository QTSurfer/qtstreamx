package com.qtsurfer.qtstreamx.dex.discovery.uniswap;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Locale;

final class EvmAbi {

    private static final HexFormat HEX = HexFormat.of();

    private EvmAbi() {}

    static String addressTopic(String topic) {
        byte[] word = word(topic);
        for (int index = 0; index < 12; index++) {
            if (word[index] != 0) {
                throw new IllegalArgumentException("address topic has non-zero padding");
            }
        }
        return "0x" + HEX.formatHex(word, 12, 32).toLowerCase(Locale.ROOT);
    }

    static String addressDataWord(String data, int wordIndex) {
        byte[] bytes = bytes(data);
        int offset = Math.multiplyExact(wordIndex, 32);
        if (bytes.length < offset + 32) {
            throw new IllegalArgumentException("event data is missing an address word");
        }
        byte[] word = Arrays.copyOfRange(bytes, offset, offset + 32);
        for (int index = 0; index < 12; index++) {
            if (word[index] != 0) {
                throw new IllegalArgumentException("address word has non-zero padding");
            }
        }
        return "0x" + HEX.formatHex(word, 12, 32).toLowerCase(Locale.ROOT);
    }

    static int uint24Topic(String topic) {
        byte[] word = word(topic);
        for (int index = 0; index < 29; index++) {
            if (word[index] != 0) {
                throw new IllegalArgumentException("uint24 topic exceeds its ABI range");
            }
        }
        return Byte.toUnsignedInt(word[29]) << 16
                | Byte.toUnsignedInt(word[30]) << 8
                | Byte.toUnsignedInt(word[31]);
    }

    static void requireDataWords(String data, int wordCount) {
        if (bytes(data).length != Math.multiplyExact(wordCount, 32)) {
            throw new IllegalArgumentException("event data has an unexpected ABI length");
        }
    }

    static int int24DataWord(String data, int wordIndex) {
        byte[] encoded = bytes(data);
        int offset = Math.multiplyExact(wordIndex, 32);
        if (encoded.length < offset + 32) {
            throw new IllegalArgumentException("event data is missing an int24 word");
        }
        boolean negative = (encoded[offset + 29] & 0x80) != 0;
        int padding = negative ? 0xff : 0;
        for (int index = offset; index < offset + 29; index++) {
            if (Byte.toUnsignedInt(encoded[index]) != padding) {
                throw new IllegalArgumentException("int24 word has invalid sign extension");
            }
        }
        int value = Byte.toUnsignedInt(encoded[offset + 29]) << 16
                | Byte.toUnsignedInt(encoded[offset + 30]) << 8
                | Byte.toUnsignedInt(encoded[offset + 31]);
        return negative ? value | 0xff000000 : value;
    }

    static String stringResult(byte[] encoded) {
        if (encoded.length < 64 || unsignedInt(encoded, 0) != 32) {
            throw new IllegalArgumentException("text result is not a dynamic ABI string");
        }
        int length = unsignedInt(encoded, 32);
        int paddedLength = Math.multiplyExact(Math.addExact(length, 31) / 32, 32);
        if (length == 0 || length > 64 || encoded.length != 64 + paddedLength) {
            throw new IllegalArgumentException("text result has an invalid length");
        }
        for (int index = 64 + length; index < encoded.length; index++) {
            if (encoded[index] != 0) {
                throw new IllegalArgumentException("text result has non-zero ABI padding");
            }
        }
        return utf8Text(encoded, 64, length, "string");
    }

    static String textResult(byte[] encoded) {
        if (encoded.length != 32) {
            return stringResult(encoded);
        }
        int length = 0;
        while (length < encoded.length && encoded[length] != 0) {
            length++;
        }
        for (int index = length; index < encoded.length; index++) {
            if (encoded[index] != 0) {
                throw new IllegalArgumentException("fixed text has non-zero padding");
            }
        }
        return utf8Text(encoded, 0, length, "fixed text");
    }

    static String addressResult(byte[] encoded) {
        if (encoded.length != 32) {
            throw new IllegalArgumentException("address result must contain one ABI word");
        }
        for (int index = 0; index < 12; index++) {
            if (encoded[index] != 0) {
                throw new IllegalArgumentException("address result has non-zero padding");
            }
        }
        return "0x" + HEX.formatHex(encoded, 12, 32).toLowerCase(Locale.ROOT);
    }

    static int uint24Result(byte[] encoded) {
        if (encoded.length != 32) {
            throw new IllegalArgumentException("uint24 result must contain one ABI word");
        }
        for (int index = 0; index < 29; index++) {
            if (encoded[index] != 0) {
                throw new IllegalArgumentException("uint24 result exceeds its ABI range");
            }
        }
        return Byte.toUnsignedInt(encoded[29]) << 16
                | Byte.toUnsignedInt(encoded[30]) << 8
                | Byte.toUnsignedInt(encoded[31]);
    }

    static BigInteger uintResult(byte[] encoded) {
        if (encoded.length != 32) {
            throw new IllegalArgumentException("uint result must contain one ABI word");
        }
        return new BigInteger(1, encoded);
    }

    static BigInteger uintDataWord(byte[] encoded, int wordIndex, int wordCount) {
        if (encoded.length != Math.multiplyExact(wordCount, 32)) {
            throw new IllegalArgumentException("result has an unexpected ABI length");
        }
        int offset = Math.multiplyExact(wordIndex, 32);
        if (wordIndex < 0 || offset + 32 > encoded.length) {
            throw new IllegalArgumentException("result is missing a uint word");
        }
        return new BigInteger(1, Arrays.copyOfRange(encoded, offset, offset + 32));
    }

    static byte[] call(String selector, String... arguments) {
        if (selector == null || !selector.matches("[0-9a-fA-F]{8}")) {
            throw new IllegalArgumentException("selector must contain four hex bytes");
        }
        StringBuilder encoded = new StringBuilder(selector.toLowerCase(Locale.ROOT));
        for (String argument : arguments) {
            if (argument == null || !argument.matches("[0-9a-fA-F]{64}")) {
                throw new IllegalArgumentException("argument must contain one ABI word");
            }
            encoded.append(argument.toLowerCase(Locale.ROOT));
        }
        return HEX.parseHex(encoded.toString());
    }

    static String addressArgument(String address) {
        if (address == null || !address.matches("0x[0-9a-fA-F]{40}")) {
            throw new IllegalArgumentException("address must be a 20-byte hex value");
        }
        return "0".repeat(24) + address.substring(2).toLowerCase(Locale.ROOT);
    }

    static String uintArgument(int value) {
        if (value < 0) {
            throw new IllegalArgumentException("value must be non-negative");
        }
        return "%064x".formatted(value);
    }

    static int uint8Result(byte[] encoded) {
        if (encoded.length != 32) {
            throw new IllegalArgumentException("uint8 result must contain one ABI word");
        }
        for (int index = 0; index < 31; index++) {
            if (encoded[index] != 0) {
                throw new IllegalArgumentException("uint8 result exceeds its ABI range");
            }
        }
        return Byte.toUnsignedInt(encoded[31]);
    }

    private static String utf8Text(byte[] encoded, int offset, int length, String field) {
        try {
            String value = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(encoded, offset, length))
                    .toString();
            if (value.isBlank() || value.codePoints().anyMatch(Character::isISOControl)) {
                throw new IllegalArgumentException(field + " contains unsupported characters");
            }
            return value;
        } catch (CharacterCodingException exception) {
            throw new IllegalArgumentException(field + " is not valid UTF-8", exception);
        }
    }

    private static int unsignedInt(byte[] encoded, int offset) {
        if (encoded.length < offset + 32) {
            throw new IllegalArgumentException("ABI word is truncated");
        }
        for (int index = offset; index < offset + 28; index++) {
            if (encoded[index] != 0) {
                throw new IllegalArgumentException("ABI value exceeds integer range");
            }
        }
        int value = ByteBuffer.wrap(encoded, offset + 28, 4).getInt();
        if (value < 0) {
            throw new IllegalArgumentException("ABI value exceeds integer range");
        }
        return value;
    }

    private static byte[] word(String value) {
        byte[] bytes = bytes(value);
        if (bytes.length != 32) {
            throw new IllegalArgumentException("topic must contain one ABI word");
        }
        return bytes;
    }

    private static byte[] bytes(String value) {
        if (value == null || !value.startsWith("0x")) {
            throw new IllegalArgumentException("hex value must start with 0x");
        }
        try {
            return HEX.parseHex(value.substring(2));
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("hex value is malformed", exception);
        }
    }
}
