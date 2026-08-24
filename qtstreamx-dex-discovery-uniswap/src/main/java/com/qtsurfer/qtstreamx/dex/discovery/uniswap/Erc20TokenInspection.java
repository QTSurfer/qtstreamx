package com.qtsurfer.qtstreamx.dex.discovery.uniswap;

import com.qtsurfer.qtstreamx.dex.core.EvmToken;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

/**
 * Bounded, trust-neutral ERC-20 metadata read from one EVM block state.
 *
 * <p>Name and symbol are untrusted display claims made by the contract. Their
 * presence does not make an address canonical or safe.
 *
 * @param network stable EVM network identity
 * @param address inspected contract address
 * @param contract whether bytecode existed at the requested block state
 * @param name sanitized optional contract name
 * @param symbol sanitized optional contract symbol
 * @param decimals optional contract decimal precision
 * @param unavailableFields metadata fields that reverted or returned unsupported data
 */
public record Erc20TokenInspection(
        String network,
        String address,
        boolean contract,
        Optional<String> name,
        Optional<String> symbol,
        OptionalInt decimals,
        Set<Field> unavailableFields
) {

    /** ERC-20 metadata fields probed independently by inspection. */
    public enum Field {
        /** The optional {@code name()} value. */
        NAME,
        /** The optional {@code symbol()} value. */
        SYMBOL,
        /** The optional {@code decimals()} value. */
        DECIMALS
    }

    /** Validates and defensively copies inspection data. */
    public Erc20TokenInspection {
        if (network == null || network.isBlank()) {
            throw new IllegalArgumentException("network must not be blank");
        }
        address = normalizeAddress(address);
        name = requireText(name, "name");
        symbol = requireText(symbol, "symbol");
        Objects.requireNonNull(decimals, "decimals");
        if (decimals.isPresent() && (decimals.getAsInt() < 0 || decimals.getAsInt() > 255)) {
            throw new IllegalArgumentException("decimals must be between 0 and 255");
        }
        unavailableFields = Set.copyOf(
                Objects.requireNonNull(unavailableFields, "unavailableFields"));
        if (!contract && (name.isPresent() || symbol.isPresent() || decimals.isPresent())) {
            throw new IllegalArgumentException("an address without code cannot expose metadata");
        }
        requireAvailability(name.isPresent(), Field.NAME, unavailableFields);
        requireAvailability(symbol.isPresent(), Field.SYMBOL, unavailableFields);
        requireAvailability(decimals.isPresent(), Field.DECIMALS, unavailableFields);
        if (!contract && unavailableFields.size() != Field.values().length) {
            throw new IllegalArgumentException("all metadata is unavailable without contract code");
        }
    }

    /**
     * Converts complete display metadata into the normalization token value.
     *
     * <p>This conversion does not authenticate the token address.
     *
     * @return token value when symbol and decimals are available
     */
    public Optional<EvmToken> token() {
        if (symbol.isEmpty() || decimals.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new EvmToken(symbol.orElseThrow(), address, decimals.getAsInt()));
    }

    private static Optional<String> requireText(Optional<String> value, String field) {
        Objects.requireNonNull(value, field);
        value.ifPresent(text -> {
            if (text.isBlank() || text.codePoints().anyMatch(Character::isISOControl)) {
                throw new IllegalArgumentException(field + " contains unsupported characters");
            }
        });
        return value;
    }

    private static void requireAvailability(
            boolean present,
            Field field,
            Set<Field> unavailableFields) {
        if (present == unavailableFields.contains(field)) {
            throw new IllegalArgumentException(field + " availability is inconsistent");
        }
    }

    private static String normalizeAddress(String value) {
        if (value == null || !value.matches("0x[0-9a-fA-F]{40}")) {
            throw new IllegalArgumentException("address must be a 20-byte hex value");
        }
        return value.toLowerCase(Locale.ROOT);
    }
}
