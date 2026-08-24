package com.qtsurfer.qtstreamx.dex.discovery.uniswap;

import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.dex.core.EvmToken;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class AddressBasedUniswapPairOrientationTest {

    private static final String ETHEREUM = "eip155:1";
    private static final String WETH = "0x0000000000000000000000000000000000000001";
    private static final String USDC = "0x0000000000000000000000000000000000000002";
    private static final String UNKNOWN = "0x0000000000000000000000000000000000000003";

    @Test
    void orientsTheAllowedBaseAgainstTheCanonicalQuoteInEitherTokenPosition() {
        AddressBasedUniswapPairOrientation orientation =
                new AddressBasedUniswapPairOrientation(Map.of(
                        ETHEREUM,
                        new UniswapNetworkTokenPolicy(Set.of(USDC), Set.of(WETH))));
        EvmToken weth = new EvmToken("WETH", WETH, 18);
        EvmToken usdc = new EvmToken("USDC", USDC, 6);

        assertThat(orientation.orient(ETHEREUM, weth, usdc))
                .contains(new Instrument("WETH", "USDC"));
        assertThat(orientation.orient(ETHEREUM, usdc, weth))
                .contains(new Instrument("WETH", "USDC"));
    }

    @Test
    void rejectsSymbolsThatSpoofTrustedAddressesAndAmbiguousPairs() {
        AddressBasedUniswapPairOrientation orientation =
                new AddressBasedUniswapPairOrientation(Map.of(
                        ETHEREUM,
                        new UniswapNetworkTokenPolicy(Set.of(USDC), Set.of(WETH))));
        EvmToken weth = new EvmToken("WETH", WETH, 18);
        EvmToken usdc = new EvmToken("USDC", USDC, 6);
        EvmToken spoofedUsdc = new EvmToken("USDC", UNKNOWN, 6);

        assertThat(orientation.orient(ETHEREUM, weth, spoofedUsdc)).isEmpty();
        assertThat(orientation.orient(
                        ETHEREUM,
                        new EvmToken("WETH", WETH, 18),
                        new EvmToken("OTHER", UNKNOWN, 18)))
                .isEmpty();
        assertThat(orientation.orient("eip155:999", weth, usdc)).isEmpty();

        AddressBasedUniswapPairOrientation twoQuotes =
                new AddressBasedUniswapPairOrientation(Map.of(
                        ETHEREUM,
                        new UniswapNetworkTokenPolicy(Set.of(USDC, UNKNOWN), Set.of(WETH))));
        assertThat(twoQuotes.orient(ETHEREUM, usdc, spoofedUsdc)).isEmpty();
    }

    @Test
    void appliesIndependentCanonicalAddressSetsPerNetwork() {
        String robinhood = "eip155:4663";
        String robinhoodQuote = "0x5fc5360D0400a0Fd4f2af552ADD042D716F1d168";
        String robinhoodBase = "0x0Bd7D308f8E1639FAb988df18A8011f41EAcAD73";
        AddressBasedUniswapPairOrientation orientation =
                new AddressBasedUniswapPairOrientation(Map.of(
                        ETHEREUM,
                        new UniswapNetworkTokenPolicy(Set.of(USDC), Set.of(WETH)),
                        robinhood,
                        new UniswapNetworkTokenPolicy(
                                Set.of("0x" + robinhoodQuote.substring(2).toUpperCase()),
                                Set.of(robinhoodBase))));

        assertThat(orientation.orient(
                        robinhood,
                        new EvmToken("WETH", robinhoodBase, 18),
                        new EvmToken("USDG", robinhoodQuote, 6)))
                .contains(new Instrument("WETH", "USDG"));
        assertThat(orientation.orient(
                        ETHEREUM,
                        new EvmToken("WETH", robinhoodBase, 18),
                        new EvmToken("USDG", robinhoodQuote, 6)))
                .isEmpty();
    }
}
