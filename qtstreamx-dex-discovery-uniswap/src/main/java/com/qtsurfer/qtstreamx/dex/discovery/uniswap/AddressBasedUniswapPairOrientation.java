package com.qtsurfer.qtstreamx.dex.discovery.uniswap;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.dex.core.EvmToken;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Orients trusted pairs using canonical token addresses scoped by network. */
public final class AddressBasedUniswapPairOrientation implements UniswapPairOrientation {

    private final Map<String, UniswapNetworkTokenPolicy> networks;

    /**
     * Creates an immutable multichain orientation policy.
     *
     * @param networks canonical token policy keyed by stable EVM network identity
     */
    public AddressBasedUniswapPairOrientation(Map<String, UniswapNetworkTokenPolicy> networks) {
        Objects.requireNonNull(networks, "networks");
        if (networks.isEmpty()) {
            throw new IllegalArgumentException("networks must not be empty");
        }
        networks.forEach((network, policy) -> {
            if (network == null || network.isBlank()) {
                throw new IllegalArgumentException("network must not be blank");
            }
            Objects.requireNonNull(policy, "network policy");
        });
        this.networks = Map.copyOf(networks);
    }

    @Override
    public boolean acceptsAddresses(
            String network,
            String token0Address,
            String token1Address) {
        UniswapNetworkTokenPolicy policy = networks.get(network);
        if (policy == null) {
            return false;
        }
        boolean token0Quote = policy.quoteTokenAddresses().contains(token0Address);
        boolean token1Quote = policy.quoteTokenAddresses().contains(token1Address);
        if (token0Quote == token1Quote) {
            return false;
        }
        String baseAddress = token0Quote ? token1Address : token0Address;
        return policy.baseTokenAddresses().contains(baseAddress);
    }

    @Override
    public Optional<Instrument> orient(String network, EvmToken token0, EvmToken token1) {
        if (!acceptsAddresses(network, token0.address(), token1.address())) {
            return Optional.empty();
        }
        UniswapNetworkTokenPolicy policy = networks.get(network);
        boolean token0Quote = policy.quoteTokenAddresses().contains(token0.address());
        EvmToken base = token0Quote ? token1 : token0;
        EvmToken quote = token0Quote ? token0 : token1;
        return Optional.of(new Instrument(base.symbol(), quote.symbol()));
    }
}
