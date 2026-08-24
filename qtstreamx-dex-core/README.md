# qtstreamx-dex-core

Shared, protocol-neutral EVM DEX vocabulary for QTStreamX. This module owns the
token metadata and ordered-pair invariants used by the Uniswap v2 and v3
adapters without hiding either protocol's behavior.

## Token and market orientation

```java
EvmToken usdc = new EvmToken("USDC", usdcAddress, 6);
EvmToken weth = new EvmToken("WETH", wethAddress, 18);

EvmTokenPair tokens = new EvmTokenPair(
        usdc,
        weth,
        new Instrument("WETH", "USDC"));
```

`EvmToken` validates metadata and normalizes its contract address.
`EvmTokenPair` requires ascending contract-address order for `token0/token1`
while keeping logical base/quote orientation independent. In the example WETH
is token1 on-chain and the logical base of `WETH/USDC`.

The pair exposes `baseToken()`, `quoteToken()`, and `token0IsBase()` so protocol
adapters do not repeat orientation logic. Only spot instruments are accepted.

## Intentional limits

This module does not define a universal pool, factory, Swap event, fee model,
price calculation, discovery mechanism, or trading interface. Those rules stay
in their versioned protocol modules. `MarketTradeStream` and normalized market
records remain in `qtstreamx-core` because they are not exclusive to EVM DEXes.

## Verification

```bash
gradle :qtstreamx-dex-core:test
gradle :qtstreamx-dex-core:javadoc
```
