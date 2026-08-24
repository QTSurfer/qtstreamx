# qtstreamx-dex-uniswap-v3

Read-only Uniswap v3 market-data normalization for QTStreamX. The module turns
confirmed pool `Swap` logs into `MarketTrade` records; it does not discover
pools, quote routes, or submit transactions.

`UniswapV3MarketDataStream` implements the core `MarketTradeStream` lifecycle,
so aggregation and capture callers can consume v2 and v3 through the same
normalized interface while configuration and protocol behavior remain explicit.
Shared `EvmToken` metadata and `EvmTokenPair` orientation invariants come from
`qtstreamx-dex-core`.

## Pool configuration

Each pool is explicit and independent from RPC credentials:

```java
UniswapV3Pool pool = new UniswapV3Pool(
        "eip155:1",
        poolAddress,
        new EvmToken("USDC", usdcAddress, 6),
        new EvmToken("WETH", wethAddress, 18),
        new Instrument("WETH", "USDC"),
        500);
```

Examples and canaries can use the reviewed immutable presets directly:

```java
UniswapV3Pool ethereum =
        KnownUniswapV3Markets.ETHEREUM_MAINNET_WETH_USDC_500;
UniswapV3Pool robinhood =
        KnownUniswapV3Markets.ROBINHOOD_MAINNET_WETH_USDG_100;
```

`KnownUniswapV3Markets` is a deliberately small convenience set, not a complete
or dynamic catalogue. Presets never rank or automatically subscribe to pools.

The [DEX discovery CLI](../qtstreamx-dex-discovery-cli/README.md) exposes these
reviewed examples and bounded V3 factory/token/pool lookup through the explicit
`uniswap` command prefix. Its descriptors preserve fee tier, native token order,
logical orientation, factory provenance, and review status; inspection never
starts this streaming adapter automatically.

`token0` and `token1` are explicit token descriptors and must be passed in
ascending contract-address order, as required by Uniswap. The `Instrument`
independently states the logical base/quote orientation; in this example WETH is
the base even though it is token1. The constructor validates that both views
agree. The adapter then applies token decimals, reports price as quote per base,
and derives `BUY`/`SELL` from the base asset's pool delta.

## Streaming

```java
EvmLogStream source = new EvmRpcLogStream(rpcConfig, JdkWebSocketClient::new);
try (UniswapV3MarketDataStream stream =
        new UniswapV3MarketDataStream(source, List.of(pool))) {
    stream.onError(error -> report(error));
    stream.start(trade -> consume(trade));
}
```

The source owns confirmation, canonical-chain checks, reconnect, and catch-up.
This module owns the canonical v3 `Swap` ABI, pool orientation, decimal scaling,
market identity, and stable event IDs. Malformed events are sent to `onError`
without stopping valid configured pools.

## Verification

```bash
gradle :qtstreamx-dex-uniswap-v3:test
gradle :qtstreamx-dex-uniswap-v3:test -Pit
```

The canary opt-in IT defaults to the credential-free dRPC public endpoints
`https://eth.drpc.org` and `wss://eth.drpc.org`. Override them with
`QTSTREAMX_EVM_HTTP_URL` and `QTSTREAMX_EVM_WS_URL`. Private provider URLs and
credentials remain runtime-only configuration and must not enter source or
artifacts.
