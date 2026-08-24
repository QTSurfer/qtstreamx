# qtstreamx-dex-uniswap-v2

Read-only Uniswap v2 market-data normalization for QTStreamX. The module turns
confirmed pair `Swap` logs into `MarketTrade` records; it does not discover
pairs, read reserves, quote routes, or submit transactions.

`UniswapV2MarketDataStream` implements the core `MarketTradeStream` lifecycle,
so aggregation and capture callers can consume v2 and v3 through the same
normalized interface while configuration and protocol behavior remain explicit.
Shared `EvmToken` metadata and `EvmTokenPair` orientation invariants come from
`qtstreamx-dex-core`.

## Pair configuration

Each pair is explicit and independent from RPC credentials:

```java
UniswapV2Pair pair = new UniswapV2Pair(
        "eip155:1",
        pairAddress,
        new EvmToken("USDC", usdcAddress, 6),
        new EvmToken("WETH", wethAddress, 18),
        new Instrument("WETH", "USDC"));
```

For examples and canaries, the module includes a small reviewed preset:

```java
UniswapV2Pair pair = KnownUniswapV2Markets.ETHEREUM_MAINNET_WETH_USDC;
```

`KnownUniswapV2Markets` is intentionally not a complete or dynamic catalogue.
Using a preset does not discover, rank, or subscribe to anything; it only avoids
repeating an already reviewed explicit descriptor.

The [DEX discovery CLI](../qtstreamx-dex-discovery-cli/README.md) lists reviewed
examples and can inspect or discover additional V2-shaped pairs through the
separate Uniswap discovery module. An inferred pair remains unreviewed unless
its factory is present in the reviewed deployment catalogue; CLI inspection
does not start this streaming adapter automatically.

`token0` and `token1` use the pair contract's ascending token-address order.
The `Instrument` independently states logical base/quote orientation. The
constructor validates that both views agree. The adapter derives execution
price from the event's actual input/output amounts and reports side from the
logical base asset's pool flow.

## Streaming

```java
EvmLogStream source = new EvmRpcLogStream(rpcConfig, JdkWebSocketClient::new);
try (UniswapV2MarketDataStream stream =
        new UniswapV2MarketDataStream(source, List.of(pair))) {
    stream.onError(error -> report(error));
    stream.start(trade -> consume(trade));
}
```

The source owns confirmation, canonical-chain checks, reconnect, and catch-up.
This module owns the canonical v2 `Swap` ABI, pair orientation, decimal scaling,
market identity, and stable event IDs. Events whose four amounts cannot be
represented as one unambiguous input and opposite output are reported through
`onError` and do not stop other configured pairs.

## Verification

```bash
gradle :qtstreamx-dex-uniswap-v2:test
gradle :qtstreamx-canary:test -Pit --tests '*UniswapV2CaptureLiveIT'
```

The opt-in IT defaults to the credential-free dRPC public endpoints
`https://eth.drpc.org` and `wss://eth.drpc.org`. Override them with
`QTSTREAMX_EVM_HTTP_URL` and `QTSTREAMX_EVM_WS_URL`. Private provider URLs and
credentials remain runtime-only configuration and must not enter source or
artifacts.
