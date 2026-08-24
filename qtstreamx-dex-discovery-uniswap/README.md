# qtstreamx-dex-discovery-uniswap

Typed, read-only Uniswap v2 and v3 factory discovery over
`qtstreamx-chain-evm-rpc`.

The module scans one explicitly configured factory through an explicit safe
block, resolves ERC-20 metadata, applies caller-owned address-based orientation,
and publishes immutable snapshots of `UniswapV2Pair` or `UniswapV3Pool`.
It never guesses base/quote from token symbols.

## Supported factory events

- V2 decodes the canonical
  [`PairCreated(address,address,address,uint256)` event](https://github.com/Uniswap/v2-core/blob/master/contracts/interfaces/IUniswapV2Factory.sol).
- V3 decodes the canonical
  [`PoolCreated(address,address,uint24,int24,address)` event](https://github.com/Uniswap/v3-core/blob/main/contracts/interfaces/IUniswapV3Factory.sol),
  including the indexed fee tier.

Indexed addresses and fees come from topics; non-indexed values come from the
event data according to the
[Solidity ABI event encoding](https://docs.soliditylang.org/en/latest/abi-spec.html#events).
The decoder requires canonical word lengths and padding before constructing the
existing versioned descriptors.

## Public seam

```java
import com.qtsurfer.qtstreamx.dex.uniswap.v3.UniswapV3Pool;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

// Keep these addresses in a reviewed per-network deployment registry.
String wethAddress = "0x...";
String usdcAddress = "0x...";

UniswapFactoryScan scan = new UniswapFactoryScan(
        "eip155:1",
        "0x00000000000000000000000000000000000000f0",
        12_345_678L);

AddressBasedUniswapPairOrientation orientation =
        new AddressBasedUniswapPairOrientation(Map.of(
                "eip155:1",
                new UniswapNetworkTokenPolicy(
                        Set.of(usdcAddress),
                        Set.of(wethAddress))));

UniswapDiscoveryPolicy policy = new UniswapDiscoveryPolicy(
        orientation,
        new UniswapDiscoveryLimits(
                50_000, // maximum blocks scanned per refresh
                2_000,  // maximum ERC-20 calls per refresh
                1_000,  // maximum retained factory candidates
                100),   // maximum descriptors published
        OptionalLong.of(7_200)); // require a recent canonical Swap

MarketDiscovery<UniswapV3Pool> discovery = new UniswapV3MarketDiscovery(
        scan,
        evmRpcReader,
        policy,
        failure -> diagnostics.accept(failure));

Set<UniswapV3Pool> pools = discovery.refresh(explicitSafeHead)
        .toCompletableFuture()
        .join();
long resumeFrom = discovery.nextBlock();
```

Reviewed examples can start from a known deployment without repeating factory,
deployment block, or the narrow example token policy:

```java
UniswapDeployment deployment = KnownUniswapDeployments.ROBINHOOD_MAINNET_V3;
UniswapFactoryScan scan = deployment.factoryScan();
AddressBasedUniswapPairOrientation orientation = deployment.orientation();
```

The supplied entries cover Ethereum mainnet V2/V3 and Robinhood mainnet V3.
Their scan cursors are factory deployment blocks; a full historical refresh can
span millions of blocks and must still use explicit limits, pagination, and a
persisted resume cursor. `KnownUniswapDeployments` is not a registry update
service, activity ranking, or automatic subscription policy. Callers may replace
its deliberately narrow example token policy with their own reviewed addresses.

### On-chain lookup for explorers and CLIs

`UniswapOnChainLookup` supports targeted, trust-neutral exploration without a
Blockscout, subgraph, Uniswap REST API, or scraped web page. It reads contract
code and ERC-20 metadata through JSON-RPC, queries V2 `getPair` or V3 `getPool`,
and validates every non-zero market against its reviewed factory:

```java
UniswapOnChainLookup lookup = new UniswapOnChainLookup(evmRpcReader);
Erc20TokenInspection token = lookup.inspectToken(
        "eip155:4663",
        "0xc0a9531cae8bea6268bd19efec1dd205830cae2a",
        EvmBlockTag.latest());

List<UniswapV3PoolInspection> pools = lookup.findV3Pools(
        KnownUniswapDeployments.ROBINHOOD_MAINNET_V3,
        token.address(),
        Set.of("0x0bd7d308f8e1639fab988df18a8011f41eacad73"),
        Set.of(100, 500, 3_000, 10_000),
        EvmBlockTag.latest());
```

`name`, `symbol`, and `decimals` are untrusted contract claims. Metadata fields
that revert or return malformed data remain explicitly unavailable without
hiding transport exhaustion. The decoder accepts canonical dynamic strings and
the common fixed-byte form, bounds text at 64 bytes, requires valid UTF-8, and
rejects control characters before values reach a terminal.

`findV2Pairs` and `findV3Pools` require explicit bounded counterparties and fee
tiers. A zero factory result means only that the exact V2/V3 combination is
absent; it says nothing about V4, another factory, or another DEX. Pair/pool
inspection reports protocol-native reserves or active liquidity, never fiat TVL.

`marketFactory` reads the factory address claimed by a pair or pool so a caller
can perform bounded V2/V3 shape inference. The result is deliberately
trust-neutral: callers must validate the market through that factory and compare
the deployment with their reviewed catalogue before marking it reviewed.

For navigation without token calls, `listFactoryMarkets` decodes a maximum of
1,000 canonical creation events over at most 100,000 inclusive blocks and
retains block, transaction, and log provenance. It does not orient, rank,
authenticate, or automatically subscribe to the returned addresses.

The installable [DEX discovery CLI](../qtstreamx-dex-discovery-cli/README.md)
uses only these public typed operations. It adds no ABI decoder or raw JSON-RPC
construction of its own, and keeps explicit unreviewed factories visibly
separate from `KnownUniswapDeployments`.

For a matching `PoolCreated` event, `pools` contains a concrete descriptor
rather than a generic nullable record:

```text
network=eip155:1
address=0x...pool
token0=WETH (18)
token1=USDC (6)
instrument=WETH/USDC
feeTier=500
nextBlock=12_345_679
```

The equivalent V2 result is an `UniswapV2Pair` with the same network, ordered
token metadata, logical instrument, and pair contract address. A second pool
for `WETH/USDC` remains a second descriptor because its native contract (and,
for V3, fee tier) is part of its identity.

### Small Ethereum mainnet sample

The following contracts were verified by calling `token0()`, `token1()`, and,
for V3, `fee()` on 2026-08-08. They illustrate why discovery retains native
contract identity even when an orientation maps each one to `WETH/USDC`.

| Version | Contract | Factory token order | Fee tier | Possible logical instrument |
|---|---|---|---:|---|
| V2 pair | [`0xb4e16…c9dc`](https://etherscan.io/address/0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc) | USDC / WETH | — | WETH/USDC |
| V3 pool | [`0x88e6a…5640`](https://etherscan.io/address/0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640) | USDC / WETH | 500 (0.05%) | WETH/USDC |
| V3 pool | [`0x8ad599…e6d8`](https://etherscan.io/address/0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8) | USDC / WETH | 3,000 (0.3%) | WETH/USDC |

This is a concise, reproducible discovery example, not an allowlist, liquidity
recommendation, activity ranking, or default subscription set. A reviewed
`UniswapNetworkTokenPolicy` decides which addresses are consumable on each
network.

### Multichain live verification

The unchanged V3 discovery path is also verified on Robinhood Chain mainnet
(`eip155:4663`). It discovers the canonical WETH/USDG fee-100 pool from block
`1506281` using the factory and token addresses published by the
[Uniswap SDK](https://github.com/Uniswap/sdks/blob/main/sdks/sdk-core/src/addresses.ts)
and [Robinhood Chain](https://docs.robinhood.com/chain/contracts/), then feeds
the descriptor into the existing V3 stream. See the
[canary instructions](../qtstreamx-canary/README.md#robinhood-chain-mainnet)
for reproducible configuration and provider requirements.

Historical metadata resolution uses `eth_call` at the explicit safe block. A
provider for historical discovery must therefore retain archive state; current-
state-only public RPC endpoints are insufficient for replaying an old factory
event even when `eth_getLogs` itself succeeds.

Use `resumeFrom` as the `startBlock` of a replacement `UniswapFactoryScan` to
resume after a restart. A refresh scans the inclusive range
`nextBlock..safeHead`; an older safe head is a no-op.

`refresh` performs the blocking `EvmRpcReader` operations on the calling thread
and returns a completed or failed `CompletionStage`. Callers that require
asynchronous scheduling must choose and own their executor rather than relying
on a hidden module thread pool.

## Snapshot and failure semantics

- Successful refreshes retain oriented factory candidates, append new contracts
  in chain order, deduplicate repeated events by native market address, apply
  current activity selection, advance the cursor, and atomically publish a new
  immutable snapshot.
- An inactive candidate remains retained and can enter a later snapshot after a
  canonical Swap appears in the configured lookback window. It leaves the
  published snapshot again when that activity ages out.
- A scan-level, activity-query, or hard-limit failure preserves both the
  previous snapshot and cursor.
- A malformed event, token metadata failure, rejected orientation, or invalid
  descriptor excludes only that market. `UniswapDiscoveryListener` receives a
  safe classification, block number, and transaction hash.
- Diagnostics never include RPC endpoints, provider bodies, token return data,
  or exception messages.
- Token metadata is cached per discovery instance and contract address. Calls
  use `symbol()` and `decimals()` at the refresh safe block.

## Trust and operational limits

Factory address, network, initial cursor, safe head, address policy, activity
window, and limits are caller inputs. Configure factory addresses, deployment
blocks, and token allowlists only from reviewed per-network registries. Token
symbols are display metadata; network plus contract address is the identity and
authorization input. Exactly one token must be a configured quote and the other
must be an allowed base; both-quote, neither-quote, unknown-network, and spoofed-
symbol pairs are rejected.

`AddressBasedUniswapPairOrientation` performs that address decision directly
from the indexed factory topics before resolving ERC-20 metadata. Untrusted
events therefore consume neither `symbol()` nor `decimals()` calls. Custom
`UniswapPairOrientation` implementations retain a permissive preselection
default and may make their final decision after metadata is available.

The supplied `EvmRpcReader` owns provider range pagination, bisection, retry,
and endpoint redaction. `UniswapDiscoveryLimits` bounds each refresh range,
ERC-20 metadata calls, retained candidates, and published descriptors. An
optional `activityLookbackBlocks` requires at least one canonical V2/V3 Swap in
that inclusive recent window. Activity is selection evidence, not a liquidity
or volume ranking. The orientation-only compatibility constructors use
`UniswapDiscoveryLimits.safeDefaults()` and disable activity selection; normal
runtime configuration should provide an explicit `UniswapDiscoveryPolicy`.

Known deployment presets merely provide reviewed initial values for those
inputs. They do not relax any discovery limit, safe-head, metadata, orientation,
activity, or failure-isolation rule.

The ERC-20 decoder accepts canonical dynamic or fixed-byte text and a single ABI
word for `decimals()`. Missing, reverted, oversized, invalid UTF-8, or otherwise
non-canonical metadata isolates the affected market.

## Intended extension path

Discovery is the catalogue input, not a pool-explorer product. The intended
QTStreamX follow-up is limited to selected pools: enrich their protocol-native
metadata, start the existing confirmed V2/V3 swap streams, treat accepted swaps
as ticks, aggregate event-time 1-second `MarketKline` candles, and publish the
normalized output to NATS for downstream QuestDB persistence.

That follow-up must keep selection explicit and bounded. It is not a mandate to
rank pools by TVL, price unknown tokens in fiat, index historical volume, or
build a search UI; those are separate applications if ever needed.

## Verification

```bash
gradle :qtstreamx-dex-discovery-uniswap:test
gradle :qtstreamx-dex-discovery-uniswap:test \
  --tests 'com.qtsurfer.qtstreamx.dex.discovery.uniswap.consumer.PublicUniswapDiscoveryTest'
gradle :qtstreamx-dex-discovery-uniswap:javadoc
```

Default tests use deterministic ABI fixtures and no live provider. The opt-in
on-chain lookup proof requires `QTSTREAMX_ROBINHOOD_HTTP_URL` and runs with:

```bash
gradle :qtstreamx-dex-discovery-uniswap:test -Pit \
  --tests '*UniswapOnChainLookupLiveIT'
```
