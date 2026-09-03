# qtstreamx-dex-discovery-cli

Installable, read-only terminal shell for DEX market discovery. Protocol support
is provided by explicit adapters; the first adapter supports bounded Uniswap
V2/V3 discovery over EVM JSON-RPC. No explorer REST API, subgraph, wallet,
signing, or transaction support is required.

## Install and run

From the repository root:

```bash
./gradlew :qtstreamx-dex-discovery-cli:installDist
export QTSTREAMX_DEX_DISCOVERY=./qtstreamx-dex-discovery-cli/build/install/qtstreamx-dex-discovery/bin/qtstreamx-dex-discovery
"$QTSTREAMX_DEX_DISCOVERY" networks
```

With no command, the executable opens a numbered menu. Every menu operation has
the same scriptable command underneath:

```text
qtstreamx-dex-discovery networks
qtstreamx-dex-discovery protocols
qtstreamx-dex-discovery uniswap markets --network <ethereum|robinhood>
qtstreamx-dex-discovery uniswap token --network <network> <token-address>
qtstreamx-dex-discovery uniswap pool --network <network> <pool-address>
qtstreamx-dex-discovery uniswap capture --network <network> --version <v2|v3> (--start-block <block>|--start-date <ISO-8601-UTC>) --out <events.csv> <reviewed-contract>
qtstreamx-dex-discovery uniswap format --source <events.csv> --out <ticks.csv>
qtstreamx-dex-discovery uniswap scan --network <network> --version <v2|v3> --from <block> --to <block>
qtstreamx-dex-discovery uniswap search --network <network> --version <v2|v3> --query <text> --from <block> --to <block>
```

`--http-url` overrides `QTSTREAMX_EVM_HTTP_URL`. On-chain commands preflight
that configuration before any RPC request and report whether it came from the
option or environment variable; the URL itself is redacted from output and
errors. The CLI deliberately has no built-in provider endpoint: choose a
provider appropriate for the network and your operational requirements.
`protocols`, `networks`, `uniswap markets`, and `help` need no RPC endpoint.

### Public Ethereum RPCs for a quick CLI check

These public, unauthenticated Ethereum Mainnet endpoints are useful for an
exploratory lookup or WebSocket check. They returned `eth_chainId = 0x1` when
this document was updated, but are not an availability, throughput,
historical-data, or WebSocket-support guarantee. They are not suitable defaults
and must not be used as either bundle of a durable capture.

| Provider | Public HTTPS endpoint | Public WSS endpoint | Documentation |
|---|---|---|---|
| dRPC | `https://eth.drpc.org` | `wss://eth.drpc.org` | [dRPC](https://drpc.org/docs/ethereum-api) |
| Alchemy | `https://eth-mainnet.g.alchemy.com/public` | `wss://eth-mainnet.g.alchemy.com/v2/demo` | [Alchemy](https://www.alchemy.com/docs/reference/best-practices-for-using-websockets-in-web3) |
| PublicNode | `https://ethereum-rpc.publicnode.com` | `wss://ethereum-rpc.publicnode.com` | [PublicNode](https://publicnode.com/) |

After the installation snippet above, copy and paste this to configure both
transports and inspect one pool. The lookup itself uses HTTP; the exported WSS
URL is ready for a later capture command.

```bash
export QTSTREAMX_EVM_HTTP_URL=https://eth.drpc.org
export QTSTREAMX_EVM_WS_URL=wss://eth.drpc.org

"$QTSTREAMX_DEX_DISCOVERY" uniswap pool --network ethereum \
  0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640
```

For `uniswap capture`, configure active and passive bundles from independent
failure domains. A public endpoint is useful for an exploratory lookup, but is
not an operational substitute for the two provider bundles required by durable
capture. Keep every private or credentialed URL and every key in CLI options or
environment variables; never put them in source, documentation examples, or
committed configuration.

## Convert a capture for QTSurfer

After capture ends, convert its raw event file in a separate pass. The derived file is directly
uploadable as a QTSurfer ticker dataset and preserves epoch microseconds and plain decimal values.

```bash
"$QTSTREAMX_DEX_DISCOVERY" uniswap format \
  --source /data/weth-usdc-v3.csv \
  --out /data/weth-usdc-qtsurfer-ticks.csv
```

It writes `timestamp,close,volume,quoteVolume`; the raw `event_id` and `side` remain in the source capture.

## Durable one-contract CSV capture

`uniswap capture` records normalized, confirmed swaps from exactly one reviewed
Uniswap V2 pair or V3 pool. It is deliberately not a discovery subscription:
the contract address must already be in QTStreamX's reviewed market catalogue
for the selected network and protocol version.

The command requires an active and passive provider bundle. Each bundle has an
HTTP endpoint for reconciliation and a WebSocket endpoint for live logs. The
active bundle is used first; after its typed recovery is exhausted, capture
switches to the passive bundle without advancing the durable cursor early.

```bash
export QTSTREAMX_EVM_HTTP_URL=https://active-http.example
export QTSTREAMX_EVM_WS_URL=wss://active-ws.example
export QTSTREAMX_EVM_PASSIVE_HTTP_URL=https://passive-http.example
export QTSTREAMX_EVM_PASSIVE_WS_URL=wss://passive-ws.example

"$QTSTREAMX_DEX_DISCOVERY" \
  uniswap capture \
  --network ethereum \
  --version v3 \
  --start-block 24000000 \
  --duration-seconds 300 \
  --out /data/weth-usdc-v3.csv \
  0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640
```

The endpoint variables may instead be provided as `--http-url`, `--ws-url`,
`--passive-http-url`, and `--passive-ws-url`. They are runtime-only: they are
never written into either CSV, diagnostics, fixtures, or goal records; CLI
output may name the supplying option or environment variable, never its value.
Capture validates all four endpoints before opening any RPC connection and
reports each option/environment origin with endpoint values redacted.
`--duration-seconds` defaults to 300. Advanced recovery controls
(`--confirmations`, `--max-block-range`, `--timeout-seconds`, `--retries`,
`--overlap-blocks`, `--max-replay-blocks`, and `--max-provider-lag-blocks`)
use the same confirmed-log semantics as the canary. `--checkpoint-dir` may be
used to choose the durable cursor directory; otherwise it is a sibling of the
event file.

Choose exactly one capture origin: `--start-block` for an explicit chain cursor,
or `--start-date` for UX-friendly ISO-8601 UTC text ending in `Z` (for example,
`2026-08-23T09:30:00Z`). The CLI resolves the latter to the first canonical
active-provider block at or after that UTC instant before it opens capture.

The command creates the explicit parent directory for `--out` when needed; it
never truncates an existing capture. Its event file is UTF-8 CSV with LF line
endings and this exact header:

```text
event_id,timestamp_us,price,base_amount,quote_amount,side
```

Decimals are emitted with `BigDecimal.toPlainString()` and timestamps are epoch
microseconds. The sibling `<events.csv>.metadata.csv` has one row describing
the stable capture context and observed time range:

```text
venue,network,contract,instrument,date_from_us,date_to_us
```

Rows are flushed and fsynced before their source batch is acknowledged. On
restart or provider overlap, existing event IDs are rebuilt and replayed rows
are skipped; malformed/incompatible files fail closed. A successful command
therefore indicates the requested bounded capture interval ended without a
persistence error, not that a particular number of swaps exists. Invalid,
unreviewed contracts are rejected before any provider is contacted.

## Bounded RUBY example on Robinhood Chain

RUBY is emitted by an unreviewed Uniswap-V2-shaped factory, so the factory must
be supplied explicitly. This reproducible example scans only its creation block
and resolves token metadata exclusively with JSON-RPC:

```bash
export QTSTREAMX_EVM_HTTP_URL=https://rpc.mainnet.chain.robinhood.com

"$QTSTREAMX_DEX_DISCOVERY" uniswap search \
  --network robinhood \
  --version v2 \
  --factory 0x8bceaa40b9acdfaedf85adf4ff01f5ad6517937f \
  --query RUBY \
  --from 30898756 \
  --to 30898756 \
  --output json
```

At block `30898756`, this finds token
`0xc0a9531cae8bea6268bd19efec1dd205830cae2a` (`RUBY`, contract-reported name
`The Reddit Dog`) in pair `0x3f653bc5425cd5e4f13dab44e4b4cef112c6767c`
with WETH. The result remains `reviewed: false`: a successful ABI read, token
name, or symbol does not establish protocol provenance or canonical token
identity.

Inspect the pair and its current protocol-native reserves with:

```bash
"$QTSTREAMX_DEX_DISCOVERY" uniswap pool \
  --network robinhood \
  0x3f653bc5425cd5e4f13dab44e4b4cef112c6767c \
  --output json
```

## Discovery and safety limits

- `scan` and `search` require an inclusive block range of at most 100,000
  blocks. They return at most `--limit` markets (default 100, maximum 1,000).
- `search` inspects at most `--token-limit` unique token contracts (default 200,
  maximum 2,000). It is a local projection of an explicit factory scan, not a
  chain-wide name index.
- Token names and symbols are untrusted display metadata. Address identity,
  factory address, network, protocol version, and review status remain in the
  output.
- Pool inference probes V2 and V3 contract shapes and validates the pool's own
  factory. A factory absent from the reviewed catalogue stays unreviewed.
- `instrument` is present only when the network's address policy can establish
  logical base/quote orientation; otherwise `orientation` is `unavailable`.
- V3 `currentLiquidity` and V2 reserves are native protocol state, not fiat TVL.
- A missing result covers only the requested V2/V3 combinations. It does not
  prove absence from Uniswap V4, another factory, or another DEX.

Use `--factory-start` when an explicitly supplied factory should reject a range
before its known deployment block. Use `--block latest|safe|finalized|<number>`
for contract reads.

## Output and exit contract

Human-readable output is the default. `--output json` emits one object with
`schemaVersion: 1`, `protocol`, `command`, `status`, `data`, and `messages`.
`protocol` is `uniswap` for adapter commands and `null` for shell-wide commands.
Output contains no prompts, ANSI controls, endpoint value, or
provider-controlled error body.

| Exit | Meaning |
|---:|---|
| 0 | Successful command |
| 2 | Invalid command or input |
| 3 | RPC provider unavailable |
| 4 | Bounded lookup or contract validation failure |
| 5 | Valid lookup with no supported market match |
| 130 | Interrupted operation |

The CLI exports descriptors and can durably capture one explicitly reviewed
market to local CSV. It never publishes data, captures a discovery result
automatically, signs transactions, or manages wallets.

## Verification

```bash
./gradlew :qtstreamx-dex-discovery-cli:test
./gradlew :qtstreamx-dex-discovery-cli:javadoc
```

The default test task builds the installable distribution and executes its real
launcher as a child process. Black-box tests cover protocol discovery, stable
JSON, menu/command equivalence, missing protocol prefixes, and capture input
validation without endpoint leakage; deterministic
application and adapter tests cover EOF, interruption, provider and lookup
errors, endpoint redaction, metadata limits, V2 inference, and bounded search.
They require no live provider.

The Robinhood examples above are opt-in live smoke commands. Supply only a
runtime `QTSTREAMX_EVM_HTTP_URL`; never commit private provider endpoints or
credentials.

## GraalVM Native Image

The CLI can be compiled as a platform-specific native executable. Native Image
is opt-in: JVM builds and the installable JVM distribution remain the default.
Install a GraalVM distribution with Native Image for the same operating system
and CPU architecture as the desired executable, then ensure that its `java` and
`native-image` commands are first on `PATH`.

If `GRAALVM_HOME` is set, it takes precedence over `PATH`; it must therefore
refer to that same matching GraalVM installation (or be unset).

```bash
java -version
native-image --version
./gradlew :qtstreamx-dex-discovery-cli:nativeCompile
```

The resulting executable is:

```text
qtstreamx-dex-discovery-cli/build/native/nativeCompile/qtstreamx-dex-discovery
```

Run deterministic endpoint-free native verification with:

```bash
./gradlew :qtstreamx-dex-discovery-cli:nativeTest
./gradlew :qtstreamx-dex-discovery-cli:nativeSmoke
```

`nativeTest` executes the native-compatible project tests as native binaries,
including the durable checkpoint JSON round trip. JVM-distribution and
build-resource checks remain in the ordinary JVM test suite. `nativeSmoke`
builds the main executable, runs `protocols --output json`, and checks invalid
capture configuration and endpoint redaction without contacting a provider.
Both tasks require the same matching GraalVM installation as `nativeCompile`.

Native executables are not cross-platform artifacts: build Linux releases on
Linux and macOS releases on macOS, with the target CPU architecture. This
repository does not ship a container or cross-compilation toolchain. The native
image has the same runtime limits as the JVM CLI: it is read-only, needs
runtime-only provider endpoints for on-chain operations, and requires active
and passive HTTP+WebSocket provider bundles for durable capture. Do not place
endpoints or credentials in Native Image configuration, build arguments, or
artifacts.

The `native` CI workflow builds, native-tests, and smoke-tests the CLI on a
matching runner per platform — Linux AMD64, macOS ARM64, and Windows AMD64 —
on every change to the CLI dependency graph, and the release workflow reuses
that exact matrix so a tagged release ships only what CI already verified.
Each platform's artifact is named `qtstreamx-dex-discovery-<target>`. A new
platform needs its own runner and passing smoke evidence before it is added to
the matrix.
