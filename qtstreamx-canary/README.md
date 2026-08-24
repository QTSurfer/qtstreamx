# qtstreamx-canary

Offline WS capture + analyze tool. Records ticker / kline / funding-rate frames from any of
the qtstreamx exchange adapters into JSONL, then produces a Markdown comparison report
against a Binance reference capture.

Zero infra: no QDB, no NATS. The tool just opens a public WS from the host running it and
writes to disk. Safe to run from any machine with internet; ideal for validating new
adapters before wiring them into `publisher-x`.

Binance and Bybit are the current operational capture targets. The remaining exchange
keys are retained for explicit validation and comparative captures; they are not an
implicit production-routing recommendation.

DEX capture uses one shared normalized-trade session for Uniswap v2 and v3.
Versioned commands keep their pair/pool configuration explicit while recording
the same trade, ticker, kline, diagnostic, and summary formats.
Discovery-driven commands can instead resolve reviewed token pairs from factory
events before starting those same versioned streams. Their default candle is
`1s`; the explicit-descriptor commands retain their existing `1m` default.
The Java live tests reuse `KnownUniswapV2Markets`, `KnownUniswapV3Markets`, and
`KnownUniswapDeployments` so their reviewed examples do not duplicate market or
factory configuration.

The opt-in Ethereum mainnet integration tests may use dRPC's credential-free
public endpoints as the active bundle:

- HTTP: `https://eth.drpc.org`
- WebSocket: `wss://eth.drpc.org`

`QTSTREAMX_EVM_ACTIVE_HTTP_URL` and `QTSTREAMX_EVM_ACTIVE_WS_URL` override those
defaults. The passive bundle is intentionally mandatory through
`QTSTREAMX_EVM_PASSIVE_HTTP_URL` and `QTSTREAMX_EVM_PASSIVE_WS_URL`; live
resilience evidence must not silently route both aliases to the same default.
Private endpoints and credentials are runtime-only inputs; capture artifacts
and diagnostics never contain endpoint values.

The Robinhood Chain opt-in test defaults to QuickNode's credential-free
Robinhood documentation endpoint as its active bundle. Override it with
`QTSTREAMX_ROBINHOOD_ACTIVE_HTTP_URL` and
`QTSTREAMX_ROBINHOOD_ACTIVE_WS_URL`; the corresponding `PASSIVE` variables are
required. The demo endpoint is rate-limited evidence, not a production service.

## Capture

```
./gradlew :qtstreamx-canary:capture \
  -Pexchange=<key> \
  -Psymbols=<csv> \
  -Pduration=<minutes> \
  [-Pinterval=<native>] \
  [-Pout=<dir>]
```

Exchange keys:

| key | endpoint | funding | notes |
|-----|----------|---------|-------|
| `binance-spot` | stream.binance.com | — | reference |
| `binance-futures` | fstream.binance.com | 8h | |
| `bybit-spot` | stream.bybit.com | — | |
| `bybit-linear` | stream.bybit.com | 4h/8h | per-symbol interval |
| `okx` | ws.okx.com | 8h | unified spot+perp |
| `kraken-spot` | ws.kraken.com/v2 | — | `XBT` remapped to `BTC` |
| `kraken-futures` | futures.kraken.com/ws/v1 | 1h | hourly FR |
| `bitget-spot` | ws.bitget.com/v2 | — | batched klines |
| `bitget-futures` | ws.bitget.com/v2 | 8h | |
| `gateio-spot` | api.gateio.ws | — | batched candles |
| `gateio-futures` | fx-ws.gateio.ws | 8h | |
| `htx-spot` | api.huobi.pro | — | gzip frames |
| `htx-linear` | api.hbdm.com | 8h | two WS endpoints (market + notify) |

Symbols use CCXT notation: `BTC/USDT` for spot, `BTC/USDT:USDT` for linear perps.

Outputs two JSONL files under `<out>/`:

- `raw.jsonl` — every decoded WS frame (ping/pong + data), with direction and endpoint tag.
- `parsed.jsonl` — every Ticker/Kline/FundingRate emitted by the adapter.

## Analyze

```
./gradlew :qtstreamx-canary:analyze \
  -Preference=<binance-capture-dir> \
  -Ptarget=<other-capture-dir> \
  [-Preport=<path.md>]
```

Report sections:

- Window span (event count + seconds).
- Raw frame mix (inbound / outbound / lifecycle).
- Parsed events per kind × instrument, with per-minute rate and delta vs reference.
- Last-price drift (|tgt - ref| / ref) paired nearest-neighbour within ±2s: median, p95, max.
- Adapter yield (parsed ÷ inbound). >100% is normal for exchanges that batch multiple records
  per frame.

## Typical workflow

Run reference + target concurrently so the windows overlap (otherwise drift pairs = 0):

```
# terminal A — Binance baseline
./gradlew :qtstreamx-canary:capture -Pexchange=binance-spot \
  -Psymbols=BTC/USDT,ETH/USDT,SOL/USDT -Pduration=60 \
  -Pout=/tmp/canary/binance-spot &

# terminal B — exchange under test
./gradlew :qtstreamx-canary:capture -Pexchange=bitget-spot \
  -Psymbols=BTC/USDT,ETH/USDT,SOL/USDT -Pduration=60 \
  -Pout=/tmp/canary/bitget-spot &

wait

./gradlew :qtstreamx-canary:analyze \
  -Preference=/tmp/canary/binance-spot \
  -Ptarget=/tmp/canary/bitget-spot \
  -Preport=/tmp/canary/report-bitget.md
```

Repeat per exchange. ~100-300 MB per exchange per 1h at top-5 symbols.

## Durable active/passive Uniswap capture

The V2, V3, and discovery-driven canaries use two runtime HTTP/WebSocket
provider bundles. Startup runs bounded HTTP and WebSocket probes, compares
their safe heads, and opens only the active bundle. A transport-recovery
exhaustion replaces HTTP and WebSocket together with the passive bundle. The
new stream reuses the same provider-neutral stream identity and file checkpoint,
then completes HTTP recovery before reporting `LIVE`.

Endpoints and credentials are runtime-only:

```bash
export QTSTREAMX_EVM_ACTIVE_HTTP_URL='https://active.example/rpc'
export QTSTREAMX_EVM_ACTIVE_WS_URL='wss://active.example/ws'
export QTSTREAMX_EVM_PASSIVE_HTTP_URL='https://passive.example/rpc'
export QTSTREAMX_EVM_PASSIVE_WS_URL='wss://passive.example/ws'
```

Aliases default to `active` and `passive`; direct CLI users may override them
with `--active-alias` and `--passive-alias`. Aliases must be opaque lowercase
labels. Never put endpoint fragments, account names, or tokens in an alias.

An explicit initial block and stable stream key are mandatory. Reuse the same
`streamKey` and checkpoint directory after every restart or deployment; changing
either creates a different logical stream.

```bash
./gradlew :qtstreamx-canary:captureUniswapV3 \
  -Pnetwork=eip155:1 \
  -PstartBlock=21000000 \
  -PstreamKey=ethereum-uniswap-v3-usdc-weth \
  -PcheckpointDir=/var/lib/qtstreamx/checkpoints \
  -Pconfirmations=2 \
  -PdurationSeconds=300 \
  -Pout=/tmp/canary/uniswap-v3 \
  -Ppools='0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640|USDC|0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48|6|WETH|0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2|18|WETH|USDC|500'
```

V2 uses the same options with `captureUniswapV2`, `-Ppairs`, and this descriptor:

```text
pairAddress|token0Symbol|token0Address|token0Decimals|token1Symbol|token1Address|token1Decimals|base|quote
```

The V3 descriptor adds a final fee tier:

```text
poolAddress|token0Symbol|token0Address|token0Decimals|token1Symbol|token1Address|token1Decimals|base|quote|feeTier
```

### Discovery-driven capture

`captureDiscoveredUniswapV2` and `captureDiscoveredUniswapV3` require explicit
`factoryStartBlock`, `discoverySafeHead`, `captureStartBlock`, and `streamKey`.
The selected discovery reader must prove both `DISCOVERY_LOGS` and
`HISTORICAL_STATE`; live eligibility alone is insufficient. Capture still
requires both bundles to prove network, head/finality, live state, recovery
logs, and log subscription support.

```bash
./gradlew :qtstreamx-canary:captureDiscoveredUniswapV2 \
  -Pnetwork=eip155:1 \
  -Pfactory=0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f \
  -PfactoryStartBlock=10008355 \
  -PdiscoverySafeHead=10008355 \
  -PcaptureStartBlock=21000000 \
  -PstreamKey=ethereum-uniswap-v2-discovery \
  -PdiscoveryMaxScanBlocks=1 \
  -PquoteTokens=0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 \
  -PbaseTokens=0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 \
  -PdurationSeconds=60 \
  -Pout=/tmp/canary/uniswap-v2-discovery
```

The same implementation supports Robinhood Chain (`eip155:4663`). Its active
and passive bundles must independently serve the requested factory logs and
exact historical token state; an Ethereum provider cannot substitute for a
Robinhood provider.

### Operational limits and failure handling

- Each bundle's startup classification runs a purpose-minimal maximum of eight
  HTTP and four WebSocket requests. Their 30-second and 15-second budgets enforce
  one aggregate ceiling of 12 requests and 45 seconds, while retaining the
  default probe limits of 10,000 log blocks and 10,000 returned logs. Probe
  HTTP requests have
  zero retries.
- Runtime capture has no hedging. Only one bundle is active; `retries` is the
  per-operation retry ceiling inside that bundle. A switch therefore does not
  multiply one logical request across providers.
- `maxProviderLagBlocks` defaults to 2. Equal-height different hashes, a head
  outside that bound, wrong networks, missing capabilities, and stale evidence
  fail startup before capture.
- `maxReplayBlocks` defaults to 10,000 and `overlapBlocks` to 2. Exceeding the
  replay ceiling, stale heads, checkpoint mismatch/corruption, downstream ACK
  failure, or checkpoint write failure is terminal and does not switch or skip
  ahead.
- Recoverable V2/V3 normalization is also fail-closed. A malformed or
  unconfigured confirmed log rejects the whole raw batch, emits an endpoint-free
  error diagnostic, and leaves the checkpoint unchanged for operator review.
- Checkpoints are fsynced and atomically replaced. Back up the checkpoint
  directory with the stream configuration. Do not edit checkpoint JSON. A
  corrupt file fails closed and requires an operator decision, not deletion by
  automation.
- Logs and supervisor snapshots contain only network, stream key, aliases,
  enums, and counts. Capture diagnostics record exception classes without
  messages. URLs, RPC bodies, and credentials are excluded.
- The completion snapshot reports selected alias, startup head lag, cursor lag,
  recovery pages, retries, switches, gaps, reorgs, duplicate suppression, and
  typed terminal counts aggregated across the replaced and current streams.

The output directory contains `trades.ndjson`, `tickers.ndjson`,
`klines.ndjson`, `diagnostics.ndjson`, and `summary.json`. Discovery runs add
endpoint-free selection and rejection counts to the summary.

### Alerts and recovery runbook

Alert on the endpoint-free completion/runtime metrics, keyed by network and
stable stream key:

- **critical** on any terminal reason or `gaps > 0`; the stream is intentionally
  stopped and its checkpoint must remain unchanged;
- **warning** when cursor lag exceeds 25% of `maxReplayBlocks`, or when retries
  and switches increase repeatedly inside the operator's incident window;
- **warning** when selected-upstream head lag reaches
  `maxProviderLagBlocks`; a bundle outside that limit is ineligible; and
- **observe** reorg and duplicate-suppression deltas. A reorg is expected
  occasionally, but repeated growth requires canonical-provider comparison.

For a terminal capture:

1. Preserve the checkpoint directory and all artifacts. Do not delete or edit
   checkpoint JSON to make health green.
2. Run the bounded route probes for both opaque provider aliases and compare
   network, HTTP/WS safe head, required capabilities, and head lag. Keep URLs
   and provider response bodies out of the incident record.
3. Correct the failed runtime endpoint/quota or restore the durable downstream
   acknowledgement boundary. A gap beyond `maxReplayBlocks` needs an explicit
   operator decision to extend the bound or backfill; it must never skip to
   latest.
4. Restart with the same network, `streamKey`, checkpoint directory,
   confirmation depth, overlap, and market descriptors. Confirm the stream
   reports `LIVE`, cursor lag returns to zero, and recovery pages stop growing.
5. Compare the recovered `eventId` set with the source/canonical interval and
   confirm no duplicates before clearing the alert.

Default tests use deterministic local fixtures. Live proofs are opt-in and
require independent passive endpoint variables in addition to the active
variables above:

```bash
./gradlew :qtstreamx-canary:test -Pit --tests '*UniswapV2CaptureLiveIT'
./gradlew :qtstreamx-canary:test -Pit --tests '*UniswapV3CaptureLiveIT'
./gradlew :qtstreamx-canary:test -Pit --tests '*RobinhoodUniswapV3CaptureLiveIT'
```
