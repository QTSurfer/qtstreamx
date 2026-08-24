# qtstreamx-chain-evm-rpc

Reusable, gap-free, confirmation-gated EVM event-log streaming for QTStreamX.
The `chain` namespace distinguishes blockchain transport and recovery from DEX
protocol semantics; Uniswap adapters consume this module, but do not own it.

The module combines WebSocket subscriptions (`logs` and `newHeads`) with HTTP
JSON-RPC catch-up. Callers receive ordered `EvmLog` values with canonical block
timestamps in epoch microseconds and do not manage request IDs, reconnect gaps,
provider range limits, duplicates, or provisional reorg events.

## Interface

```java
EvmLogStreamConfig config = new EvmLogStreamConfig(
        network,
        wsUrl,
        httpUrl,
        startBlock,
        confirmationDepth,
        2_000,
        Duration.ofSeconds(10),
        3);

try (EvmLogStream stream = new EvmRpcLogStream(config, JdkWebSocketClient::new)) {
    stream.onError(error -> log.error("EVM stream stopped", error));
    stream.start(
            new EvmLogFilter(Set.of(poolAddress), Set.of(eventTopic)),
            logEvent -> handle(logEvent));
}
```

`confirmationDepth` is the number of canonical descendant blocks required. At
depth `2`, a log in block N is released when canonical head N+2 is observed.
This is an application confirmation policy, not a claim of consensus finality.

## Bounded HTTP reads

Discovery and other read-only consumers use `EvmRpcReader`. Its interface
returns block numbers, typed raw logs, and opaque contract-call bytes without
exposing JSON or the HTTP transport:

```java
EvmRpcReaderConfig config = new EvmRpcReaderConfig(
        "eip155:1",
        httpUrl,
        2_000,
        Duration.ofSeconds(10),
        3);

EvmRpcReader reader = new EvmHttpRpcReader(config);
long head = reader.latestBlockNumber();

List<EvmRpcLog> logs = reader.logs(
        new EvmLogFilter(Set.of(factoryAddress), Set.of(pairCreatedTopic)),
        fromBlock,
        head);

byte[] symbolResult = reader.call(
        tokenAddress,
        symbolCallData,
        EvmBlockTag.number(head));

byte[] contractCode = reader.code(tokenAddress, EvmBlockTag.number(head));
```

`logs` uses an inclusive interval, pages it by `maxBlockRange`, bisects ranges
rejected by the provider, and returns immutable chain-ordered results. These
are raw `eth_getLogs` values: the caller owns its scan cursor, safe-head policy,
and handling of the `removed` flag. Use `EvmLogStream` when confirmation,
canonical timestamp enrichment, reconnect catch-up, and live delivery are
required.

Contract calls and bytecode reads require an explicit numeric, `latest`, `safe`,
or `finalized` `EvmBlockTag`. `code` wraps `eth_getCode` and returns an empty
array when no bytecode exists at that state. Call input and all result payloads
remain opaque bytes; ABI encoding and decoding belong to the consuming protocol
module.

Numeric historical calls require the configured provider to retain state at
that block. Some public RPCs can return old logs but reject `eth_call` against
the same old block. Factory-event replay that resolves token metadata therefore
needs an archive-capable HTTP endpoint; this module reports the bounded failure
and never substitutes current state silently.

## Provider capability probes

Provider product names are not capabilities. `EvmHttpRpcCapabilityProbe` and
`EvmWebSocketRpcCapabilityProbe` produce endpoint-free evidence for one opaque
upstream alias and CAIP-2 network. The HTTP probe measures the chain ID, head,
safe/finalized block tags, current call/code, one recent log range, one old log
range, and call/code at an exact old block. Old-log and historical-state
evidence remain independent in `EvmRpcCapabilityReport`.

```java
EvmRpcProbePlan plan = new EvmRpcProbePlan(
        new EvmLogFilter(Set.of(factoryAddress), Set.of(createdTopic)),
        recentFromBlock,
        recentToBlock,
        historicalFromBlock,
        historicalToBlock,
        tokenAddress,
        symbolCallData,
        historicalStateBlock);

EvmRpcCapabilityReport httpReport = new EvmHttpRpcCapabilityProbe(
        readerConfig,
        "ethereum-primary")
        .probe(plan, EvmRpcProbeBudget.safeDefaults(), EvmRpcProbeScope.FULL);

EvmRpcCapabilityReport webSocketReport = new EvmWebSocketRpcCapabilityProbe(
        new EvmRpcWebSocketProbeConfig(network, wsUrl, Duration.ofSeconds(5)),
        "ethereum-primary",
        JdkWebSocketClient::new)
        .probe(plan.logFilter(), new EvmRpcProbeBudget(
                4, Duration.ofSeconds(10), 1, 1));

EvmRpcCapabilityReport capabilities = httpReport.merge(webSocketReport);
```

The direct log observations issue one RPC request without normal reader
pagination or bisection, so a successful interval is a proven provider range.
`EvmRpcProbeScope.STARTUP` runs only six network, head/finality, and current
state operations. `ROUTE` uses eight purpose-minimal HTTP operations so an
application can combine them with the four WebSocket operations inside one
12-request provider budget. The general `FULL` scope retains all ten HTTP
operations.
The fixed HTTP sequence disables retries, batches, and hedges and applies a
three-second per-request ceiling, a cumulative returned-log ceiling, and a
16 MiB response-body ceiling. The WebSocket probe opens one connection and
sends four bounded requests: filtered `logs` and `newHeads` subscriptions,
`eth_chainId`, and `eth_getBlockByNumber("safe", false)`. Subscription support
is recorded only after valid non-empty acknowledgements; the probe does not wait
for a nondeterministic live event.

`EvmRpcProbeBudget` is enforced before contact and during the sequence. A
report can contain only the safe upstream alias, network, operation/category,
block numbers and hashes, counts, durations, timestamps, and numeric JSON-RPC
code. Endpoint values, provider text, result bytes, and subscription IDs have
no field in the public model. Ambiguous provider errors are `UNKNOWN`, not
misreported as a range or history capability.

These probes classify candidates; they do not route requests or replace HTTP
reconciliation, confirmations, durable cursors, or reorg checks. A WebSocket
acknowledgement proves subscription acceptance only. The separate WebSocket
network and safe-head observations let an application compare HTTP and
WebSocket evidence for the same bundle; agreement is never inferred from
endpoint ownership.

## Recovery contract

- Live logs and heads arrive over WebSocket.
- After disconnect, HTTP `eth_getLogs` fills from the last committed block to
  the current safe head before a new socket becomes active.
- Overlapping live/replayed logs are deduplicated by block hash, transaction
  hash, and log index.
- Provider-rejected ranges are bisected down to an accepted size.
- Block hashes are re-read before emission; orphaned and `removed` logs do not
  cross the public seam.
- Transport failures retry with bounded exponential backoff. JSON-RPC protocol
  errors remain terminal.

For a cursor that survives process replacement, supply a stable
`EvmLogStreamId`, an `EvmLogCheckpointStore`, and an `EvmRecoveryPolicy`, then
use the recoverable batch seam:

```java
EvmLogStreamId streamId = new EvmLogStreamId("eip155:1", "eth-usdc-weth-v3");
EvmRecoveryPolicy recovery = new EvmRecoveryPolicy(
        streamId, "provider-a", 12, 10_000);

try (EvmLogStream stream = new EvmRpcLogStream(
        config, JdkWebSocketClient::new, checkpointStore, recovery)) {
    stream.startRecoverable(filter, batch -> {
        persistEveryNormalizedEvent(batch);
        return EvmLogAcknowledgement.ACKNOWLEDGED;
    });
}
```

The checkpoint advances through the batch's canonical cursor only after the
handler acknowledges it. Rejected/failed delivery, checkpoint write failure,
canonical overlap disagreement, stale provider head, corrupt checkpoint, or a
gap beyond `maxReplayBlocks` fails closed. Recreate a stream for another
provider with the same `streamId` and checkpoint store; provider selection and
endpoint lifecycle remain an application boundary, not a protocol concern in
this module.

## Security

RPC URLs are runtime configuration and may contain provider credentials. The
configuration diagnostic representations always redact endpoints. Public
`EvmRpcException` values expose the protocol error code but discard
provider-controlled error text. WebSocket transport logs likewise omit the
endpoint, provider close reason, and exception message. Do not commit URLs or
keys to source, fixtures, goals, or build files.

## Verification

```bash
gradle :qtstreamx-chain-evm-rpc:test
```

An opt-in live smoke test requires these environment variables:

```text
QTSTREAMX_EVM_NETWORK
QTSTREAMX_EVM_WS_URL
QTSTREAMX_EVM_HTTP_URL
QTSTREAMX_EVM_START_BLOCK
QTSTREAMX_EVM_POOL_ADDRESS
QTSTREAMX_EVM_EVENT_TOPIC
```

Run it with:

```bash
gradle :qtstreamx-chain-evm-rpc:test -Pit
```
