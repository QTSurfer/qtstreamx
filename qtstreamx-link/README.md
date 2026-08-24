# qtstreamx-link

Managed WebSocket connection pools for CEX market-data streams.

The module turns one exchange instrument universe into a bounded set of
`StreamClient` connections. It partitions subscriptions according to exchange
limits, refreshes the universe, reconnects dropped or silent links, and fans
normalized ticker, kline, and funding-rate events into application callbacks.

## Responsibilities

- Refresh an `InstrumentsCache` and partition its instruments across links.
- Respect per-connection stream capacity and per-IP connection-rate limits.
- Stagger initial connections and reconnects to avoid connection bursts.
- Resubscribe after disconnects with bounded exponential backoff and jitter.
- Renew sockets before an exchange-enforced connection lifetime expires.
- Reconcile added and removed instruments without rebuilding healthy links.
- Detect connected-but-silent links and reconnect only the affected buckets.
- Expose lifecycle callbacks and lightweight liveness counters for monitoring.

`LinkManager` is the normal application entry point. `Link` owns one
`StreamClient` and is public for lower-level integrations and diagnostics.

## Main types

| Type | Purpose |
|---|---|
| `LinkManager` | Owns a connection pool for one exchange and market. |
| `Link` | Owns one WebSocket client, its instruments, and reconnect lifecycle. |
| `InstrumentsCache` | Supplies an atomically refreshed CEX instrument set. |
| `WsLimits` | Describes exchange connection, stream, message, and lifetime limits. |
| `LinkPartitioner` | Converts instruments and subscriptions into bounded link buckets. |
| `ConnectionRateLimiter` | Shares one adaptive connection-rate gate across a manager. |
| `LinkLifecycleListener` | Reports connects, disconnects, failed reconnects, and silence. |

## Basic usage

Exchange modules provide the concrete `StreamClient` and `InstrumentsCache`.
Configure callbacks before calling `start()`:

```java
InstrumentsCache instruments = createInstrumentsCache();

LinkManager.Configuration config = new LinkManager.Configuration(
    "binance-spot",
    WsLimits.binanceSpot(),
    Set.of(Link.Subscription.TICKER, Link.Subscription.KLINE),
    "1m",
    500,
    2,
    Duration.ofMinutes(5),
    Duration.ofMillis(250),
    assignedInstruments -> createStreamClient(),
    LinkLifecycleListener.NOOP,
    true,
    Duration.ofMinutes(2));

try (LinkManager manager = new LinkManager(config, instruments)) {
    manager
        .onTicker(this::publishTicker)
        .onKline(this::publishKline);

    manager.start().toCompletableFuture().join();
    runUntilShutdown();
}
```

The `streamClientFactory` receives the instruments assigned to its link. The
manager then calls the client's normal subscribe methods for every configured
event type and instrument.

`targetStreamsPerLink <= 0` uses one stream below the exchange limit.
`streamsPerInstrument <= 0` defaults to one. Count every selected subscription
that consumes a distinct exchange stream when configuring
`streamsPerInstrument`.

## Discovery and refresh

`start()` performs the first `InstrumentsCache.refresh()`, partitions the
result, and schedules link creation with the configured startup jitter. Later
refreshes are diffed against active subscriptions:

- additions use spare capacity or create an overflow link;
- removals unsubscribe from the owning link;
- a failed refresh leaves the current subscriptions running.

`InstrumentsCache` intentionally models a set of normalized CEX instruments.
It does not retain chain, protocol, contract, token, or fee-tier identity and
must not be used as the descriptor model for DEX pool discovery.

## Recovery and liveness

Each link recreates its `StreamClient` after a disconnect and resubscribes its
complete instrument bucket. One manager-level rate limiter paces connections
for the exchange and adapts after provider backpressure signals.

An open socket can stop delivering market data without disconnecting. The
silence watchdog detects that condition using per-link message timestamps and
reconnects only silent links. Configure `linkSilenceThreshold` according to the
slowest expected subscription:

- `null` selects the two-minute default;
- `Duration.ZERO` disables the watchdog;
- funding-rate-only links normally need a longer explicit threshold.

Useful monitoring methods include `connectedLinkCount()`, `liveLinkCount()`,
`silentLinks()`, `instrumentsOnSilentLinks()`, `messagesReceived()`, and
`lastEventMicros()`. `reconnectAll()` is available for an explicit pool-wide
recovery action.

Implement `LinkLifecycleListener` for metrics or structured operational events.
Listeners and market-data consumers must be thread-safe because callbacks may
arrive from scheduler and WebSocket reader threads.

## Link grouping

Grouping is deterministic by default. With `randomizeLinkGrouping=true`, the
manager derives a stable per-instance shuffle seed from the exchange key,
hostname, and optional `QTSTREAMX_LINK_SEED_SALT`. Supply a deployment-unique
salt when multiple processes share the same hostname; otherwise they can place
the same instruments on corresponding links and correlate failures.

## Ownership and limits

- One manager owns one exchange/market group and its scheduler.
- `close()` stops refresh/watchdog tasks, closes every link, and shuts down the
  scheduler, including a scheduler supplied through the testable constructor.
- Call event-sink registration methods before `start()`.
- The module does not decode exchange payloads, discover DEX pools, publish to
  NATS, persist checkpoints, or deduplicate market events.
- `Link` subscription mutation is coordinated by `LinkManager`; callers should
  not mutate one link concurrently from multiple threads.

## Dependency

```kotlin
dependencies {
    implementation(project(":qtstreamx-link"))
}
```

Published artifact coordinates use
`com.qtsurfer.qtstreamx:qtstreamx-link:<version>`.

## Verification

```bash
gradle :qtstreamx-link:test
```

The deterministic suite covers partitioning, exchange limits, adaptive
connection pacing, reconnect backoff, preemptive renewal, dynamic subscription
changes, lifecycle callbacks, randomized grouping, and silent-link recovery.
