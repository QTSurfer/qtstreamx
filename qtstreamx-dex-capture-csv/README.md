# QTStreamX DEX CSV capture

`qtstreamx-dex-capture-csv` persists normalized `MarketTrade` batches to a
durable, append-only event CSV plus a sibling metadata CSV. It is a core-only library: it has no JSON-RPC,
Uniswap ABI, discovery, CLI, or provider configuration dependency.

## Schema

The event CSV is UTF-8 without a BOM and contains only changing event fields:

```text
event_id,timestamp_us,price,base_amount,quote_amount,side
```

The sibling metadata CSV contains one capture-level row:

```text
venue,network,contract,instrument,date_from_us,date_to_us
```

`event_id` is the replay-idempotency key; `timestamp_us` is source epoch microseconds. Decimal fields preserve
`BigDecimal.toPlainString()` output, so they never use locale formatting or
scientific notation.

## Usage

```java
try (CsvMarketTradeSink sink = new CsvMarketTradeSink(Path.of("captures/pool.csv"), market)) {
    stream.startRecoverable(sink);
}
```

The sink writes and forces a batch before it returns `ACKNOWLEDGED`. On reopen
it validates the schema and existing records, rebuilds the event-ID set, and
skips replayed trades. A malformed, incompatible, or duplicate existing record
fails closed; a failed append poisons the sink, so callers must reopen it to
reconcile any partial append safely.

The caller owns stream lifetime, contract selection, and output-path retention.
Endpoints and provider diagnostics are not stored in the CSV.
