# qtstreamx-codec-json

Jackson implementation of the core `StreamCodec<T>` contract.

`JsonCodec<T>` serializes a value to UTF-8 JSON and deserializes it using the
requested runtime class. It is the general-purpose codec for normalized records
and configuration-shaped values; exchange WebSocket adapters parse their native
payloads directly and do not route hot-path frames through this codec.

## Usage

```java
StreamCodec<Ticker> codec = new JsonCodec<>();
byte[] encoded = codec.encode(ticker);
Ticker decoded = codec.decode(encoded, Ticker.class);
```

## Dependency and verification

```kotlin
implementation(project(":qtstreamx-codec-json"))
```

```bash
gradle :qtstreamx-codec-json:test
```
