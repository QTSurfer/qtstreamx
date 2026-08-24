# qtstreamx-codec-msgpack

Hand-tuned MessagePack codec for normalized `Ticker` records.

`MsgpackTickerCodec` uses a fixed field order and `msgpack-core` directly. It
avoids reflection and is intended for compact, stable ticker transport where
both producer and consumer use QTStreamX. Its payload layout is an internal
compatibility contract: use `JsonCodec` when arbitrary types or human-readable
messages are required.

## Dependency and verification

```kotlin
implementation(project(":qtstreamx-codec-msgpack"))
```

```bash
gradle :qtstreamx-codec-msgpack:test
```
