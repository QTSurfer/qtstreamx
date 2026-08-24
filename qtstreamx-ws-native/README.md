# qtstreamx-ws-native

JDK `java.net.http.WebSocket` implementations of the core `WebSocketClient`
interface, with no external WebSocket dependency.

`JdkWebSocketClient` is the normal text-frame client. `GzipAwareJdkWebSocketClient`
adds binary-frame gzip decompression for venues such as HTX. Both expose message,
close, and error callbacks before or after connection setup and provide explicit
connect timeouts.

This module owns the transport only. Exchange modules own protocol messages,
subscriptions, pings, and reconnect policy.

## Dependency and verification

```kotlin
implementation(project(":qtstreamx-ws-native"))
```

```bash
gradle :qtstreamx-ws-native:test
```
