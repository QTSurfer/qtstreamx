# qtstreamx-ws-javaws

`WebSocketClient` adapter backed by TooTallNate's Java-WebSocket library.

`JavaWsClient` offers the same core callback and lifecycle surface as the JDK
implementation. Prefer `qtstreamx-ws-native` for the zero-dependency default;
use this module only where Java-WebSocket-specific behaviour or compatibility is
needed by an embedding application.

## Dependency and verification

```kotlin
implementation(project(":qtstreamx-ws-javaws"))
```

```bash
gradle :qtstreamx-ws-javaws:test
```
