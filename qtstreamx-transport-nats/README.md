# qtstreamx-transport-nats

NATS implementations of the core byte-payload transport interfaces.

`NatsPublisher` publishes bytes to a subject and `NatsSubscriber` registers a
callback for subject messages. Constructors accept either a NATS URL or an
already-owned `io.nats.client.Connection`; the latter lets an application share
connection lifecycle across QTStreamX components.

This module does not choose subject names, codecs, JetStream streams, retention,
or delivery semantics. Those are application-level concerns.

## Dependency and verification

```kotlin
implementation(project(":qtstreamx-transport-nats"))
```

```bash
gradle :qtstreamx-transport-nats:test
```
