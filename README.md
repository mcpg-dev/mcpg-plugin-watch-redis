# Redis Streams Watch Strategy — `dev.mcpg.watch.redis`

> class `watch_strategy` · `native` · package `mcpg-plugin-watch-redis` · artifact `libmcpg_plugin_watch_redis.so` · Apache-2.0

Reads a Redis stream through a consumer group and makes each entry an MCP
Event. The `redis_stream` strategy:

- reads new entries with `XREADGROUP`, and acknowledges an entry (`XACK`)
  once the gateway has stored its `redis.stream.entry` event, and only then;
- leaves an entry the gateway has not taken pending in the group, and claims
  it back (`XAUTOCLAIM`) once it has been idle for `ack_timeout_ms`; the same
  claim takes over the entries of a consumer in the group that died;
- reads this consumer's own pending entries first after a restart.

Delivery is at-least-once, and the event id is `{stream}/{entry id}`, so the
gateway stores an entry delivered twice once. The consumer name is derived
from the event source, the same on every restart; two watches must not
share one. Redis 6.2 or later, or a compatible server (Valkey, Dragonfly).

## Configuration

```yaml
plugins:
  - id: dev.mcpg.watch.redis
    class: watch_strategy
    kind: native
    source: { oci: "<registry>/plugins/watch-redis:protocol-1" }

mcp:
  configurations:
    events:
      enabled: true
      store: { kind: file, dir: /var/lib/mcpg/events/subscriptions }
      log_store: { kind: file, dir: /var/lib/mcpg/events/log }
  capabilities:
    events:
      - name: orders.appended
        description: An entry on the orders stream.
        source:
          watch:
            strategy:
              type: plugin
              kind: redis_stream
              url: rediss://:${env.REDIS_PASSWORD}@redis.internal:6380/0
              stream: orders
            event_type: redis.stream.entry
```

| Field | Type | Default | Notes |
|---|---|---|---|
| `url` | string (required) | — | `redis://` or `rediss://`, with its password; resolved before the watch starts (`${env.X}`, `${secret.X}`, `vault://…`) and never logged. `cred://` is rejected. `rediss://` verifies against public roots. |
| `stream` | string (required) | — | The stream key. |
| `group` | string | `mcpg-events` | The consumer group; created (with the stream) when missing. |
| `consumer` | string | derived from the source | This watch's consumer in the group. |
| `start` | `new` \| `beginning` | `new` | Where a group the watch creates starts: entries added from now, or every entry the stream holds. An existing group keeps its own position. |
| `max_in_flight` | int | `256` | Most entries read and not yet acknowledged (1 to 1000). |
| `ack_timeout_ms` | int | `60000` | How long an entry stays unacknowledged before it is claimed back. |
| `max_body_bytes` | int | `131072` | Larger fields are left out of the event, with `fieldsTruncated: true` and `fieldsSize` (at most 196608). |

An entry trimmed from the stream (`MAXLEN`, `XTRIM`) before the gateway
took it is lost; the watch logs it and removes it from the pending list.

## Events

Each `redis.stream.entry` event's `data` is `{stream, id, fields,
binaryFields?}`: `fields` maps each field to its value when it is UTF-8, and
`binaryFields` holds the others as standard base64. The entry id's time is
the event time.

## Build / test

```bash
cargo build  -p mcpg-plugin-watch-redis
cargo test   -p mcpg-plugin-watch-redis                              # unit tests
cargo test   -p mcpg-plugin-watch-redis --features integration-tests # Redis (docker)
cargo clippy -p mcpg-plugin-watch-redis --all-targets -- -D warnings
```

The integration suite runs the watch through a window of two, an
acknowledgement, a claim-back after the timeout, the consumer's own pending
entry after a restart, and the take-over of a dead consumer's entry.
