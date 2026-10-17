//! Redis Streams `watch_strategy` plugin (`dev.mcpg.watch.redis`).
//!
//! The `redis_stream` strategy reads a stream through a consumer group
//! (`XREADGROUP`), through the SDK's queue helper. Each entry is one
//! `redis.stream.entry` event, acknowledged (`XACK`) once the gateway has
//! stored it. An entry the gateway does not acknowledge stays pending in the
//! group: the watch claims it back (`XAUTOCLAIM`) once it has been idle for
//! `ack_timeout_ms`, which also takes over the entries of a consumer that
//! died, and reads its own pending entries first after a restart. Delivery is
//! at-least-once, and the entry id, the event id, lets the gateway take an
//! entry once.
//!
//! Reads block on their own connection; acknowledgements use another, so
//! they never wait behind a blocking read.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use base64::Engine as _;
use mcpg_plugin_protocol::backend::{WatchError, WatchEvent};
use mcpg_plugin_protocol::{PluginManifest, firstparty_manifest};
use mcpg_plugin_sdk::HostHandle;
use mcpg_plugin_sdk::ffi::{SyncWatchStrategyPlugin, WatchHandleBox};
use mcpg_plugin_sdk::queue::{
    Delivery, QueueOptions, QueueSource, acknowledge_queue_watch, cancel_queue_watch, defaults,
    spawn_queue_watch,
};
use redis::AsyncCommands;
use redis::aio::MultiplexedConnection;
use redis::streams::{
    StreamAutoClaimOptions, StreamAutoClaimReply, StreamId, StreamReadOptions, StreamReadReply,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};

pub const PLUGIN_ID: &str = "dev.mcpg.watch.redis";

/// The strategy discriminator this entity handles.
pub const WATCH_KIND: &str = "redis_stream";

/// The event type each entry makes.
pub const ENTRY_EVENT_TYPE: &str = "redis.stream.entry";

/// Largest `max_body_bytes`: the event must stay under the host's 256 KiB.
const MAX_BODY_BYTES: usize = 192 * 1024;

/// How long one read blocks for an entry.
const RECEIVE_WAIT: Duration = Duration::from_secs(1);

/// How often the watch looks for entries idle past `ack_timeout_ms`.
const CLAIM_EVERY: Duration = Duration::from_secs(1);

fn default_group() -> String {
    "mcpg-events".to_owned()
}

/// Where a new consumer group starts reading.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Start {
    /// Entries added after the group is created.
    #[default]
    New,
    /// Every entry the stream holds.
    Beginning,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WatchSpec {
    /// `redis://` or `rediss://`, with its password; resolved before the
    /// watch starts (`${env.X}`, `${secret.X}`, `vault://…`).
    url: String,
    /// The stream key.
    stream: String,
    /// The consumer group, created when missing.
    #[serde(default = "default_group")]
    group: String,
    /// This watch's consumer in the group; default: one derived from the
    /// source, the same on every restart, so its pending entries come back.
    #[serde(default)]
    consumer: Option<String>,
    /// Where a group the watch creates starts.
    #[serde(default)]
    start: Start,
    #[serde(default = "defaults::max_in_flight")]
    max_in_flight: usize,
    /// How long an entry stays unacknowledged before the watch claims it back.
    #[serde(default = "defaults::ack_timeout_ms")]
    ack_timeout_ms: u64,
    #[serde(default = "defaults::max_body_bytes")]
    max_body_bytes: usize,
}

/// A consumer name the same for the same source on every restart.
fn derived_consumer(resource_uri: &str) -> String {
    let digest = blake3::hash(resource_uri.as_bytes()).to_hex();
    format!("mcpg-{}", &digest[..16])
}

/// The time of an entry id `{ms}-{seq}`.
fn id_millis(id: &str) -> Option<i64> {
    id.split_once('-')?.0.parse().ok()
}

/// The broker side of one `redis_stream` watch.
struct RedisStream {
    client: redis::Client,
    stream: String,
    group: String,
    consumer: String,
    start: Start,
    claim_idle_ms: u64,
    max_body_bytes: usize,
    rt: tokio::runtime::Runtime,
    reader: Mutex<Option<MultiplexedConnection>>,
    writer: Mutex<Option<MultiplexedConnection>>,
    /// Where the next read of this consumer's own pending entries starts;
    /// `None` once they are all read.
    backlog: Mutex<Option<String>>,
    last_claim: Mutex<Instant>,
}

impl RedisStream {
    fn writer(&self) -> Option<MultiplexedConnection> {
        self.writer
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    async fn open(&self) -> Result<(MultiplexedConnection, MultiplexedConnection), String> {
        let mut reader = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| format!("Redis connect failed: {e}"))?;
        let writer = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| format!("Redis connect failed: {e}"))?;
        let from = match self.start {
            Start::New => "$",
            Start::Beginning => "0",
        };
        let created: Result<(), redis::RedisError> = reader
            .xgroup_create_mkstream(&self.stream, &self.group, from)
            .await;
        match created {
            Ok(()) => {}
            Err(e) if e.code() == Some("BUSYGROUP") => {}
            Err(e) => return Err(format!("Redis XGROUP CREATE failed: {e}")),
        }
        Ok((reader, writer))
    }

    /// The entries `ids` names, as deliveries; an entry trimmed away is
    /// acknowledged, so it leaves the pending list.
    fn deliveries(&self, ids: Vec<StreamId>) -> Vec<Delivery> {
        let mut deliveries = Vec::new();
        for entry in ids {
            if entry.map.is_empty() {
                tracing::warn!(stream = %self.stream, id = %entry.id, "redis_stream: a pending entry was trimmed from the stream before the gateway took it; it is lost");
                let _ = self.ack(&Value::from(entry.id.clone()));
                continue;
            }
            deliveries.push(Delivery {
                event: entry_event(&self.stream, &entry, self.max_body_bytes),
                receipt: Value::from(entry.id),
            });
        }
        deliveries
    }
}

impl QueueSource for RedisStream {
    fn connect(&self) -> Result<(), String> {
        let (reader, writer) = self.rt.block_on(self.open())?;
        *self.reader.lock().unwrap_or_else(|e| e.into_inner()) = Some(reader);
        *self.writer.lock().unwrap_or_else(|e| e.into_inner()) = Some(writer);
        *self.backlog.lock().unwrap_or_else(|e| e.into_inner()) = Some("0".into());
        Ok(())
    }

    fn receive(&self, max: usize, wait: Duration) -> Result<Vec<Delivery>, String> {
        let Some(mut conn) = self
            .reader
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
        else {
            return Err("not connected".into());
        };
        let streams = [self.stream.as_str()];
        // This consumer's own pending entries first: what a previous run
        // read and never acknowledged.
        let backlog = self
            .backlog
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(after) = backlog {
            let options = StreamReadOptions::default()
                .group(&self.group, &self.consumer)
                .count(max);
            let reply: StreamReadReply = self
                .rt
                .block_on(conn.xread_options(&streams, &[after.as_str()], &options))
                .map_err(|e| format!("Redis XREADGROUP failed: {e}"))?;
            let ids: Vec<StreamId> = reply.keys.into_iter().flat_map(|k| k.ids).collect();
            *self.backlog.lock().unwrap_or_else(|e| e.into_inner()) =
                ids.last().map(|last| last.id.clone());
            if !ids.is_empty() {
                return Ok(self.deliveries(ids));
            }
        }
        // Entries idle past the timeout: released, or a dead consumer's.
        let claim_due = {
            let mut last = self.last_claim.lock().unwrap_or_else(|e| e.into_inner());
            let due = last.elapsed() >= CLAIM_EVERY;
            if due {
                *last = Instant::now();
            }
            due
        };
        if claim_due {
            let reply: StreamAutoClaimReply = self
                .rt
                .block_on(conn.xautoclaim_options(
                    &self.stream,
                    &self.group,
                    &self.consumer,
                    self.claim_idle_ms,
                    "0-0",
                    StreamAutoClaimOptions::default().count(max),
                ))
                .map_err(|e| format!("Redis XAUTOCLAIM failed: {e}"))?;
            if !reply.claimed.is_empty() {
                return Ok(self.deliveries(reply.claimed));
            }
        }
        let options = StreamReadOptions::default()
            .group(&self.group, &self.consumer)
            .count(max)
            .block(usize::try_from(wait.as_millis()).unwrap_or(1_000));
        let reply: Option<StreamReadReply> = self
            .rt
            .block_on(conn.xread_options(&streams, &[">"], &options))
            .map_err(|e| format!("Redis XREADGROUP failed: {e}"))?;
        let ids = reply
            .map(|r| r.keys.into_iter().flat_map(|k| k.ids).collect())
            .unwrap_or_default();
        Ok(self.deliveries(ids))
    }

    fn ack(&self, receipt: &Value) -> Result<(), String> {
        let (Some(id), Some(mut conn)) = (receipt.as_str(), self.writer()) else {
            return Ok(());
        };
        let _: i64 = self
            .rt
            .block_on(conn.xack(&self.stream, &self.group, &[id]))
            .map_err(|e| format!("Redis XACK failed: {e}"))?;
        Ok(())
    }

    /// Redis cannot hand an entry back; it stays pending and is claimed
    /// once idle for `ack_timeout_ms`.
    fn release(&self, _receipt: &Value) -> Result<(), String> {
        Ok(())
    }
}

/// The `redis.stream.entry` event of one entry.
fn entry_event(stream: &str, entry: &StreamId, max_body_bytes: usize) -> WatchEvent {
    let mut fields = BTreeMap::new();
    let mut binary = BTreeMap::new();
    let mut size = 0usize;
    for (name, value) in &entry.map {
        let bytes: Vec<u8> = match value {
            redis::Value::BulkString(bytes) => bytes.clone(),
            redis::Value::SimpleString(text) => text.clone().into_bytes(),
            redis::Value::Int(n) => n.to_string().into_bytes(),
            _ => continue,
        };
        size += name.len() + bytes.len();
        match String::from_utf8(bytes) {
            Ok(text) => {
                fields.insert(name.clone(), json!(text));
            }
            Err(e) => {
                binary.insert(
                    name.clone(),
                    json!(base64::engine::general_purpose::STANDARD.encode(e.into_bytes())),
                );
            }
        }
    }
    let mut data = Map::new();
    data.insert("stream".into(), json!(stream));
    data.insert("id".into(), json!(entry.id));
    if size > max_body_bytes {
        data.insert("fields".into(), Value::Null);
        data.insert("fieldsTruncated".into(), json!(true));
        data.insert("fieldsSize".into(), json!(size));
    } else {
        data.insert("fields".into(), json!(fields));
        if !binary.is_empty() {
            data.insert("binaryFields".into(), json!(binary));
        }
    }
    WatchEvent {
        event_id: Some(format!("{stream}/{}", entry.id)),
        event_type: Some(ENTRY_EVENT_TYPE.to_owned()),
        occurred_at: id_millis(&entry.id).and_then(mcpg_plugin_protocol::occurred_at_from_millis),
        data: Some(Value::Object(data)),
        ..WatchEvent::default()
    }
}

/// Install the crypto provider rustls builds TLS configurations from: a
/// build that links both rustls providers has no default, and this
/// cdylib's copy of rustls does not see the host's.
fn install_crypto_provider() {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// `watch_strategy` entity. Stateless beyond its manifest — each watcher's
/// server and stream arrive on the per-watch spec.
pub struct RedisWatchPlugin {
    manifest: PluginManifest,
}

impl RedisWatchPlugin {
    /// Infallible cdylib factory; the watch carries no plugin-level config.
    pub fn from_host_config(_config_json: &str, _host: HostHandle) -> Self {
        install_crypto_provider();
        Self {
            manifest: firstparty_manifest! {
                id: "dev.mcpg.watch.redis",
                name: "Redis Streams Watch Strategy",
                class: WatchStrategy,
            },
        }
    }
}

impl SyncWatchStrategyPlugin for RedisWatchPlugin {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn kind(&self) -> &str {
        WATCH_KIND
    }

    fn watch(
        &self,
        resource_uri: &str,
        spec: &Value,
        emit_event: Box<dyn Fn(&str) + Send + Sync + 'static>,
    ) -> Result<WatchHandleBox, WatchError> {
        let (spec, _) = mcpg_plugin_protocol::take_watch_resume(spec);
        let parsed: WatchSpec =
            serde_json::from_value(spec).map_err(|e| WatchError::InvalidSpec {
                message: format!("invalid redis_stream watch spec: {e}"),
            })?;
        let invalid = |m: String| WatchError::InvalidSpec { message: m };
        if !parsed.url.starts_with("redis://") && !parsed.url.starts_with("rediss://") {
            return Err(invalid("url must start with redis:// or rediss://".into()));
        }
        if parsed.url.contains("cred://") {
            return Err(invalid(
                "url must not contain cred:// — per-caller credentials are unsupported; use \
                 ${env.X} / ${secret.X} / vault:// (resolved before the watch starts)"
                    .into(),
            ));
        }
        if parsed.stream.trim().is_empty() || parsed.group.trim().is_empty() {
            return Err(invalid("stream and group must not be empty".into()));
        }
        if !(1..=MAX_BODY_BYTES).contains(&parsed.max_body_bytes) {
            return Err(invalid(format!(
                "max_body_bytes must be 1 to {MAX_BODY_BYTES}"
            )));
        }
        let consumer = parsed
            .consumer
            .clone()
            .unwrap_or_else(|| derived_consumer(resource_uri));
        if consumer.trim().is_empty() {
            return Err(invalid("consumer must not be empty".into()));
        }
        // The URL is not echoed: it carries the password.
        let client = redis::Client::open(parsed.url.as_str())
            .map_err(|_| invalid("url is not a Redis URL".into()))?;
        let options = QueueOptions {
            source: format!("redis stream {} group {}", parsed.stream, parsed.group),
            max_in_flight: parsed.max_in_flight,
            ack_timeout: Duration::from_millis(parsed.ack_timeout_ms),
            receive_wait: RECEIVE_WAIT,
        };
        options.validate().map_err(invalid)?;
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("mcpg-redis-watch")
            .enable_all()
            .build()
            .map_err(|e| WatchError::Subscribe {
                message: format!("redis_stream: tokio runtime init failed: {e}"),
            })?;
        let source = RedisStream {
            client,
            stream: parsed.stream,
            group: parsed.group,
            consumer,
            start: parsed.start,
            claim_idle_ms: parsed.ack_timeout_ms,
            max_body_bytes: parsed.max_body_bytes,
            rt,
            reader: Mutex::new(None),
            writer: Mutex::new(None),
            backlog: Mutex::new(None),
            last_claim: Mutex::new(Instant::now()),
        };
        spawn_queue_watch(resource_uri, options, emit_event, source)
    }

    fn acknowledge(&self, watch_handle: &WatchHandleBox, position: &Value) {
        acknowledge_queue_watch(watch_handle, position);
    }

    fn cancel(&self, watch_handle: WatchHandleBox) {
        cancel_queue_watch(watch_handle);
    }
}

mcpg_plugin_sdk::declare_plugin! {
    plugin_id: "dev.mcpg.watch.redis",
    plugin_version: env!("CARGO_PKG_VERSION"),
    descriptor_yaml: include_str!("../plugin.yaml"),
    capabilities: &[::mcpg_plugin_protocol::capability::Capability::NetworkOutbound],
    entities: [
        watch_strategy as watch {
            inner_name: "",
            plugin_type: RedisWatchPlugin,
            factory: |cfg: &str, host: ::mcpg_plugin_sdk::HostHandle|
                RedisWatchPlugin::from_host_config(cfg, host),
        },
    ],
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn stub_host() -> HostHandle {
        // SAFETY: `stub_host_ref` returns a process-static no-op host ref; the
        // factory ignores the host entirely.
        #[allow(unsafe_code)]
        unsafe {
            HostHandle::from_ffi(mcpg_plugin_sdk::testing::stub_host_ref())
        }
    }

    fn plugin() -> RedisWatchPlugin {
        RedisWatchPlugin::from_host_config("", stub_host())
    }

    #[test]
    fn a_tls_configuration_can_be_built_once_the_plugin_is_made() {
        let _ = plugin();
        let _ = rustls::ClientConfig::builder();
    }

    fn refused(spec: Value) -> String {
        match plugin().watch("mcpg-event://orders", &spec, Box::new(|_| {})) {
            Err(WatchError::InvalidSpec { message }) => message,
            Err(other) => panic!("not a spec error: {other:?}"),
            Ok(handle) => {
                plugin().cancel(handle);
                panic!("accepted {spec}");
            }
        }
    }

    #[test]
    fn manifest_and_kind_are_correct() {
        use mcpg_plugin_protocol::PluginClass;
        let p = plugin();
        assert_eq!(SyncWatchStrategyPlugin::manifest(&p).id, PLUGIN_ID);
        assert_eq!(
            SyncWatchStrategyPlugin::manifest(&p).plugin_class,
            PluginClass::WatchStrategy
        );
        assert_eq!(p.kind(), WATCH_KIND);
    }

    #[test]
    fn spec_parses_with_defaults() {
        let parsed: WatchSpec =
            serde_json::from_value(json!({ "url": "redis://cache", "stream": "orders" })).unwrap();
        assert_eq!(parsed.group, "mcpg-events");
        assert_eq!(parsed.start, Start::New);
        assert!(parsed.consumer.is_none());
        assert_eq!(parsed.ack_timeout_ms, 60_000);
    }

    #[test]
    fn a_bad_spec_is_refused_before_anything_connects() {
        let with = |extra: Value| {
            let mut spec = json!({ "url": "redis://cache", "stream": "orders" });
            for (k, v) in extra.as_object().unwrap() {
                spec[k] = v.clone();
            }
            spec
        };
        assert!(refused(with(json!({ "bogus": 1 }))).contains("unknown field"));
        let scheme = refused(with(json!({ "url": "http://:secret@cache" })));
        assert!(scheme.contains("redis://") && !scheme.contains("secret"));
        assert!(refused(with(json!({ "url": "redis://:cred://x@cache" }))).contains("cred://"));
        assert!(refused(with(json!({ "stream": " " }))).contains("stream"));
        assert!(refused(with(json!({ "consumer": " " }))).contains("consumer"));
        assert!(refused(with(json!({ "max_in_flight": 0 }))).contains("max_in_flight"));
        assert!(refused(with(json!({ "ack_timeout_ms": 0 }))).contains("ack_timeout_ms"));
        assert!(refused(with(json!({ "max_body_bytes": 300_000 }))).contains("max_body_bytes"));
    }

    #[test]
    fn an_entry_becomes_a_redis_stream_entry_event() {
        let entry = StreamId {
            id: "1700000000123-0".into(),
            map: HashMap::from([
                ("order".to_owned(), redis::Value::BulkString(b"42".to_vec())),
                (
                    "blob".to_owned(),
                    redis::Value::BulkString(vec![0xff, 0xfe]),
                ),
            ]),
        };
        let event = entry_event("orders", &entry, 1024);
        assert_eq!(event.event_id.as_deref(), Some("orders/1700000000123-0"));
        assert_eq!(
            event.occurred_at.as_deref(),
            Some("2023-11-14T22:13:20.123Z")
        );
        let data = event.data.unwrap();
        assert_eq!(data["fields"], json!({ "order": "42" }));
        assert_eq!(data["binaryFields"], json!({ "blob": "//4=" }));
        let big = entry_event("orders", &entry, 4).data.unwrap();
        assert_eq!(big["fields"], Value::Null);
        assert_eq!(big["fieldsTruncated"], json!(true));
    }

    #[test]
    fn the_derived_consumer_is_the_same_for_the_same_source() {
        assert_eq!(derived_consumer("a"), derived_consumer("a"));
        assert_ne!(derived_consumer("a"), derived_consumer("b"));
        assert_eq!(id_millis("1700000000123-7"), Some(1_700_000_000_123));
        assert_eq!(id_millis("nope"), None);
    }
}
