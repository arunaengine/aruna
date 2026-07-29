use super::backend::{build_backend_path, rebuild_backend_path};
use super::{
    BackendRegistry, BlobHandle, BlobHandler, ControlPlaneTimeoutKind, NodeBackend,
    control_plane::control_plane_timeout_event,
    control_plane::{
        parse_replication_init, validate_replication_init_ack, with_control_plane_timeout,
    },
};
use crate::messages::{MessageType, ReplicationMessage};
use aruna_core::UserId;
use aruna_core::alpn::Alpn;
use aruna_core::effects::{BlobEffect, StagingSourceEffect, StorageEffect};
use aruna_core::egress::EgressPolicy;
use aruna_core::errors::{BlobError, ConversionError};
use aruna_core::events::{BlobEvent, Event, StagingSourceEvent, StorageEvent};
use aruna_core::keyspaces::{BLOB_LOCATIONS_KEYSPACE, BUCKET_STATS_DB, HASH_PATHS_INDEX_KEYSPACE};
use aruna_core::stream::BackendStream;
use aruna_core::structs::checksum::HASH_BLAKE3;
use aruna_core::structs::{
    Backend, BackendConfig, BackendLocation, BackendRef, BlobTimeoutConfig, HiddenBlobKey,
    MultipartUploadPartKey, RealmId, ResolvedBackend, ResolvedSourceAccess, SourceConnectorKind,
    Status,
};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_storage::storage;
use futures::TryStreamExt;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use tempfile::tempdir;
use ulid::Ulid;

mod failing_close {
    use opendal::raw::oio;
    use opendal::raw::{Access, AccessorInfo, OpWrite, RpWrite};
    use opendal::{Buffer, Builder, Capability, Error, ErrorKind, Metadata, Operator};
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    #[derive(Debug, Default)]
    pub(super) struct CloseFailsBuilder {
        aborts: Arc<AtomicUsize>,
    }

    impl Builder for CloseFailsBuilder {
        type Config = ();

        fn build(self) -> opendal::Result<impl Access> {
            Ok(CloseFailsBackend {
                aborts: self.aborts,
            })
        }
    }

    #[derive(Debug)]
    pub(super) struct CloseFailsBackend {
        aborts: Arc<AtomicUsize>,
    }

    impl Access for CloseFailsBackend {
        type Reader = ();
        type Writer = CloseFailsWriter;
        type Lister = ();
        type Deleter = ();
        type Copier = ();

        fn info(&self) -> Arc<AccessorInfo> {
            let info = Arc::new(AccessorInfo::default());
            info.set_scheme("close_fails")
                .set_root("/")
                .set_native_capability(Capability {
                    write: true,
                    write_can_empty: true,
                    write_can_multi: true,
                    ..Default::default()
                });
            info
        }

        async fn write(
            &self,
            _path: &str,
            _args: OpWrite,
        ) -> opendal::Result<(RpWrite, Self::Writer)> {
            Ok((
                RpWrite::new(),
                CloseFailsWriter {
                    aborts: self.aborts.clone(),
                },
            ))
        }
    }

    pub(super) struct CloseFailsWriter {
        aborts: Arc<AtomicUsize>,
    }

    impl oio::Write for CloseFailsWriter {
        async fn write(&mut self, _bs: Buffer) -> opendal::Result<()> {
            Ok(())
        }

        async fn close(&mut self) -> opendal::Result<Metadata> {
            Err(Error::new(
                ErrorKind::Unexpected,
                "injected finalization failure",
            ))
        }

        async fn abort(&mut self) -> opendal::Result<()> {
            self.aborts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    pub(super) fn operator_with_aborts() -> (Operator, Arc<AtomicUsize>) {
        let aborts = Arc::new(AtomicUsize::new(0));
        let operator = Operator::new(CloseFailsBuilder {
            aborts: aborts.clone(),
        })
        .unwrap()
        .finish();
        (operator, aborts)
    }
}

async fn loopback_net_handle() -> (NetHandle, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let net_handle = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage_handle,
    )
    .await
    .unwrap();
    (net_handle, dir)
}

async fn connected_stream_pair() -> (NetHandle, tempfile::TempDir, NetHandle, tempfile::TempDir) {
    let (net_a, dir_a) = loopback_net_handle().await;
    let (net_b, dir_b) = loopback_net_handle().await;
    net_a.add_peer_addr(net_b.endpoint_addr()).await;
    net_b.add_peer_addr(net_a.endpoint_addr()).await;
    (net_a, dir_a, net_b, dir_b)
}

struct TestContext {
    _temp_dir: tempfile::TempDir,
    blob_handle: BlobHandle,
    storage_handle: aruna_storage::storage::StorageHandle,
}

async fn setup_blob_handle(max_bucket_size: u64) -> TestContext {
    let temp_dir = tempdir().unwrap();
    let temp_root = temp_dir.path().to_str().unwrap().to_string();
    let blob_root = format!("{temp_root}/blobstore");
    std::fs::create_dir_all(&blob_root).unwrap();
    let storage_handle = storage::FjallStorage::open(&temp_root).unwrap();
    let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
        .await
        .unwrap();
    let blob_handle = BlobHandler::new(
        BackendConfig {
            backend_type: Backend::FileSystem,
            root: blob_root,
            service_config: HashMap::new(),
            bucket_prefix: Some("aruna-test-".to_string()),
            max_bucket_size: Some(max_bucket_size),
            multipart_bucket: Some("uploaded-parts".to_string()),
            timeouts: Default::default(),
        },
        storage_handle.clone(),
        net_handle,
    )
    .await
    .unwrap();

    TestContext {
        _temp_dir: temp_dir,
        blob_handle,
        storage_handle,
    }
}

fn stream_from_bytes(
    bytes: &[u8],
) -> BackendStream<Result<bytes::Bytes, aruna_core::stream::StreamError>> {
    BackendStream::new(tokio_util::io::ReaderStream::new(std::io::Cursor::new(
        bytes.to_vec(),
    )))
}

async fn bucket_load(
    storage_handle: &storage::StorageHandle,
    backend: &BackendRef,
    bucket: &str,
) -> u64 {
    let mut key = backend.key_bytes();
    key.push(0);
    key.extend_from_slice(bucket.as_bytes());
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: BUCKET_STATS_DB.to_string(),
            key: key.into(),
            txn_id: None,
        })
        .await
    else {
        panic!("unexpected storage event")
    };

    value
        .map(|value| u64::from_le_bytes(value.as_ref().try_into().unwrap()))
        .unwrap_or(0)
}

async fn keyspace_count(storage_handle: &storage::StorageHandle, key_space: &str) -> usize {
    let Event::Storage(StorageEvent::IterResult { values, .. }) = storage_handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix: None,
            start: None,
            limit: 16,
            txn_id: None,
        })
        .await
    else {
        panic!("unexpected storage event")
    };
    values.len()
}

fn test_user_id() -> UserId {
    UserId::nil(RealmId::from_bytes([1u8; 32]))
}

fn make_test_location() -> BackendLocation {
    BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/tmp".to_string(),
        storage_bucket: "bucket".to_string(),
        backend_path: format!("blob/{}", Ulid::generate()),
        ulid: Ulid::generate(),
        compressed: false,
        encrypted: false,
        created_by: test_user_id(),
        created_at: SystemTime::now(),
        staging: false,
        partial: false,
        blob_size: 32,
        hashes: HashMap::new(),
    }
}

fn filesystem_backend(root: &str, prefix: &str, parts: &str) -> BackendConfig {
    std::fs::create_dir_all(root).unwrap();
    BackendConfig {
        backend_type: Backend::FileSystem,
        root: root.to_string(),
        service_config: HashMap::new(),
        bucket_prefix: Some(prefix.to_string()),
        max_bucket_size: Some(4),
        multipart_bucket: Some(parts.to_string()),
        timeouts: Default::default(),
    }
}

// Two filesystem backends with distinct roots are the multi-backend fixture:
// deterministic, hermetic, and enough to prove registry dispatch.
async fn setup_two_backends() -> TestContext {
    let temp_dir = tempdir().unwrap();
    let temp_root = temp_dir.path().to_str().unwrap().to_string();
    let storage_handle = storage::FjallStorage::open(&temp_root).unwrap();
    let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
        .await
        .unwrap();
    let mut backends = std::collections::BTreeMap::new();
    backends.insert(
        "default".to_string(),
        NodeBackend::new(
            filesystem_backend(&format!("{temp_root}/hot"), "hot-", "hot-parts"),
            None,
        ),
    );
    backends.insert(
        "cold".to_string(),
        NodeBackend::new(
            filesystem_backend(&format!("{temp_root}/cold"), "cold-", "cold-parts"),
            Some("cold".to_string()),
        ),
    );
    let registry = BackendRegistry::new(backends, "default".to_string()).unwrap();
    let blob_handle = BlobHandler::with_registry(
        registry,
        storage_handle.clone(),
        net_handle,
        EgressPolicy::loopback(),
    )
    .await
    .unwrap();

    TestContext {
        _temp_dir: temp_dir,
        blob_handle,
        storage_handle,
    }
}

fn cold_backend() -> ResolvedBackend {
    ResolvedBackend::new(
        BackendRef::Node("cold".to_string()),
        Some("cold".to_string()),
    )
}

#[test]
fn registry_reads_config() {
    // The parsed backends file is the only source of names, classes and rules.
    let file = aruna_core::structs::BackendsFile::parse(
        r#"
[backend.hot]
type = "filesystem"
root = "/data/hot"
default = true

[backend.cold]
type = "filesystem"
class = "cold"
root = "/data/cold"

[[routing]]
key_prefix = "archive/"
target = { class = "cold" }

[egress]
serve_group_backends = false
"#,
    )
    .unwrap();
    let config = file
        .resolve(&|_| None, BlobTimeoutConfig::default())
        .unwrap();

    let registry = BackendRegistry::from_config(&config).unwrap();
    let routing = registry.routing();

    assert_eq!(registry.default_name(), "hot");
    assert_eq!(registry.entries().count(), 2);
    assert_eq!(routing.rules.len(), 1);
    assert_eq!(routing.catalog.class_of("cold"), Some("cold"));
    let snapshot = routing.snapshot(Ulid::from_bytes([1u8; 16]));
    assert_eq!(
        aruna_core::structs::resolve_backend(&snapshot, "bucket", "archive/one").unwrap(),
        ResolvedBackend::new(
            BackendRef::Node("cold".to_string()),
            Some("cold".to_string())
        )
    );
    assert_eq!(
        aruna_core::structs::resolve_backend(&snapshot, "bucket", "other").unwrap(),
        ResolvedBackend::new(BackendRef::Node("hot".to_string()), None)
    );
    assert_eq!(
        aruna_core::structs::resolve_backend(
            &snapshot.with_group_default(Some(aruna_core::structs::RoutingTarget::Backend(
                BackendRef::Group(Ulid::from_bytes([2u8; 16]))
            ))),
            "bucket",
            "other"
        ),
        Err(aruna_core::structs::RoutingError::GroupEgressDisabled)
    );
}

#[tokio::test]
async fn write_lands_on_backend() {
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();

    let BlobEvent::WriteFinished { location } = handler
        .write_blob(
            "bucket",
            "cold.bin",
            cold_backend(),
            test_user_id(),
            stream_from_bytes(b"cold"),
        )
        .await
    else {
        panic!("cold write failed")
    };

    assert_eq!(location.backend, BackendRef::Node("cold".to_string()));
    assert_eq!(location.storage_class.as_deref(), Some("cold"));
    assert!(location.storage_bucket.starts_with("cold-"));
    assert!(std::path::Path::new(&location.get_full_path().unwrap()).exists());
    assert!(matches!(
        handler.read_blob(location).await,
        BlobEvent::ReadFinished { .. }
    ));
}

#[tokio::test]
async fn stats_keys_qualify() {
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();

    let BlobEvent::WriteFinished { location: hot } = handler
        .write_blob(
            "bucket",
            "hot.bin",
            handler.registry.default_resolved(),
            test_user_id(),
            stream_from_bytes(b"hot"),
        )
        .await
    else {
        panic!("hot write failed")
    };
    let BlobEvent::WriteFinished { location: cold } = handler
        .write_blob(
            "bucket",
            "cold.bin",
            cold_backend(),
            test_user_id(),
            stream_from_bytes(b"cold"),
        )
        .await
    else {
        panic!("cold write failed")
    };

    assert_ne!(hot.storage_bucket, cold.storage_bucket);
    assert_eq!(
        bucket_load(&context.storage_handle, &hot.backend, &hot.storage_bucket).await,
        1
    );
    assert_eq!(
        bucket_load(&context.storage_handle, &cold.backend, &cold.storage_bucket).await,
        1
    );
    // The other backend's qualifier must not see the same bucket name.
    assert_eq!(
        bucket_load(&context.storage_handle, &cold.backend, &hot.storage_bucket).await,
        0
    );
}

#[tokio::test]
async fn read_rejects_unknown() {
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();

    let mut location = make_test_location();
    location.backend = BackendRef::Node("removed".to_string());
    location
        .hashes
        .insert(HASH_BLAKE3.to_string(), vec![0u8; 32]);

    assert!(matches!(
        handler.read_blob(location.clone()).await,
        BlobEvent::Error(BlobError::UnknownBackend(_))
    ));
    assert!(matches!(
        handler.delete_blob(location).await,
        BlobEvent::Error(BlobError::UnknownBackend(_))
    ));
}

#[tokio::test]
async fn backends_track_health() {
    let context = setup_two_backends().await;
    let registry = context.blob_handle.handler.registry.clone();

    assert_eq!(registry.entries().count(), 2);
    for (_, backend) in registry.entries() {
        assert_eq!(*backend.status.read().await, Status::Available);
    }
    assert_eq!(registry.catalog().class_of("cold"), Some("cold"));
    assert!(registry.catalog().class_of("default").is_none());
}

#[tokio::test]
async fn parts_use_pinned_area() {
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();

    let BlobEvent::WriteFinished { location } = handler
        .write_blob_part(
            MultipartUploadPartKey::new(Ulid::generate(), 1),
            cold_backend(),
            test_user_id(),
            false,
            false,
            stream_from_bytes(b"part"),
        )
        .await
    else {
        panic!("part write failed")
    };

    assert_eq!(location.backend, BackendRef::Node("cold".to_string()));
    assert_eq!(location.storage_bucket, "cold-parts");
}

#[test]
fn backend_config_exposes_custom_timeout_values() {
    let config = BackendConfig {
        backend_type: Backend::FileSystem,
        root: "/tmp".to_string(),
        service_config: HashMap::new(),
        bucket_prefix: Some("aruna-test-".to_string()),
        max_bucket_size: Some(1),
        multipart_bucket: Some("multipart".to_string()),
        timeouts: BlobTimeoutConfig {
            control_plane_connect_timeout: Duration::from_secs(11),
            control_plane_io_timeout: Duration::from_secs(12),
            transfer_idle_timeout: Duration::from_secs(13),
        },
    };

    assert_eq!(
        config.timeouts.control_plane_connect_timeout,
        Duration::from_secs(11)
    );
    assert_eq!(
        config.timeouts.control_plane_io_timeout,
        Duration::from_secs(12)
    );
    assert_eq!(
        config.timeouts.transfer_idle_timeout,
        Duration::from_secs(13)
    );
}

#[test]
fn replication_init_ack_accepts_matching_ack() {
    let replication_id = Ulid::generate();
    let ack = ReplicationMessage::new(replication_id, MessageType::BaoTreeInfoReceived);

    assert_eq!(validate_replication_init_ack(ack, replication_id), Ok(()));
}

#[test]
fn replication_init_ack_rejects_unexpected_message_type() {
    let replication_id = Ulid::generate();
    let message = ReplicationMessage::new(
        replication_id,
        MessageType::BaoTreeInfo {
            root: blake3::hash(b"hello world"),
            location: make_test_location(),
        },
    );

    let result = validate_replication_init_ack(message, replication_id);
    assert!(matches!(
        result,
        Err(BlobError::ReplicationRejected(message))
            if message.starts_with("unexpected replication init response: BaoTreeInfo")
    ));
}

#[test]
fn replication_init_ack_rejects_wrong_replication_id() {
    let replication_id = Ulid::generate();
    let wrong_id = Ulid::generate();
    let ack = ReplicationMessage::new(wrong_id, MessageType::BaoTreeInfoReceived);

    assert_eq!(
        validate_replication_init_ack(ack, replication_id),
        Err(BlobError::ReplicationRejected(format!(
            "received replication init ack for unexpected replication id: expected {replication_id}, got {wrong_id}"
        )))
    );
}

#[test]
fn parse_replication_init_accepts_matching_bao_tree_info() {
    let replication_id = Ulid::generate();
    let location = make_test_location();
    let root = blake3::hash(b"hello world");
    let message = ReplicationMessage::new(
        replication_id,
        MessageType::BaoTreeInfo {
            root,
            location: location.clone(),
        },
    );

    assert_eq!(
        parse_replication_init(message, Some(replication_id)),
        Ok((replication_id, root, location))
    );
}

#[test]
fn parse_replication_init_rejects_wrong_replication_id() {
    let replication_id = Ulid::generate();
    let wrong_id = Ulid::generate();
    let message = ReplicationMessage::new(
        wrong_id,
        MessageType::BaoTreeInfo {
            root: blake3::hash(b"hello world"),
            location: make_test_location(),
        },
    );

    assert_eq!(
        parse_replication_init(message, Some(replication_id)),
        Err(BlobError::ReplicationRejected(format!(
            "received replication init for unexpected replication id: expected {replication_id}, got {wrong_id}"
        )))
    );
}

#[test]
fn parse_replication_init_uses_message_id_when_unknown() {
    let replication_id = Ulid::generate();
    let location = make_test_location();
    let root = blake3::hash(b"hello world");
    let message = ReplicationMessage::new(
        replication_id,
        MessageType::BaoTreeInfo {
            root,
            location: location.clone(),
        },
    );

    assert_eq!(
        parse_replication_init(message, None),
        Ok((replication_id, root, location))
    );
}

#[tokio::test]
async fn control_plane_timeout_reports_read_timeout() {
    let event = with_control_plane_timeout(
        std::future::pending::<()>(),
        Duration::from_millis(1),
        ControlPlaneTimeoutKind::Read,
        "reading replication control message",
    )
    .await
    .unwrap_err();

    assert_eq!(
        event,
        BlobEvent::Error(BlobError::ReadError(
            "control-plane timeout after 1ms while reading replication control message".to_string()
        ))
    );
}

#[test]
fn control_plane_timeout_reports_connection_timeout() {
    assert_eq!(
        control_plane_timeout_event(
            ControlPlaneTimeoutKind::Connection,
            "opening bao replication stream",
            Duration::from_secs(30),
        ),
        BlobEvent::Error(BlobError::ConnectionFailed(
            "control-plane timeout after 30s while opening bao replication stream".to_string()
        ))
    );
}

#[tokio::test]
async fn reuses_bucket_until_max_object_count_is_reached() {
    let context = setup_blob_handle(2).await;

    let Event::Blob(BlobEvent::WriteFinished { location: first }) = context
        .blob_handle
        .send_blob_effect(BlobEffect::Write {
            bucket: "bucket-a".to_string(),
            key: "one.bin".to_string(),
            created_by: test_user_id(),
            blob: stream_from_bytes(b"one"),
        })
        .await
    else {
        panic!("first write failed")
    };

    let Event::Blob(BlobEvent::WriteFinished { location: second }) = context
        .blob_handle
        .send_blob_effect(BlobEffect::Write {
            bucket: "bucket-a".to_string(),
            key: "two.bin".to_string(),
            created_by: test_user_id(),
            blob: stream_from_bytes(b"two"),
        })
        .await
    else {
        panic!("second write failed")
    };

    assert_eq!(first.storage_bucket, second.storage_bucket);
    assert!(first.storage_bucket.starts_with("aruna-test-"));
    assert_eq!(
        bucket_load(
            &context.storage_handle,
            &BackendRef::node_default(),
            &first.storage_bucket
        )
        .await,
        2
    );
}

#[tokio::test]
async fn hidden_bucket_registered() {
    let context = setup_blob_handle(5).await;

    assert!(matches!(
        context
            .blob_handle
            .send_blob_effect(BlobEffect::SpoolHidden {
                namespace: Ulid::from_bytes([2u8; 16]),
                name: "partial".to_string(),
                created_by: test_user_id(),
                max_bytes: Some(0),
                blob: stream_from_bytes(b"x"),
            })
            .await,
        Event::Blob(BlobEvent::Error(BlobError::SizeLimitExceeded { limit: 0 }))
    ));
    assert_eq!(
        keyspace_count(&context.storage_handle, BUCKET_STATS_DB).await,
        1
    );
}

#[tokio::test]
async fn creates_new_bucket_after_reaching_max_object_count() {
    let context = setup_blob_handle(1).await;

    let Event::Blob(BlobEvent::WriteFinished { location: first }) = context
        .blob_handle
        .send_blob_effect(BlobEffect::Write {
            bucket: "bucket-a".to_string(),
            key: "one.bin".to_string(),
            created_by: test_user_id(),
            blob: stream_from_bytes(b"one"),
        })
        .await
    else {
        panic!("first write failed")
    };

    let Event::Blob(BlobEvent::WriteFinished { location: second }) = context
        .blob_handle
        .send_blob_effect(BlobEffect::Write {
            bucket: "bucket-a".to_string(),
            key: "two.bin".to_string(),
            created_by: test_user_id(),
            blob: stream_from_bytes(b"two"),
        })
        .await
    else {
        panic!("second write failed")
    };

    assert_ne!(first.storage_bucket, second.storage_bucket);
    assert_eq!(
        bucket_load(
            &context.storage_handle,
            &BackendRef::node_default(),
            &first.storage_bucket
        )
        .await,
        1
    );
    assert_eq!(
        bucket_load(
            &context.storage_handle,
            &BackendRef::node_default(),
            &second.storage_bucket
        )
        .await,
        1
    );
}

#[tokio::test]
async fn deleting_last_object_keeps_bucket_stat_row_at_zero_for_reuse() {
    let context = setup_blob_handle(1).await;

    let Event::Blob(BlobEvent::WriteFinished { location: first }) = context
        .blob_handle
        .send_blob_effect(BlobEffect::Write {
            bucket: "bucket-a".to_string(),
            key: "one.bin".to_string(),
            created_by: test_user_id(),
            blob: stream_from_bytes(b"one"),
        })
        .await
    else {
        panic!("write failed")
    };

    let Event::Blob(BlobEvent::DeleteFinished) = context
        .blob_handle
        .send_blob_effect(BlobEffect::Delete {
            location: first.clone(),
        })
        .await
    else {
        panic!("delete failed")
    };

    assert_eq!(
        bucket_load(
            &context.storage_handle,
            &BackendRef::node_default(),
            &first.storage_bucket
        )
        .await,
        0
    );

    let Event::Blob(BlobEvent::WriteFinished { location: second }) = context
        .blob_handle
        .send_blob_effect(BlobEffect::Write {
            bucket: "bucket-a".to_string(),
            key: "two.bin".to_string(),
            created_by: test_user_id(),
            blob: stream_from_bytes(b"two"),
        })
        .await
    else {
        panic!("second write failed")
    };

    assert_eq!(first.storage_bucket, second.storage_bucket);
    assert_eq!(
        bucket_load(
            &context.storage_handle,
            &BackendRef::node_default(),
            &first.storage_bucket
        )
        .await,
        1
    );
}

#[tokio::test]
async fn multipart_part_bucket_is_excluded_from_bucket_stats() {
    let context = setup_blob_handle(5).await;

    let Event::Blob(BlobEvent::WriteFinished { location }) = context
        .blob_handle
        .send_blob_effect(BlobEffect::WritePart {
            upload_id: Ulid::generate(),
            part_number: 1,
            created_by: test_user_id(),
            compressed: false,
            encrypted: false,
            blob: stream_from_bytes(b"part"),
        })
        .await
    else {
        panic!("multipart write failed")
    };

    assert_eq!(location.storage_bucket, "uploaded-parts");
    assert_eq!(
        bucket_load(
            &context.storage_handle,
            &BackendRef::node_default(),
            "uploaded-parts"
        )
        .await,
        0
    );
}

#[tokio::test]
async fn hidden_spool_roundtrip() {
    let context = setup_blob_handle(5).await;
    let namespace = Ulid::from_bytes([3u8; 16]);
    let Event::Blob(BlobEvent::HiddenSpooled {
        location,
        blake3,
        size,
    }) = context
        .blob_handle
        .send_blob_effect(BlobEffect::SpoolHidden {
            namespace,
            name: "input.zip".to_string(),
            created_by: test_user_id(),
            max_bytes: Some(16),
            blob: stream_from_bytes(b"hidden"),
        })
        .await
    else {
        panic!("hidden spool failed")
    };

    assert_eq!(size, 6);
    assert_eq!(blake3, *blake3::hash(b"hidden").as_bytes());
    assert!(
        location
            .backend_path
            .starts_with(&format!("_jobs/{namespace}/input.zip_"))
    );
    assert_eq!(
        keyspace_count(&context.storage_handle, BLOB_LOCATIONS_KEYSPACE).await,
        0
    );
    assert_eq!(
        keyspace_count(&context.storage_handle, HASH_PATHS_INDEX_KEYSPACE).await,
        0
    );

    let Event::Blob(BlobEvent::HiddenListed { entries }) = context
        .blob_handle
        .send_blob_effect(BlobEffect::ListHidden {
            namespace: Some(namespace),
        })
        .await
    else {
        panic!("hidden list failed")
    };
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].key, HiddenBlobKey::try_from(&location).unwrap());

    let Event::Blob(BlobEvent::HiddenRead { blob, stream_size }) = context
        .blob_handle
        .send_blob_effect(BlobEffect::ReadHiddenRange {
            location: location.clone(),
            range: 1..4,
        })
        .await
    else {
        panic!("hidden read failed")
    };
    let chunks: Vec<bytes::Bytes> = blob.try_collect().await.unwrap();
    assert_eq!(stream_size, 3);
    assert_eq!(chunks.concat(), b"idd");

    assert!(matches!(
        context
            .blob_handle
            .send_blob_effect(BlobEffect::ReadHiddenRange {
                location: location.clone(),
                range: 0..7,
            })
            .await,
        Event::Blob(BlobEvent::Error(BlobError::ReadError(_)))
    ));

    let Event::Blob(BlobEvent::HiddenDeleted) = context
        .blob_handle
        .send_blob_effect(BlobEffect::DeleteHidden {
            key: HiddenBlobKey::try_from(&location).unwrap(),
        })
        .await
    else {
        panic!("hidden delete failed")
    };
    let Event::Blob(BlobEvent::HiddenListed { entries }) = context
        .blob_handle
        .send_blob_effect(BlobEffect::ListHidden {
            namespace: Some(namespace),
        })
        .await
    else {
        panic!("hidden list failed")
    };
    assert!(entries.is_empty());
}

#[tokio::test]
async fn hidden_spool_limits() {
    let context = setup_blob_handle(5).await;
    let namespace = Ulid::from_bytes([5u8; 16]);
    let Event::Blob(BlobEvent::HiddenSpooled { location, .. }) = context
        .blob_handle
        .send_blob_effect(BlobEffect::SpoolHidden {
            namespace,
            name: "seed".to_string(),
            created_by: test_user_id(),
            max_bytes: None,
            blob: stream_from_bytes(b"seed"),
        })
        .await
    else {
        panic!("seed spool failed")
    };
    let _ = context
        .blob_handle
        .send_blob_effect(BlobEffect::DeleteHidden {
            key: HiddenBlobKey::try_from(&location).unwrap(),
        })
        .await;

    assert!(matches!(
        context
            .blob_handle
            .send_blob_effect(BlobEffect::SpoolHidden {
                namespace,
                name: "limited".to_string(),
                created_by: test_user_id(),
                max_bytes: Some(3),
                blob: stream_from_bytes(b"four"),
            })
            .await,
        Event::Blob(BlobEvent::Error(BlobError::SizeLimitExceeded { limit: 3 }))
    ));
    let Event::Blob(BlobEvent::HiddenListed { entries }) = context
        .blob_handle
        .send_blob_effect(BlobEffect::ListHidden {
            namespace: Some(namespace),
        })
        .await
    else {
        panic!("hidden list failed")
    };
    assert!(entries.is_empty());
}

#[tokio::test]
async fn range_passes_writes() {
    // Import readers fetch hidden ranges while a write consumes their payload.
    let context = setup_blob_handle(5).await;
    let namespace = Ulid::from_bytes([6u8; 16]);
    let Event::Blob(BlobEvent::HiddenSpooled { location, .. }) = context
        .blob_handle
        .send_blob_effect(BlobEffect::SpoolHidden {
            namespace,
            name: "source".to_string(),
            created_by: test_user_id(),
            max_bytes: None,
            blob: stream_from_bytes(b"source"),
        })
        .await
    else {
        panic!("source spool failed")
    };
    let (body_sender, body_receiver) =
        tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(1);
    let body_receiver = std::sync::Arc::new(std::sync::Mutex::new(body_receiver));
    let (started_sender, started_receiver) = tokio::sync::oneshot::channel();
    let started_sender = std::sync::Arc::new(std::sync::Mutex::new(Some(started_sender)));
    let body = BackendStream::new(futures::stream::poll_fn(move |context| {
        if let Some(sender) = started_sender.lock().unwrap().take() {
            let _ = sender.send(());
        }
        let mut receiver = body_receiver.lock().unwrap();
        std::pin::Pin::new(&mut *receiver).poll_recv(context)
    }));
    let write_handle = context.blob_handle.clone();
    let write_task = tokio::spawn(async move {
        write_handle
            .send_blob_effect(BlobEffect::Write {
                bucket: "target".to_string(),
                key: "object".to_string(),
                created_by: test_user_id(),
                blob: body,
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(30), started_receiver)
        .await
        .expect("blob write did not start")
        .unwrap();

    let event = tokio::time::timeout(
        Duration::from_secs(30),
        context
            .blob_handle
            .send_blob_effect(BlobEffect::ReadHiddenRange {
                location,
                range: 0..6,
            }),
    )
    .await
    .expect("hidden range read deadlocked");
    let Event::Blob(BlobEvent::HiddenRead { blob, .. }) = event else {
        panic!("hidden range read failed")
    };
    let chunks: Vec<bytes::Bytes> = blob.try_collect().await.unwrap();
    assert_eq!(chunks.concat(), b"source");

    body_sender
        .send(Ok(bytes::Bytes::from_static(b"target")))
        .await
        .unwrap();
    drop(body_sender);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(30), write_task)
            .await
            .expect("blob write did not finish")
            .unwrap(),
        Event::Blob(BlobEvent::WriteFinished { .. })
    ));
}

#[tokio::test]
async fn interlocked_writes_complete() {
    // Each body yields only once both writes stream concurrently; the old
    // sequential effect loop deadlocked here.
    let context = setup_blob_handle(16).await;
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
    let mut tasks = Vec::new();
    for index in 0..2 {
        let handle = context.blob_handle.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            let body = BackendStream::new(futures::stream::once(async move {
                barrier.wait().await;
                Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"payload"))
            }));
            handle
                .send_blob_effect(BlobEffect::Write {
                    bucket: "bucket".to_string(),
                    key: format!("object-{index}"),
                    created_by: test_user_id(),
                    blob: body,
                })
                .await
        }));
    }
    for task in tasks {
        let event = tokio::time::timeout(Duration::from_secs(30), task)
            .await
            .expect("write starved")
            .unwrap();
        assert!(matches!(
            event,
            Event::Blob(BlobEvent::WriteFinished { .. })
        ));
    }
}

#[tokio::test]
async fn tracks_concurrent_loads() {
    // Racing writes into one backend bucket must not lose load increments.
    let context = setup_blob_handle(64).await;
    let Event::Blob(BlobEvent::WriteFinished { location }) = context
        .blob_handle
        .send_blob_effect(BlobEffect::Write {
            bucket: "bucket".to_string(),
            key: "seed".to_string(),
            created_by: test_user_id(),
            blob: stream_from_bytes(b"seed"),
        })
        .await
    else {
        panic!("seed write failed")
    };
    let writers = 8;
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(writers));
    let mut tasks = Vec::new();
    for index in 0..writers {
        let handle = context.blob_handle.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            let body = BackendStream::new(futures::stream::once(async move {
                barrier.wait().await;
                Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"payload"))
            }));
            handle
                .send_blob_effect(BlobEffect::Write {
                    bucket: "bucket".to_string(),
                    key: format!("object-{index}"),
                    created_by: test_user_id(),
                    blob: body,
                })
                .await
        }));
    }
    for task in tasks {
        let event = tokio::time::timeout(Duration::from_secs(30), task)
            .await
            .expect("write starved")
            .unwrap();
        assert!(matches!(
            event,
            Event::Blob(BlobEvent::WriteFinished { .. })
        ));
    }
    let load = bucket_load(
        &context.storage_handle,
        &BackendRef::node_default(),
        &location.storage_bucket,
    )
    .await;
    assert_eq!(load, 1 + writers as u64);
}

#[tokio::test]
async fn staging_source_effect_dispatches_via_blob_handle() {
    let context = setup_blob_handle(1).await;

    let event = context
        .blob_handle
        .send_staging_source_effect(StagingSourceEffect::Head {
            access: ResolvedSourceAccess::OpenDal {
                kind: SourceConnectorKind::Http,
                config: HashMap::from([(
                    "endpoint".to_string(),
                    "https://missing.example.org".to_string(),
                )]),
                path: "not-found".to_string(),
                version: None,
            },
        })
        .await;

    assert!(matches!(
        event,
        Event::StagingSource(StagingSourceEvent::Error { .. })
            | Event::StagingSource(StagingSourceEvent::HeadResult { .. })
    ));
}

#[tokio::test]
async fn concurrent_connections_receive_distinct_non_nil_ids() {
    let context = setup_blob_handle(1).await;
    let handler = context.blob_handle.handler.clone();
    let (net_a, _dir_a, net_b, _dir_b) = connected_stream_pair().await;
    let peer_id = net_b.node_id();

    let stream_a = net_a.open_stream(peer_id, Alpn::Bao).await.unwrap();
    let stream_b = net_a.open_stream(peer_id, Alpn::Bao).await.unwrap();

    let id_a = handler
        .add_connection(None, peer_id, stream_a)
        .await
        .unwrap();
    let id_b = handler
        .add_connection(None, peer_id, stream_b)
        .await
        .unwrap();

    assert!(!id_a.is_nil());
    assert!(!id_b.is_nil());
    assert_ne!(id_a, id_b);

    handler.close_connection(id_a).await;
    assert!(handler.connection_handle(id_a).await.is_err());
    assert!(handler.connection_handle(id_b).await.is_ok());

    net_a.shutdown().await;
    net_b.shutdown().await;
}

#[tokio::test]
async fn add_connection_rejects_nil_and_duplicate_ids() {
    let context = setup_blob_handle(1).await;
    let handler = context.blob_handle.handler.clone();
    let (net_a, _dir_a, net_b, _dir_b) = connected_stream_pair().await;
    let peer_id = net_b.node_id();

    let explicit = Ulid::generate();
    let stream = net_a.open_stream(peer_id, Alpn::Bao).await.unwrap();
    let id = handler
        .add_connection(Some(explicit), peer_id, stream)
        .await
        .unwrap();
    assert_eq!(id, explicit);

    let duplicate = net_a.open_stream(peer_id, Alpn::Bao).await.unwrap();
    assert!(matches!(
        handler
            .add_connection(Some(explicit), peer_id, duplicate)
            .await,
        Err(BlobError::ConnectionFailed(_))
    ));

    let nil_stream = net_a.open_stream(peer_id, Alpn::Bao).await.unwrap();
    assert!(matches!(
        handler
            .add_connection(Some(Ulid::nil()), peer_id, nil_stream)
            .await,
        Err(BlobError::ConnectionFailed(_))
    ));

    net_a.shutdown().await;
    net_b.shutdown().await;
}

#[tokio::test]
async fn write_finalization_failure_emits_no_success_or_load() {
    let context = setup_blob_handle(1).await;
    let handler = context.blob_handle.handler.clone();
    let location = BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/tmp".to_string(),
        storage_bucket: "finalization-bucket".to_string(),
        backend_path: format!("obj/{}", Ulid::generate()),
        ulid: Ulid::generate(),
        compressed: false,
        encrypted: false,
        created_by: test_user_id(),
        created_at: SystemTime::now(),
        staging: false,
        partial: false,
        blob_size: 0,
        hashes: HashMap::new(),
    };

    let (operator, aborts) = failing_close::operator_with_aborts();
    let event = handler
        .write_stream_to_location(location.clone(), operator, stream_from_bytes(b"payload"))
        .await;

    assert!(
        matches!(event, BlobEvent::Error(BlobError::WriteError(_))),
        "close failure must surface as an error, got {event:?}"
    );
    assert_eq!(
        bucket_load(
            &context.storage_handle,
            &BackendRef::node_default(),
            &location.storage_bucket
        )
        .await,
        0
    );
    assert_eq!(aborts.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn failed_write_cleans() {
    let context = setup_blob_handle(1).await;
    let root = tempdir().unwrap();
    let location = BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: root.path().to_str().unwrap().to_string(),
        storage_bucket: "bucket".to_string(),
        backend_path: "partial.bin".to_string(),
        ulid: Ulid::generate(),
        compressed: false,
        encrypted: false,
        created_by: test_user_id(),
        created_at: SystemTime::now(),
        staging: false,
        partial: false,
        blob_size: 0,
        hashes: HashMap::new(),
    };
    let operator = crate::opendal::init_operator(
        Backend::FileSystem,
        HashMap::from([(
            "root".to_string(),
            root.path().to_str().unwrap().to_string(),
        )]),
    )
    .unwrap();
    let blob = BackendStream::new(futures::stream::iter([
        Ok(bytes::Bytes::from_static(b"partial")),
        Err(std::io::Error::other("injected stream failure")),
    ]));

    let event = context
        .blob_handle
        .handler
        .write_stream_to_location(location.clone(), operator, blob)
        .await;

    assert!(matches!(
        event,
        BlobEvent::Error(BlobError::StreamFailed(_))
    ));
    assert!(
        !std::path::Path::new(&location.get_full_path().unwrap()).exists(),
        "partial filesystem target remains"
    );
}

#[tokio::test]
async fn compose_close_fails() {
    let context = setup_blob_handle(5).await;
    let handler = context.blob_handle.handler.clone();

    let Event::Blob(BlobEvent::WriteFinished { location: part }) = context
        .blob_handle
        .send_blob_effect(BlobEffect::Write {
            bucket: "bucket-a".to_string(),
            key: "part.bin".to_string(),
            created_by: test_user_id(),
            blob: stream_from_bytes(b"part-data"),
        })
        .await
    else {
        panic!("part write failed")
    };

    let target = BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/tmp".to_string(),
        storage_bucket: "compose-target".to_string(),
        backend_path: format!("obj/{}", Ulid::generate()),
        ulid: Ulid::generate(),
        compressed: false,
        encrypted: false,
        created_by: test_user_id(),
        created_at: SystemTime::now(),
        staging: false,
        partial: false,
        blob_size: 0,
        hashes: HashMap::new(),
    };

    let (operator, aborts) = failing_close::operator_with_aborts();
    let event = handler
        .compose_parts_to_location(target.clone(), operator, vec![part])
        .await;

    assert!(
        matches!(event, BlobEvent::Error(BlobError::WriteError(_))),
        "compose close failure must surface as an error, got {event:?}"
    );
    assert_eq!(
        bucket_load(
            &context.storage_handle,
            &BackendRef::node_default(),
            &target.storage_bucket
        )
        .await,
        0
    );
    assert_eq!(aborts.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn replication_close_fails() {
    let (operator, aborts) = failing_close::operator_with_aborts();
    let mut writer =
        crate::bao_tree::OpenDalWriter::new(&operator, "obj/replica", Duration::from_secs(5))
            .await
            .unwrap();

    iroh_io::AsyncSliceWriter::write_bytes_at(&mut writer, 0, bytes::Bytes::from_static(b"data"))
        .await
        .unwrap();

    assert!(writer.finalize().await.is_err());
    assert_eq!(aborts.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[test]
fn build_backend_path_rejects_traversal_keys() {
    let ulid = Ulid::generate();
    assert!(build_backend_path("bucket", "nested/object.bin", ulid).is_ok());

    for key in ["../escape", "../../etc/passwd", "a/../../b", "/abs/path"] {
        assert!(
            matches!(
                build_backend_path("bucket", key, ulid),
                Err(ConversionError::UnsafePath(_))
            ),
            "key {key:?} must be rejected"
        );
    }
}

#[test]
fn reserved_bucket_rejected() {
    let ulid = Ulid::generate();
    for bucket in ["_jobs", "./_jobs"] {
        assert!(matches!(
            build_backend_path(bucket, "object", ulid),
            Err(ConversionError::UnsafePath(_))
        ));
    }
}

#[test]
fn rebuild_backend_path_rejects_sender_supplied_traversal() {
    let ulid = Ulid::generate();
    assert!(rebuild_backend_path("bucket/object_0000", ulid).is_ok());

    for path in ["../../etc/cron.d/evil_00", "../escape_00", "/abs/object_00"] {
        assert!(
            matches!(
                rebuild_backend_path(path, ulid),
                Err(ConversionError::UnsafePath(_))
            ),
            "replicated path {path:?} must be rejected"
        );
    }
}

#[test]
fn get_storage_path_rejects_replicated_traversal_path() {
    let mut location = make_test_location();
    location.storage_bucket = "bucket".to_string();
    location.backend_path = "../../etc/passwd".to_string();

    assert!(matches!(
        location.get_storage_path(),
        Err(BlobError::ConversionError(ConversionError::UnsafePath(_)))
    ));
}

#[tokio::test]
async fn read_holds_permit() {
    // The read slot must stay taken until the lazily consumed stream is dropped.
    let context = setup_blob_handle(8).await;
    let Event::Blob(BlobEvent::WriteFinished { location }) = context
        .blob_handle
        .send_blob_effect(BlobEffect::Write {
            bucket: "bucket".to_string(),
            key: "object".to_string(),
            created_by: test_user_id(),
            blob: stream_from_bytes(b"payload"),
        })
        .await
    else {
        panic!("write failed")
    };

    let slots = context.blob_handle.handler.read_slots.clone();
    let free = slots.available_permits();
    let Event::Blob(BlobEvent::ReadFinished { blob, .. }) = context
        .blob_handle
        .send_blob_effect(BlobEffect::Read { location })
        .await
    else {
        panic!("read failed")
    };

    assert_eq!(slots.available_permits(), free - 1);
    drop(blob);
    assert_eq!(slots.available_permits(), free);
}

#[tokio::test]
async fn reservation_forces_rollover() {
    // With one slot per bucket, concurrent writers must roll over instead of
    // all landing in the bucket they read as free.
    let context = setup_blob_handle(1).await;
    let writers = 4;
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(writers));
    let mut tasks = Vec::new();
    for index in 0..writers {
        let handle = context.blob_handle.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            let body = BackendStream::new(futures::stream::once(async move {
                barrier.wait().await;
                Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"payload"))
            }));
            handle
                .send_blob_effect(BlobEffect::Write {
                    bucket: "bucket".to_string(),
                    key: format!("object-{index}"),
                    created_by: test_user_id(),
                    blob: body,
                })
                .await
        }));
    }

    let mut buckets = Vec::new();
    for task in tasks {
        let event = tokio::time::timeout(Duration::from_secs(30), task)
            .await
            .expect("write starved")
            .unwrap();
        let Event::Blob(BlobEvent::WriteFinished { location }) = event else {
            panic!("write failed: {event:?}")
        };
        buckets.push(location.storage_bucket);
    }

    buckets.sort();
    buckets.dedup();
    assert_eq!(buckets.len(), writers);
    for bucket in buckets {
        assert_eq!(
            bucket_load(
                &context.storage_handle,
                &BackendRef::node_default(),
                &bucket
            )
            .await,
            1
        );
    }
}
