use super::backend::{build_backend_path, rebuild_backend_path};
use super::{
    BlobHandle, BlobHandler, ControlPlaneTimeoutKind,
    control_plane::control_plane_timeout_event,
    control_plane::{
        parse_replication_init, validate_replication_init_ack, with_control_plane_timeout,
    },
};
use crate::messages::{MessageType, ReplicationMessage};
use aruna_core::UserId;
use aruna_core::alpn::Alpn;
use aruna_core::effects::{BlobEffect, StagingSourceEffect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError};
use aruna_core::events::{BlobEvent, Event, StagingSourceEvent, StorageEvent};
use aruna_core::keyspaces::BUCKET_STATS_DB;
use aruna_core::stream::BackendStream;
use aruna_core::structs::{
    Backend, BackendConfig, BackendLocation, BlobTimeoutConfig, RealmId, ResolvedSourceAccess,
    SourceConnectorKind,
};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_storage::storage;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use tempfile::tempdir;
use ulid::Ulid;

mod failing_close {
    use opendal::raw::oio;
    use opendal::raw::{Access, AccessorInfo, OpWrite, RpWrite};
    use opendal::{Buffer, Capability, Error, ErrorKind, Metadata, Operator, OperatorBuilder};
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    #[derive(Debug)]
    pub(super) struct CloseFailsBackend {
        info: Arc<AccessorInfo>,
        aborts: Arc<AtomicUsize>,
    }

    impl CloseFailsBackend {
        fn new(aborts: Arc<AtomicUsize>) -> Self {
            let info = AccessorInfo::default();
            info.set_scheme("close_fails")
                .set_root("/")
                .set_native_capability(Capability {
                    write: true,
                    write_can_empty: true,
                    write_can_multi: true,
                    ..Default::default()
                });
            Self {
                info: info.into(),
                aborts,
            }
        }
    }

    impl Access for CloseFailsBackend {
        type Reader = ();
        type Writer = CloseFailsWriter;
        type Lister = ();
        type Deleter = ();
        type Copier = ();

        fn info(&self) -> Arc<AccessorInfo> {
            self.info.clone()
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
        let operator = OperatorBuilder::new(CloseFailsBackend::new(aborts.clone())).finish();
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

async fn bucket_load(storage_handle: &storage::StorageHandle, bucket: &str) -> u64 {
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: BUCKET_STATS_DB.to_string(),
            key: bucket.as_bytes().to_vec().into(),
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

fn test_user_id() -> UserId {
    UserId::nil(RealmId::from_bytes([1u8; 32]))
}

fn make_test_location() -> BackendLocation {
    BackendLocation {
        root: "/tmp".to_string(),
        storage_bucket: "bucket".to_string(),
        backend_path: format!("blob/{}", Ulid::new()),
        ulid: Ulid::new(),
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
    let replication_id = Ulid::new();
    let ack = ReplicationMessage::new(replication_id, MessageType::BaoTreeInfoReceived);

    assert_eq!(validate_replication_init_ack(ack, replication_id), Ok(()));
}

#[test]
fn replication_init_ack_rejects_unexpected_message_type() {
    let replication_id = Ulid::new();
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
    let replication_id = Ulid::new();
    let wrong_id = Ulid::new();
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
    let replication_id = Ulid::new();
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
    let replication_id = Ulid::new();
    let wrong_id = Ulid::new();
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
    let replication_id = Ulid::new();
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

#[tokio::test(start_paused = true)]
async fn control_plane_timeout_reports_read_timeout() {
    let event = with_control_plane_timeout(
        tokio::time::sleep(Duration::from_millis(10)),
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
        bucket_load(&context.storage_handle, &first.storage_bucket).await,
        2
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
        bucket_load(&context.storage_handle, &first.storage_bucket).await,
        1
    );
    assert_eq!(
        bucket_load(&context.storage_handle, &second.storage_bucket).await,
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
        bucket_load(&context.storage_handle, &first.storage_bucket).await,
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
        bucket_load(&context.storage_handle, &first.storage_bucket).await,
        1
    );
}

#[tokio::test]
async fn multipart_part_bucket_is_excluded_from_bucket_stats() {
    let context = setup_blob_handle(5).await;

    let Event::Blob(BlobEvent::WriteFinished { location }) = context
        .blob_handle
        .send_blob_effect(BlobEffect::WritePart {
            upload_id: Ulid::new(),
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
        bucket_load(&context.storage_handle, "uploaded-parts").await,
        0
    );
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
    let mut handler = context.blob_handle.handler.clone();
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
    let mut handler = context.blob_handle.handler.clone();
    let (net_a, _dir_a, net_b, _dir_b) = connected_stream_pair().await;
    let peer_id = net_b.node_id();

    let explicit = Ulid::new();
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
        root: "/tmp".to_string(),
        storage_bucket: "finalization-bucket".to_string(),
        backend_path: format!("obj/{}", Ulid::new()),
        ulid: Ulid::new(),
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
        bucket_load(&context.storage_handle, &location.storage_bucket).await,
        0
    );
    assert_eq!(aborts.load(std::sync::atomic::Ordering::SeqCst), 1);
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
        root: "/tmp".to_string(),
        storage_bucket: "compose-target".to_string(),
        backend_path: format!("obj/{}", Ulid::new()),
        ulid: Ulid::new(),
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
        bucket_load(&context.storage_handle, &target.storage_bucket).await,
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
    let ulid = Ulid::new();
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
fn rebuild_backend_path_rejects_sender_supplied_traversal() {
    let ulid = Ulid::new();
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
