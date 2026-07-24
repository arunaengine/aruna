use std::io::{Cursor, Read};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime};

use aruna_blob::blob::BlobHandler;
use aruna_core::effects::{BlobEffect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, BLOB_VERSIONS_KEYSPACE, REALM_CONFIG_KEYSPACE, ROCRATE_UPLOAD_KEYSPACE,
};
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::{
    Actor, AuthContext, Backend, BackendConfig, BucketInfo, ExportRoCrateSpec,
    GroupAuthorizationDocument, ImportMetadataTarget, ImportReportRow, ImportRoCrateSource,
    ImportRoCrateSpec, ImportRoCrateTarget, JobId, JobPayload, JobRecord, JobResultPayload,
    MetadataRegistryRecord, PathRestriction, Permission, RealmAuthorizationDocument,
    RealmConfigDocument, RealmId, RealmNodeKind, ReasonCode, RoCrateLimits, RoCrateMediaType,
    RoCrateUploadRecord, SourceConnectorKind, VersionKey,
};
use aruna_core::types::{GroupId, UserId};
use aruna_core::util::unix_timestamp_millis;
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::connectors::create_source_connector::{
    CreateSourceConnectorInput, CreateSourceConnectorOperation,
};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::jobs::executor::{JobContext, JobRunOutcome, ProgressReporter};
use aruna_operations::jobs::export::run_export_job;
use aruna_operations::jobs::import::{load_rocrate_upload, run_rocrate_import};
use aruna_operations::jobs::service::read_artifact_range;
use aruna_operations::jobs::store::{
    ClaimOutcome, claim_job, insert_job, list_job_entries, release_job, transition_to_running,
};
use aruna_operations::metadata::MetadataHandle;
use aruna_operations::metadata::materialization_queue::process_metadata_materialization_batch;
use aruna_operations::metadata::projector::replay_metadata_event_log;
use aruna_operations::s3::create_bucket::CreateBucketOperation;
use aruna_operations::s3::put_object::{
    PutObjectConfig, PutObjectInput, PutObjectOperation, PutObjectResult,
};
use aruna_storage::{FjallStorage, StorageHandle};
use aruna_tasks::TaskHandle;
use async_zip::{Compression, StringEncoding, ZipEntryBuilder, ZipString};
use axum::Router;
use axum::http::header;
use axum::routing::get;
use bytes::Bytes;
use byteview::ByteView;
use futures_util::io::AsyncWriteExt;
use futures_util::{StreamExt, stream};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

const BUCKET: &str = "rocrate-target";
const TARGET_KEY: &str = "imported/data.txt";
const SOURCE_KEY: &str = "sources/crate.zip";
const PAYLOAD: &[u8] = b"driver boundary payload";

struct Fixture {
    _root: TempDir,
    actor: Actor,
    group_id: GroupId,
    context: Arc<DriverContext>,
    net: NetHandle,
    gate: Option<StorageGate>,
}

struct StorageGate {
    storage: StorageHandle,
    hit: Option<oneshot::Receiver<()>>,
    release: mpsc::Sender<()>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl Fixture {
    async fn stop(mut self) {
        self.net.shutdown().await;
        if let Some(gate) = self.gate.as_mut() {
            gate.stop().await;
        }
    }
}

impl StorageGate {
    fn take_hit(&mut self) -> oneshot::Receiver<()> {
        self.hit.take().expect("version gate receiver exists")
    }

    fn release(&self) {
        self.release.send(()).expect("version gate releases");
    }

    async fn stop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let _ = self
            .storage
            .send_storage_effect(StorageEffect::SyncAll)
            .await;
        if let Some(thread) = self.thread.take() {
            thread.join().expect("storage proxy exits");
        }
    }
}

#[tokio::test]
async fn drivers_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let document_id = Ulid::generate();
    let upload_id = create_upload(&fixture, crate_archive().await?).await?;
    let import = import_spec(&fixture, upload_id, document_id);
    let import_job = JobId::new();
    let import_context = claim_context(
        &fixture,
        import_job,
        JobPayload::ImportRoCrate(import.clone()),
    )
    .await?;

    let imported = run_rocrate_import(&import_context, &import).await;
    let JobRunOutcome::Succeeded(JobResultPayload::ImportRoCrate(result)) = imported else {
        return Err("RO-Crate import did not succeed".into());
    };
    assert_eq!(result.imported, 1);
    assert_eq!(result.failed, 0);

    assert_eq!(
        replay_metadata_event_log(fixture.context.as_ref()).await?,
        1
    );
    let materialized = process_metadata_materialization_batch(fixture.context.as_ref()).await?;
    assert_eq!(materialized.processed, 1);

    let export = ExportRoCrateSpec {
        auth_context: AuthContext {
            user_id: fixture.actor.user_id,
            realm_id: fixture.actor.realm_id,
            path_restrictions: None,
        },
        document_id,
        limits: RoCrateLimits::default(),
    };
    let export_job = JobId::new();
    let export_context = claim_context(
        &fixture,
        export_job,
        JobPayload::ExportRoCrate(export.clone()),
    )
    .await?;
    let exported = run_export_job(&export_context, &export).await;
    let JobRunOutcome::Succeeded(JobResultPayload::ExportRoCrate(result)) = exported else {
        return Err("RO-Crate export did not succeed".into());
    };
    assert_eq!(result.included, 1);
    assert_eq!(result.omitted, Default::default());
    let artifact = result.artifact.ok_or("export artifact is missing")?;
    let bytes = artifact_bytes(&fixture, &artifact).await?;
    let mut archive = zip::ZipArchive::new(Cursor::new(bytes))?;
    let mut payload = Vec::new();
    archive.by_name("data.txt")?.read_to_end(&mut payload)?;
    assert_eq!(payload, PAYLOAD);
    assert!(archive.by_name("ro-crate-metadata.json").is_ok());

    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn report_rows_coexist() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let payload_path = "signature/ro-crate-metadata.json.minisig";
    let target_key = format!("imported/{payload_path}");
    let json = crate_json().replace("data.txt", payload_path);
    let mut archive = async_zip::base::write::ZipFileWriter::new(Vec::new());
    archive
        .write_entry_whole(
            ZipEntryBuilder::new(
                "ro-crate-metadata.json".to_string().into(),
                Compression::Stored,
            ),
            json.as_bytes(),
        )
        .await?;
    archive
        .write_entry_whole(
            ZipEntryBuilder::new(
                "ro-crate-metadata.json.minisig".to_string().into(),
                Compression::Stored,
            ),
            b"untrusted signature",
        )
        .await?;
    archive
        .write_entry_whole(
            ZipEntryBuilder::new(payload_path.to_string().into(), Compression::Deflate),
            PAYLOAD,
        )
        .await?;

    let document_id = Ulid::generate();
    let upload_id = create_upload(&fixture, archive.close().await?).await?;
    let spec = import_spec(&fixture, upload_id, document_id);
    let job_id = JobId::new();
    let context = claim_context(&fixture, job_id, JobPayload::ImportRoCrate(spec.clone())).await?;
    let JobRunOutcome::Succeeded(JobResultPayload::ImportRoCrate(result)) =
        run_rocrate_import(&context, &spec).await
    else {
        return Err("RO-Crate collision import did not succeed".into());
    };
    assert_eq!(result.imported, 1);
    assert_eq!(result.unlisted, 0);

    let rows = read_rows(&fixture, job_id).await?;
    let matching = rows
        .iter()
        .filter(|row| row.entry_key == payload_path)
        .collect::<Vec<_>>();
    assert_eq!(matching.len(), 2);
    let dropped = matching
        .iter()
        .find(|row| row.code == ReasonCode::SignatureDropped)
        .ok_or("signature report row is missing")?;
    assert_eq!(
        dropped.detail.archive_path,
        "ro-crate-metadata.json.minisig"
    );
    assert_eq!(dropped.detail.target_key, None);
    assert_eq!(dropped.detail.version_id, None);
    let imported = matching
        .iter()
        .find(|row| row.code == ReasonCode::Imported)
        .ok_or("payload report row is missing")?;
    assert_eq!(imported.detail.archive_path, payload_path);
    assert_eq!(
        imported.detail.target_key.as_deref(),
        Some(target_key.as_str())
    );
    assert!(imported.detail.version_id.is_some());
    assert_eq!(imported.detail.size, Some(PAYLOAD.len() as u64));
    assert!(imported.detail.blake3.is_some());
    assert!(imported.detail.arn.is_some());
    assert!(imported.detail.w3id.is_some());

    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn rejects_name_mismatch() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let mut archive = crate_archive().await?;
    let local = archive
        .windows(4)
        .position(|bytes| bytes == b"PK\x03\x04")
        .ok_or("local ZIP header is missing")?;
    archive[local + 30] ^= 1;

    let message = run_rejected_import(&fixture, archive, RoCrateLimits::default()).await?;

    assert!(
        message.contains("local file header name did not match the central directory name"),
        "{message}"
    );
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn rejects_descriptor_flag() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let mut archive = crate_archive().await?;
    let local = archive
        .windows(4)
        .position(|bytes| bytes == b"PK\x03\x04")
        .ok_or("local ZIP header is missing")?;
    archive[local + 6] ^= 0x08;

    let message = run_rejected_import(&fixture, archive, RoCrateLimits::default()).await?;

    assert!(
        message.contains(
            "local file header data-descriptor flag did not match the central directory flag"
        ),
        "{message}"
    );
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn rejects_directory_limit() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let mut archive = crate_archive().await?;
    let central = archive
        .windows(4)
        .position(|bytes| bytes == b"PK\x01\x02")
        .ok_or("central ZIP header is missing")?;
    archive[central] = 0;
    let limits = RoCrateLimits {
        max_entries: 2,
        key_bytes: 1,
        ..RoCrateLimits::default()
    };
    let directory_limit = limits.max_entries * (46 + limits.key_bytes * 2);

    let message = run_rejected_import(&fixture, archive, limits).await?;

    assert_eq!(
        message,
        format!("ZIP central directory exceeds limit {directory_limit}")
    );
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn rejects_encrypted_zip() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let mut archive = crate_archive().await?;
    let local = zip_offsets(&archive, b"PK\x03\x04")[0];
    let central = zip_offsets(&archive, b"PK\x01\x02")[0];
    archive[local + 6] |= 0x01;
    archive[central + 8] |= 0x01;

    let message = run_rejected_import(&fixture, archive, RoCrateLimits::default()).await?;

    assert!(
        message.to_ascii_lowercase().contains("encryption"),
        "{message}"
    );
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn rejects_malformed_cd() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let mut archive = crate_archive().await?;
    let central = zip_offsets(&archive, b"PK\x01\x02")[0];
    archive[central] = 0;

    let message = run_rejected_import(&fixture, archive, RoCrateLimits::default()).await?;

    assert!(message.contains("invalid ZIP archive"), "{message}");
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn rejects_size_bomb() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let mut archive = crate_archive().await?;
    let local = zip_offsets(&archive, b"PK\x03\x04")[1];
    let central = zip_offsets(&archive, b"PK\x01\x02")[1];
    write_u32(&mut archive, local + 22, 1);
    write_u32(&mut archive, central + 24, 1);

    let message = run_rejected_import(&fixture, archive, RoCrateLimits::default()).await?;

    assert!(
        message.contains("expanded size") || message.contains("content length"),
        "{message}"
    );
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn rejects_crc_mismatch() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let mut archive = crate_archive().await?;
    let local = zip_offsets(&archive, b"PK\x03\x04")[1];
    let central = zip_offsets(&archive, b"PK\x01\x02")[1];
    write_u32(&mut archive, local + 14, 0);
    write_u32(&mut archive, central + 16, 0);

    let message = run_rejected_import(&fixture, archive, RoCrateLimits::default()).await?;

    assert!(
        message.to_ascii_lowercase().contains("checksum"),
        "{message}"
    );
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn rejects_special_files() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    for mode in [0o120777, 0o060600] {
        let archive = custom_archive(
            crate_json(),
            ZipEntryBuilder::new("data.txt".to_string().into(), Compression::Stored)
                .unix_permissions(mode),
            false,
        )
        .await?;
        let message = run_rejected_import(&fixture, archive, RoCrateLimits::default()).await?;
        assert!(message.contains("not a regular file"), "{message}");
    }
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn rejects_invalid_utf8() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let archive = custom_archive(
        crate_json(),
        ZipEntryBuilder::new(
            ZipString::new(b"data-\xff.txt".to_vec(), StringEncoding::Raw),
            Compression::Stored,
        ),
        false,
    )
    .await?;

    let message = run_rejected_import(&fixture, archive, RoCrateLimits::default()).await?;

    assert!(message.contains("archive path is not UTF-8"), "{message}");
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn accepts_data_descriptor() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let archive = custom_archive(
        crate_json(),
        ZipEntryBuilder::new("data.txt".to_string().into(), Compression::Deflate),
        true,
    )
    .await?;

    let JobRunOutcome::Succeeded(JobResultPayload::ImportRoCrate(result)) =
        run_import(&fixture, archive, RoCrateLimits::default()).await?
    else {
        return Err("data-descriptor RO-Crate import did not succeed".into());
    };
    assert_eq!(result.imported, 1);
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn accepts_unicode_path() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let unicode_path = "dátá.txt";
    let archive = custom_archive(
        crate_json().replace("data.txt", unicode_path),
        ZipEntryBuilder::new(
            ZipString::new_with_alternative(unicode_path.to_string(), b"data.txt".to_vec()),
            Compression::Stored,
        ),
        false,
    )
    .await?;

    let JobRunOutcome::Succeeded(JobResultPayload::ImportRoCrate(result)) =
        run_import(&fixture, archive, RoCrateLimits::default()).await?
    else {
        return Err("Unicode-path RO-Crate import did not succeed".into());
    };
    assert_eq!(result.imported, 1);
    assert_eq!(result.unlisted, 0);
    assert_eq!(result.failed, 0);
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn local_denial_omits() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let document_id = Ulid::generate();
    let upload_id = create_upload(&fixture, crate_archive().await?).await?;
    let import = import_spec(&fixture, upload_id, document_id);
    let import_context = claim_context(
        &fixture,
        JobId::new(),
        JobPayload::ImportRoCrate(import.clone()),
    )
    .await?;
    if !matches!(
        run_rocrate_import(&import_context, &import).await,
        JobRunOutcome::Succeeded(JobResultPayload::ImportRoCrate(_))
    ) {
        return Err("RO-Crate import did not succeed".into());
    }
    assert_eq!(
        replay_metadata_event_log(fixture.context.as_ref()).await?,
        1
    );
    assert_eq!(
        process_metadata_materialization_batch(fixture.context.as_ref())
            .await?
            .processed,
        1
    );

    let metadata_path = MetadataRegistryRecord::permission_path_for(
        &fixture.actor.realm_id,
        fixture.group_id,
        &import.metadata.path,
        document_id,
    );
    let export = ExportRoCrateSpec {
        auth_context: AuthContext {
            user_id: fixture.actor.user_id,
            realm_id: fixture.actor.realm_id,
            path_restrictions: Some(vec![PathRestriction {
                pattern: metadata_path,
                permission: Permission::READ,
            }]),
        },
        document_id,
        limits: RoCrateLimits::default(),
    };
    let export_context = claim_context(
        &fixture,
        JobId::new(),
        JobPayload::ExportRoCrate(export.clone()),
    )
    .await?;
    let JobRunOutcome::Succeeded(JobResultPayload::ExportRoCrate(result)) =
        run_export_job(&export_context, &export).await
    else {
        return Err("restricted RO-Crate export did not succeed".into());
    };
    assert_eq!(result.included, 0);
    assert_eq!(result.omitted.denied, 1);
    let artifact = result.artifact.ok_or("export artifact is missing")?;
    let bytes = artifact_bytes(&fixture, &artifact).await?;
    let mut archive = zip::ZipArchive::new(Cursor::new(bytes))?;
    assert!(archive.by_name("data.txt").is_err());

    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn write_restart_dedupes() -> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = build_fixture(true).await?;
    let document_id = Ulid::generate();
    let upload_id = create_upload(&fixture, crate_archive().await?).await?;
    let spec = import_spec(&fixture, upload_id, document_id);
    let job_id = JobId::new();
    let first_context =
        claim_context(&fixture, job_id, JobPayload::ImportRoCrate(spec.clone())).await?;
    let first_token = first_context.claim_token;
    let mut first_run = tokio::spawn({
        let spec = spec.clone();
        async move { run_rocrate_import(&first_context, &spec).await }
    });
    let mut hit = fixture.gate.as_mut().expect("gated fixture").take_hit();

    let gate_result = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::select! {
            result = &mut first_run => Err(format!(
                "import ended before the committed version gate: {}",
                run_name(result)
            )),
            result = &mut hit => {
                result.map_err(|error| error.to_string())?;
                Ok(())
            }
        }
    })
    .await
    .map_err(|_| "import did not reach the committed version gate")?;
    gate_result?;

    first_run.abort();
    let _ = first_run.await;
    fixture.gate.as_ref().expect("gated fixture").release();

    let fenced = read_rows(&fixture, job_id).await?;
    let fenced = fenced
        .iter()
        .find(|row| row.detail.target_key.as_deref() == Some(TARGET_KEY))
        .ok_or("preassigned version report row is missing")?;
    let version_id = fenced
        .detail
        .version_id
        .ok_or("preassigned version fence is missing")?;
    assert_eq!(fenced.detail.blake3, None);
    assert_eq!(version_keys(&fixture).await?, vec![version_id]);

    release_job(
        &fixture.context.storage_handle,
        job_id,
        first_token,
        unix_timestamp_millis(),
    )
    .await?;
    let second_context =
        claim_context(&fixture, job_id, JobPayload::ImportRoCrate(spec.clone())).await?;
    assert_ne!(second_context.claim_token, first_token);

    let resumed = run_rocrate_import(&second_context, &spec).await;
    let JobRunOutcome::Succeeded(JobResultPayload::ImportRoCrate(result)) = resumed else {
        return Err("resumed RO-Crate import did not succeed".into());
    };
    assert_eq!(result.imported, 1);
    assert_eq!(version_keys(&fixture).await?, vec![version_id]);
    let rows = read_rows(&fixture, job_id).await?;
    let imported = rows
        .iter()
        .find(|row| row.detail.target_key.as_deref() == Some(TARGET_KEY))
        .ok_or("completed import report row is missing")?;
    assert_eq!(imported.code, ReasonCode::Imported);
    assert_eq!(imported.detail.version_id, Some(version_id));
    assert!(imported.detail.blake3.is_some());

    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn object_source_imports() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let source_version = put_object(&fixture, SOURCE_KEY, crate_archive().await?).await?;
    let document_id = Ulid::generate();
    let source = ImportRoCrateSource::Object {
        bucket: BUCKET.to_string(),
        key: SOURCE_KEY.to_string(),
        version: Some(source_version.version_id),
    };
    let spec = spec_with_source(&fixture, source, document_id);
    let job_id = JobId::new();
    let context = claim_context(&fixture, job_id, JobPayload::ImportRoCrate(spec.clone())).await?;

    let JobRunOutcome::Succeeded(JobResultPayload::ImportRoCrate(result)) =
        run_rocrate_import(&context, &spec).await
    else {
        return Err("object-source RO-Crate import did not succeed".into());
    };
    assert_eq!(result.imported, 1);
    assert_eq!(result.failed, 0);
    assert_eq!(result.document_id, Some(document_id));

    let versions = object_versions(&fixture, TARGET_KEY).await?;
    assert_eq!(versions.len(), 1);
    let rows = read_rows(&fixture, job_id).await?;
    let imported = rows
        .iter()
        .find(|row| row.detail.target_key.as_deref() == Some(TARGET_KEY))
        .ok_or("imported report row is missing")?;
    assert_eq!(imported.code, ReasonCode::Imported);
    assert_eq!(imported.detail.version_id, Some(versions[0]));
    assert_eq!(hidden_count(&fixture, job_id.0).await?, 0);

    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn object_source_pins() -> Result<(), Box<dyn std::error::Error>> {
    // The acquired snapshot must win over a later overwrite of the same object.
    let fixture = build_fixture(false).await?;
    let pinned = put_object(&fixture, SOURCE_KEY, crate_archive().await?).await?;
    put_object(&fixture, SOURCE_KEY, b"corrupted archive bytes".to_vec()).await?;
    let document_id = Ulid::generate();
    let source = ImportRoCrateSource::Object {
        bucket: BUCKET.to_string(),
        key: SOURCE_KEY.to_string(),
        version: Some(pinned.version_id),
    };
    let spec = spec_with_source(&fixture, source, document_id);
    let context = claim_context(
        &fixture,
        JobId::new(),
        JobPayload::ImportRoCrate(spec.clone()),
    )
    .await?;

    let JobRunOutcome::Succeeded(JobResultPayload::ImportRoCrate(result)) =
        run_rocrate_import(&context, &spec).await
    else {
        return Err("pinned object-source import did not succeed".into());
    };
    assert_eq!(result.imported, 1);
    assert_eq!(result.document_id, Some(document_id));

    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn connector_source_imports() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let (address, server) = serve_archive(crate_archive().await?).await?;
    let connector_id = create_connector(&fixture, &format!("http://{address}")).await?;
    let document_id = Ulid::generate();
    let source = ImportRoCrateSource::Connector {
        group_id: fixture.group_id,
        connector_id,
        path: "crate.zip".to_string(),
    };
    let spec = spec_with_source(&fixture, source, document_id);
    let job_id = JobId::new();
    let context = claim_context(&fixture, job_id, JobPayload::ImportRoCrate(spec.clone())).await?;

    let outcome = run_rocrate_import(&context, &spec).await;
    server.abort();
    match outcome {
        JobRunOutcome::Succeeded(JobResultPayload::ImportRoCrate(result)) => {
            assert_eq!(result.imported, 1);
            assert_eq!(result.failed, 0);
            assert_eq!(result.document_id, Some(document_id));
        }
        JobRunOutcome::Failed(error) => {
            return Err(format!("connector-source import failed: {}", error.message).into());
        }
        _ => return Err("connector-source import did not succeed".into()),
    }
    assert_eq!(object_versions(&fixture, TARGET_KEY).await?.len(), 1);
    assert_eq!(hidden_count(&fixture, job_id.0).await?, 0);

    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn cancel_mid_write() -> Result<(), Box<dyn std::error::Error>> {
    // Cancelling after the first payload commits must freeze the remaining entries.
    let mut fixture = build_fixture(true).await?;
    let document_id = Ulid::generate();
    let upload_id = create_upload(&fixture, pair_archive().await?).await?;
    let spec = spec_with_source(
        &fixture,
        ImportRoCrateSource::Upload { upload_id },
        document_id,
    );
    let job_id = JobId::new();
    let context = claim_context(&fixture, job_id, JobPayload::ImportRoCrate(spec.clone())).await?;
    let cancel = context.cancel.clone();
    let mut run = tokio::spawn({
        let spec = spec.clone();
        async move { run_rocrate_import(&context, &spec).await }
    });
    let mut hit = fixture.gate.as_mut().expect("gated fixture").take_hit();

    tokio::time::timeout(Duration::from_secs(30), async {
        tokio::select! {
            result = &mut run => Err(format!(
                "import ended before the first committed version gate: {}",
                run_name(result)
            )),
            result = &mut hit => {
                result.map_err(|error| error.to_string())?;
                Ok(())
            }
        }
    })
    .await
    .map_err(|_| "import did not reach the first committed version gate")??;

    cancel.cancel();
    fixture.gate.as_ref().expect("gated fixture").release();
    let outcome = tokio::time::timeout(Duration::from_secs(30), run)
        .await
        .map_err(|_| "cancelled import did not terminate")??;
    if !matches!(outcome, JobRunOutcome::Cancelled) {
        return Err(format!("import was not cancelled: {}", run_name(Ok(outcome))).into());
    }

    let rows = read_rows(&fixture, job_id).await?;
    let first = rows
        .iter()
        .find(|row| row.detail.target_key.as_deref() == Some("imported/data1.txt"))
        .ok_or("first entry report row is missing")?;
    assert_eq!(first.code, ReasonCode::Imported);
    let first_version = first
        .detail
        .version_id
        .ok_or("first entry version is missing")?;
    assert!(first.detail.blake3.is_some());
    assert_eq!(
        object_versions(&fixture, "imported/data1.txt").await?,
        vec![first_version]
    );

    let second = rows
        .iter()
        .find(|row| row.detail.target_key.as_deref() == Some("imported/data2.txt"))
        .ok_or("second entry report row is missing")?;
    assert_eq!(second.code, ReasonCode::NotAttempted);
    assert!(
        second
            .message
            .as_deref()
            .is_some_and(|message| message.contains("cancelled"))
    );
    assert!(
        object_versions(&fixture, "imported/data2.txt")
            .await?
            .is_empty()
    );

    assert_eq!(hidden_count(&fixture, upload_id).await?, 0);
    assert!(
        load_rocrate_upload(&fixture.context, upload_id)
            .await?
            .is_none()
    );

    fixture.stop().await;
    Ok(())
}

async fn build_fixture(gated: bool) -> Result<Fixture, Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let storage_path = root.path().join("storage");
    let metadata_path = root.path().join("metadata");
    let blob_path = root.path().join("blob");
    std::fs::create_dir_all(&storage_path)?;
    std::fs::create_dir_all(&metadata_path)?;
    std::fs::create_dir_all(&blob_path)?;
    let direct = FjallStorage::open(storage_path.to_str().ok_or("invalid storage path")?)?;
    let (storage, gate) = if gated {
        let (storage, gate) = storage_proxy(direct)?;
        (storage, Some(gate))
    } else {
        (direct, None)
    };
    let realm_id = RealmId::from_bytes([41; 32]);
    let net = NetHandle::new(
        NetConfig {
            bind_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            realm_id,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage.clone(),
    )
    .await?;
    let actor = Actor {
        node_id: net.node_id(),
        user_id: UserId::local(Ulid::generate(), realm_id),
        realm_id,
    };
    seed_config(&storage, &actor).await?;
    let group_id = Ulid::generate();
    seed_auth(&storage, &actor, group_id).await?;
    let blob = BlobHandler::new(
        BackendConfig {
            backend_type: Backend::FileSystem,
            bucket_prefix: Some("aruna_".to_string()),
            max_bucket_size: Some(100_000),
            multipart_bucket: Some("multipart".to_string()),
            root: blob_path.to_str().ok_or("invalid blob path")?.to_string(),
            service_config: Default::default(),
            timeouts: Default::default(),
        },
        storage.clone(),
        net.clone(),
    )
    .await?;
    let metadata = MetadataHandle::new(
        &metadata_path,
        actor.node_id,
        storage.clone(),
        None,
        None,
        None,
    )?;
    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: Some(net.clone()),
        blob_handle: Some(blob),
        metadata_handle: Some(metadata),
        task_handle: Some(TaskHandle::new()),
        compute_handle: None,
    });
    create_bucket(&context, &actor, group_id).await?;
    Ok(Fixture {
        _root: root,
        actor,
        group_id,
        context,
        net,
        gate,
    })
}

fn storage_proxy(
    direct: StorageHandle,
) -> Result<(StorageHandle, StorageGate), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let (storage, receiver) = StorageHandle::new();
    let (hit_sender, hit) = oneshot::channel();
    let (release, release_receiver) = mpsc::channel();
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = stop.clone();
    let thread = std::thread::Builder::new()
        .name("rocrate-storage-gate".to_string())
        .spawn(move || {
            let mut version_transaction = None;
            let mut hit_sender = Some(hit_sender);
            while let Ok((effect, response, _span, _queued, _in_flight)) = receiver.recv() {
                if let Some(transaction) = version_txn(&effect) {
                    version_transaction = Some(transaction);
                }
                let gated_commit = match &effect {
                    StorageEffect::CommitTransaction { txn_id } => {
                        Some(*txn_id) == version_transaction
                    }
                    _ => false,
                };
                let Event::Storage(event) = runtime.block_on(direct.send_storage_effect(effect))
                else {
                    unreachable!("storage handle only returns storage events")
                };
                let committed = gated_commit
                    && matches!(
                        &event,
                        StorageEvent::TransactionCommitted { txn_id }
                            if Some(*txn_id) == version_transaction
                    );
                if committed {
                    if let Some(sender) = hit_sender.take() {
                        let _ = sender.send(());
                        let _ = release_receiver.recv();
                    }
                    version_transaction = None;
                }
                response.send(event);
                if thread_stop.load(Ordering::Acquire) {
                    break;
                }
            }
        })?;
    Ok((
        storage.clone(),
        StorageGate {
            storage,
            hit: Some(hit),
            release,
            stop,
            thread: Some(thread),
        },
    ))
}

fn version_txn(effect: &StorageEffect) -> Option<Ulid> {
    match effect {
        StorageEffect::Write {
            key_space,
            txn_id: Some(txn_id),
            ..
        } if key_space == BLOB_VERSIONS_KEYSPACE => Some(*txn_id),
        StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        } if writes
            .iter()
            .any(|(key_space, _, _)| key_space == BLOB_VERSIONS_KEYSPACE) =>
        {
            Some(*txn_id)
        }
        _ => None,
    }
}

async fn seed_config(
    storage: &StorageHandle,
    actor: &Actor,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut config = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
    config.seed_default_placement();
    config.ensure_node(actor.node_id, RealmNodeKind::Server);
    write_value(
        storage,
        REALM_CONFIG_KEYSPACE,
        actor.realm_id.as_bytes().to_vec(),
        config.to_bytes(actor)?,
    )
    .await
}

async fn seed_auth(
    storage: &StorageHandle,
    actor: &Actor,
    group_id: GroupId,
) -> Result<(), Box<dyn std::error::Error>> {
    let realm = RealmAuthorizationDocument::new_default_realm_doc(actor.realm_id);
    let group =
        GroupAuthorizationDocument::new_default_group_doc(actor.user_id, actor.realm_id, group_id);
    write_value(
        storage,
        AUTH_KEYSPACE,
        actor.realm_id.as_bytes().to_vec(),
        realm.to_bytes(actor)?,
    )
    .await?;
    write_value(
        storage,
        AUTH_KEYSPACE,
        group_id.to_bytes().to_vec(),
        group.to_bytes(actor)?,
    )
    .await
}

async fn write_value(
    storage: &StorageHandle,
    key_space: &str,
    key: Vec<u8>,
    value: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: key_space.to_string(),
            key: ByteView::from(key),
            value: ByteView::from(value),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        event => Err(format!("unexpected storage write event: {event:?}").into()),
    }
}

async fn create_bucket(
    context: &DriverContext,
    actor: &Actor,
    group_id: GroupId,
) -> Result<(), Box<dyn std::error::Error>> {
    drive(
        CreateBucketOperation::new(
            BUCKET.to_string(),
            BucketInfo {
                group_id,
                created_at: SystemTime::now(),
                created_by: actor.user_id,
                cors_configuration: None,
            },
        ),
        context,
    )
    .await?
    .ok_or("bucket operation did not finish")??;
    Ok(())
}

fn crate_json() -> String {
    serde_json::json!({
        "@context": "https://w3id.org/ro/crate/1.2/context",
        "@graph": [
            {
                "@id": "ro-crate-metadata.json",
                "@type": "CreativeWork",
                "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
                "about": {"@id": "./"}
            },
            {
                "@id": "./",
                "@type": "Dataset",
                "name": "Driver fixture",
                "description": "Exercises the job driver boundary",
                "datePublished": "2026-07-24",
                "hasPart": {"@id": "data.txt"}
            },
            {
                "@id": "data.txt",
                "@type": "File",
                "name": "Driver payload"
            }
        ]
    })
    .to_string()
}

async fn crate_archive() -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    custom_archive(
        crate_json(),
        ZipEntryBuilder::new("data.txt".to_string().into(), Compression::Deflate),
        false,
    )
    .await
}

async fn custom_archive(
    json: String,
    payload: ZipEntryBuilder,
    streamed: bool,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut archive = async_zip::base::write::ZipFileWriter::new(Vec::new());
    archive
        .write_entry_whole(
            ZipEntryBuilder::new(
                "ro-crate-metadata.json".to_string().into(),
                Compression::Stored,
            ),
            json.as_bytes(),
        )
        .await?;
    if streamed {
        let mut entry = archive.write_entry_stream(payload).await?;
        entry.write_all(PAYLOAD).await?;
        entry.close().await?;
    } else {
        archive.write_entry_whole(payload, PAYLOAD).await?;
    }
    Ok(archive.close().await?)
}

fn zip_offsets(archive: &[u8], signature: &[u8; 4]) -> Vec<usize> {
    archive
        .windows(signature.len())
        .enumerate()
        .filter_map(|(offset, bytes)| (bytes == signature).then_some(offset))
        .collect()
}

fn write_u32(archive: &mut [u8], offset: usize, value: u32) {
    archive[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

async fn create_upload(
    fixture: &Fixture,
    archive: Vec<u8>,
) -> Result<Ulid, Box<dyn std::error::Error>> {
    let upload_id = Ulid::generate();
    let size = archive.len() as u64;
    let blob = BackendStream::<Result<Bytes, StreamError>>::new(stream::once(async move {
        Ok::<Bytes, std::io::Error>(Bytes::from(archive))
    }));
    let handle = fixture
        .context
        .blob_handle
        .as_ref()
        .ok_or("blob handle is missing")?;
    let Event::Blob(aruna_core::events::BlobEvent::HiddenSpooled {
        location,
        blake3,
        size: stored_size,
    }) = handle
        .send_blob_effect(BlobEffect::SpoolHidden {
            namespace: upload_id,
            name: "input".to_string(),
            created_by: fixture.actor.user_id,
            max_bytes: Some(size),
            blob,
        })
        .await
    else {
        return Err("RO-Crate fixture spool failed".into());
    };
    let record = RoCrateUploadRecord {
        upload_id,
        owner: fixture.actor.user_id,
        location,
        blake3,
        size: stored_size,
        media_type: RoCrateMediaType::Zip,
        expires_at_ms: unix_timestamp_millis().saturating_add(60_000),
        claimed_by: None,
    };
    write_value(
        &fixture.context.storage_handle,
        ROCRATE_UPLOAD_KEYSPACE,
        upload_id.to_bytes().to_vec(),
        postcard::to_allocvec(&record)?,
    )
    .await?;
    Ok(upload_id)
}

async fn run_rejected_import(
    fixture: &Fixture,
    archive: Vec<u8>,
    limits: RoCrateLimits,
) -> Result<String, Box<dyn std::error::Error>> {
    match run_import(fixture, archive, limits).await? {
        JobRunOutcome::Failed(error) => Ok(error.message),
        _ => Err("adversarial RO-Crate import was not rejected".into()),
    }
}

async fn run_import(
    fixture: &Fixture,
    archive: Vec<u8>,
    limits: RoCrateLimits,
) -> Result<JobRunOutcome, Box<dyn std::error::Error>> {
    let document_id = Ulid::generate();
    let upload_id = create_upload(fixture, archive).await?;
    let mut spec = import_spec(fixture, upload_id, document_id);
    spec.limits = limits;
    let context = claim_context(
        fixture,
        JobId::new(),
        JobPayload::ImportRoCrate(spec.clone()),
    )
    .await?;
    Ok(run_rocrate_import(&context, &spec).await)
}

fn import_spec(fixture: &Fixture, upload_id: Ulid, document_id: Ulid) -> ImportRoCrateSpec {
    spec_with_source(
        fixture,
        ImportRoCrateSource::Upload { upload_id },
        document_id,
    )
}

fn spec_with_source(
    fixture: &Fixture,
    source: ImportRoCrateSource,
    document_id: Ulid,
) -> ImportRoCrateSpec {
    ImportRoCrateSpec {
        auth_context: AuthContext {
            user_id: fixture.actor.user_id,
            realm_id: fixture.actor.realm_id,
            path_restrictions: None,
        },
        source,
        target: ImportRoCrateTarget {
            bucket: BUCKET.to_string(),
            prefix: "imported".to_string(),
        },
        metadata: ImportMetadataTarget {
            group_id: fixture.group_id,
            path: format!("datasets/{document_id}"),
            public: false,
        },
        limits: RoCrateLimits::default(),
        document_id,
    }
}

async fn claim_context(
    fixture: &Fixture,
    job_id: JobId,
    payload: JobPayload,
) -> Result<JobContext, Box<dyn std::error::Error>> {
    if aruna_operations::jobs::store::read_job_record(&fixture.context.storage_handle, job_id, None)
        .await?
        .is_none()
    {
        let now = unix_timestamp_millis();
        insert_job(
            &fixture.context.storage_handle,
            &JobRecord::new(
                job_id,
                payload,
                fixture.actor.user_id,
                fixture.actor.node_id,
                now,
                now,
                None,
            ),
        )
        .await?;
    }
    let ClaimOutcome::Claimed(claimed) = claim_job(
        &fixture.context.storage_handle,
        job_id,
        fixture.actor.node_id,
        unix_timestamp_millis(),
    )
    .await?
    else {
        return Err("job was not claimable".into());
    };
    let token = claimed
        .claim
        .as_ref()
        .ok_or("claimed job has no claim")?
        .claim_token;
    let running = transition_to_running(
        &fixture.context.storage_handle,
        job_id,
        token,
        unix_timestamp_millis(),
    )
    .await?;
    Ok(JobContext {
        driver: fixture.context.clone(),
        job_id,
        owner_node_id: fixture.actor.node_id,
        claim_token: token,
        final_attempt: false,
        cancel: CancellationToken::new(),
        shutdown: CancellationToken::new(),
        progress: ProgressReporter::from_progress(&running.progress),
    })
}

async fn read_rows(
    fixture: &Fixture,
    job_id: JobId,
) -> Result<Vec<ImportReportRow>, Box<dyn std::error::Error>> {
    let (entries, next) =
        list_job_entries(&fixture.context.storage_handle, job_id, None, 100).await?;
    assert_eq!(next, None);
    entries
        .into_iter()
        .map(|(_, value)| postcard::from_bytes(value.as_ref()).map_err(Into::into))
        .collect()
}

async fn version_keys(fixture: &Fixture) -> Result<Vec<Ulid>, Box<dyn std::error::Error>> {
    object_versions(fixture, TARGET_KEY).await
}

async fn object_versions(
    fixture: &Fixture,
    key: &str,
) -> Result<Vec<Ulid>, Box<dyn std::error::Error>> {
    match fixture
        .context
        .storage_handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            prefix: Some(ByteView::from(VersionKey::object_prefix(BUCKET, key)?)),
            start: None,
            limit: 10,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => values
            .into_iter()
            .map(|(key, _)| {
                VersionKey::from_bytes(key.as_ref())
                    .map(|key| key.version_id)
                    .map_err(Into::into)
            })
            .collect(),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        event => Err(format!("unexpected version iteration event: {event:?}").into()),
    }
}

async fn hidden_count(
    fixture: &Fixture,
    namespace: Ulid,
) -> Result<usize, Box<dyn std::error::Error>> {
    let handle = fixture
        .context
        .blob_handle
        .as_ref()
        .ok_or("blob handle is missing")?;
    match handle
        .send_blob_effect(BlobEffect::ListHidden {
            namespace: Some(namespace),
        })
        .await
    {
        Event::Blob(aruna_core::events::BlobEvent::HiddenListed { entries }) => Ok(entries.len()),
        Event::Blob(aruna_core::events::BlobEvent::Error(error)) => Err(error.to_string().into()),
        other => Err(format!("unexpected hidden list event: {other:?}").into()),
    }
}

async fn put_object(
    fixture: &Fixture,
    key: &str,
    bytes: Vec<u8>,
) -> Result<PutObjectResult, Box<dyn std::error::Error>> {
    let size = bytes.len() as u64;
    let body = BackendStream::<Result<Bytes, StreamError>>::new(stream::once(async move {
        Ok::<Bytes, std::io::Error>(Bytes::from(bytes))
    }));
    Ok(drive(
        PutObjectOperation::new(PutObjectConfig {
            user_id: fixture.actor.user_id,
            group_id: fixture.group_id,
            realm_id: fixture.actor.realm_id,
            node_id: fixture.actor.node_id,
            request: PutObjectInput {
                bucket: BUCKET.to_string(),
                key: key.to_string(),
                content_length: Some(size),
                body: Some(body),
            },
            expected_checksums: vec![],
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: None,
            quota_ceiling: None,
        }),
        fixture.context.as_ref(),
    )
    .await?
    .ok_or("put object returned no result")??)
}

async fn create_connector(
    fixture: &Fixture,
    endpoint: &str,
) -> Result<Ulid, Box<dyn std::error::Error>> {
    let result = drive(
        CreateSourceConnectorOperation::new(CreateSourceConnectorInput {
            group_id: fixture.group_id,
            created_by: fixture.actor.user_id,
            name: "driver-http".to_string(),
            kind: SourceConnectorKind::Http,
            public_config: std::collections::HashMap::from([(
                "endpoint".to_string(),
                endpoint.to_string(),
            )]),
            secret_config: std::collections::HashMap::new(),
        }),
        fixture.context.as_ref(),
    )
    .await?;
    Ok(result.connector.connector_id)
}

async fn serve_archive(
    archive: Vec<u8>,
) -> Result<(SocketAddr, tokio::task::JoinHandle<()>), Box<dyn std::error::Error>> {
    let archive = Arc::new(archive);
    let router = Router::new().route(
        "/{*path}",
        get(move || {
            let archive = archive.clone();
            async move {
                (
                    [(header::CONTENT_TYPE, "application/zip")],
                    archive.as_ref().clone(),
                )
            }
        }),
    );
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    Ok((address, server))
}

fn pair_json() -> String {
    serde_json::json!({
        "@context": "https://w3id.org/ro/crate/1.2/context",
        "@graph": [
            {
                "@id": "ro-crate-metadata.json",
                "@type": "CreativeWork",
                "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
                "about": {"@id": "./"}
            },
            {
                "@id": "./",
                "@type": "Dataset",
                "name": "Driver pair",
                "description": "Exercises multi-entry writes",
                "datePublished": "2026-07-24",
                "hasPart": [{"@id": "data1.txt"}, {"@id": "data2.txt"}]
            },
            {"@id": "data1.txt", "@type": "File", "name": "First payload"},
            {"@id": "data2.txt", "@type": "File", "name": "Second payload"}
        ]
    })
    .to_string()
}

async fn pair_archive() -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut archive = async_zip::base::write::ZipFileWriter::new(Vec::new());
    archive
        .write_entry_whole(
            ZipEntryBuilder::new(
                "ro-crate-metadata.json".to_string().into(),
                Compression::Stored,
            ),
            pair_json().as_bytes(),
        )
        .await?;
    archive
        .write_entry_whole(
            ZipEntryBuilder::new("data1.txt".to_string().into(), Compression::Deflate),
            PAYLOAD,
        )
        .await?;
    archive
        .write_entry_whole(
            ZipEntryBuilder::new("data2.txt".to_string().into(), Compression::Deflate),
            PAYLOAD,
        )
        .await?;
    Ok(archive.close().await?)
}

async fn artifact_bytes(
    fixture: &Fixture,
    artifact: &aruna_core::structs::ArtifactRef,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut read =
        read_artifact_range(fixture.context.as_ref(), artifact, 0..artifact.size).await?;
    let mut bytes = Vec::new();
    while let Some(chunk) = read.blob.next().await {
        bytes.extend_from_slice(&chunk?);
    }
    Ok(bytes)
}

fn run_name(result: Result<JobRunOutcome, tokio::task::JoinError>) -> &'static str {
    match result {
        Ok(JobRunOutcome::Succeeded(_)) => "succeeded",
        Ok(JobRunOutcome::Failed(_)) => "failed",
        Ok(JobRunOutcome::Cancelled) => "cancelled",
        Ok(JobRunOutcome::Interrupted) => "interrupted",
        Err(_) => "panicked",
    }
}
