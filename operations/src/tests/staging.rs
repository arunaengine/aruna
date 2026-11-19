//! Sets up a driver context with blob and net handles plus test buckets and connectors.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::connectors::create_connector::{SourceConnectorInput, SourceConnectorOperation};
use crate::driver::{DriverContext, drive};
use crate::s3::bucket::create::CreateBucketOperation;
use aruna_blob::blob::{BackendRegistry, BlobHandler};
use aruna_core::UserId;
use aruna_core::egress::EgressPolicy;
use aruna_core::structs::execution::source_access::{
    ResolvedSourceAccess, ResolvedSourceConnector, SourceMetadata,
};
use aruna_core::structs::execution::source_connector::{SourceConnector, SourceConnectorKind};
use aruna_core::structs::storage::blob::{Backend, BackendConfig, BucketInfo};
use aruna_core::structs::storage::format::Compression;
use aruna_net::NetHandle;
use aruna_storage::storage;
use std::collections::HashMap;
use std::time::SystemTime;
use tempfile::TempDir;
use ulid::Ulid;

pub(crate) struct StagingTestContext {
    pub(crate) _tempdir: TempDir,
    pub(crate) driver_context: DriverContext,
}

pub(crate) async fn setup_driver_context() -> StagingTestContext {
    let tempdir = tempfile::tempdir().expect("tempdir must be created");
    let temp_root = tempdir.path().to_str().expect("temp path must be utf-8");
    let blob_root = format!("{temp_root}/blobstore");
    std::fs::create_dir_all(&blob_root).expect("blob root must be created");

    let storage_handle = storage::FjallStorage::open(temp_root).expect("storage must open");
    storage_handle.open_vault(aruna_core::node_vault::NodeVaultKey::random());
    let net_handle = NetHandle::new(crate::testing::local_net(), storage_handle.clone())
        .await
        .expect("net handle must initialize");
    // Verified reads of references spool their bytes before they are served.
    let spool = Some(format!("{temp_root}/spool").into());
    let blob_handle = BlobHandler::with_registry(
        BackendRegistry::single(BackendConfig {
            backend_type: Backend::FileSystem,
            root: blob_root,
            service_config: HashMap::new(),
            bucket_prefix: Some("aruna-test-".to_string()),
            max_bucket_size: Some(1),
            multipart_bucket: Some("uploaded-parts".to_string()),
            timeouts: Default::default(),
        }),
        storage_handle.clone(),
        net_handle,
        EgressPolicy::loopback(),
        spool,
    )
    .await
    .expect("blob handle must initialize");
    let (http, policy) = blob_handle.repository_client();

    StagingTestContext {
        _tempdir: tempdir,
        driver_context: DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: Some(blob_handle),
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: Some(aruna_outbound::OutboundHandle::new(http, policy)),
        },
    }
}

pub(crate) async fn create_http_connector(
    context: &DriverContext,
    group_id: Ulid,
    endpoint: &str,
) -> SourceConnector {
    drive(
        SourceConnectorOperation::new(SourceConnectorInput {
            group_id,
            created_by: Default::default(),
            name: "http-source".to_string(),
            kind: SourceConnectorKind::Http,
            public_config: HashMap::from([("endpoint".to_string(), endpoint.to_string())]),
            secret_config: HashMap::new(),
        }),
        context,
    )
    .await
    .expect("connector creation must succeed")
    .connector
}

pub(crate) async fn create_test_bucket(
    context: &DriverContext,
    group_id: Ulid,
    created_by: UserId,
    bucket: &str,
) -> BucketInfo {
    let info = BucketInfo {
        group_id,
        created_at: std::time::SystemTime::UNIX_EPOCH,
        created_by,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    };
    drive(
        CreateBucketOperation::new(bucket.to_string(), info.clone()),
        context,
    )
    .await
    .expect("bucket creation must succeed");
    info
}

pub(crate) fn sample_connector() -> SourceConnector {
    SourceConnector::new(
        Ulid::from_bytes([2u8; 16]),
        Ulid::from_bytes([1u8; 16]),
        "staging-http".to_string(),
        SourceConnectorKind::Http,
        HashMap::from([("endpoint".to_string(), "http://127.0.0.1:1".to_string())]),
        SystemTime::UNIX_EPOCH,
        SystemTime::UNIX_EPOCH,
        Default::default(),
    )
}

pub(crate) fn sample_resolved_connector() -> ResolvedSourceConnector {
    ResolvedSourceConnector {
        connector: sample_connector(),
        secret_fingerprint: None,
        access: ResolvedSourceAccess::OpenDal {
            kind: SourceConnectorKind::Http,
            config: HashMap::from([("endpoint".to_string(), "http://127.0.0.1:1".to_string())]),
            path: "folder/file.txt".to_string(),
            version: None,
        },
    }
}

pub(crate) fn sample_metadata() -> SourceMetadata {
    SourceMetadata {
        content_length: 42,
        content_type: Some("application/octet-stream".to_string()),
        etag: Some("etag-1".to_string()),
        last_modified: Some(SystemTime::UNIX_EPOCH),
        source_version: None,
        digests: Default::default(),
    }
}

/// Runs a staging job under the host of its job context, as the job runner does, without pacing.
pub(crate) async fn run_staging_job(
    ctx: &crate::jobs::context::JobContext,
    spec: &aruna_core::structs::execution::job::StagingJobSpec,
) -> crate::jobs::executor::JobRunOutcome {
    let run = crate::jobs::staging::StagingRun {
        spec: std::sync::Arc::new(spec.clone()),
        job_id: ctx.job_id,
        claim_token: ctx.claim_token,
        routing: crate::driver::node_routing(&ctx.driver),
        seed: 7,
    };
    let job = crate::jobs::staging::StagingJobOperation::new(run);
    crate::testing::run_hosted(ctx, job).await
}
