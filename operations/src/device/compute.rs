//! Accepts owner-initiated local runs on a user device and reports its compute plane.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::compute::{ExecutorKind, ResourceEnvelope};

use super::local_run::{BackendState, LocalRunOperation, LocalRunsOperation, RunOutput, status_of};
pub use super::local_run::{ComputeStatus, LocalExecutionConfig, LocalExecutionError};
use crate::driver::{DriverContext, drive};

/// Accepts one local run on behalf of the device owner; see [`LocalRunOperation`].
pub async fn submit_local_execution(
    context: &DriverContext,
    config: LocalExecutionConfig,
) -> Result<RunOutput, LocalExecutionError> {
    let constraint = config.spec.executor_constraint.as_deref();
    let limits = backend_limits(context, constraint);
    let run = LocalRunOperation::new(config, limits, context.node_and_realm());
    drive(run, context).await
}

/// Reports the local compute plane and the owner's runs on it. Only the backend's health check
/// runs here, as it lives on the backend the registry holds.
pub async fn compute_status(
    context: &DriverContext,
    owner: UserId,
) -> Result<ComputeStatus, LocalExecutionError> {
    let backend = context
        .compute_handle
        .as_ref()
        .and_then(|registry| registry.select(None).cloned());
    let runs = drive(LocalRunsOperation::new(owner), context).await?;
    let plane = match &backend {
        Some(backend) => BackendState {
            kind: Some(backend.kind().as_wire()),
            limits: backend.capabilities().limits,
            failure: backend.health().await.err().map(|error| error.to_string()),
        },
        None => BackendState {
            kind: None,
            limits: ResourceEnvelope::default(),
            failure: Some("no compute backend is configured on this device".to_string()),
        },
    };
    Ok(status_of(plane, runs))
}

/// Static ceilings of the backend this request would run on.
fn backend_limits(context: &DriverContext, constraint: Option<&str>) -> Option<ResourceEnvelope> {
    let registry = context.compute_handle.as_ref()?;
    let constraint = constraint.map(ExecutorKind::from_wire);
    let backend = registry.select(constraint.as_ref())?;
    Some(backend.capabilities().limits)
}

#[cfg(test)]
mod tests {
    use super::super::local_run::tests::{REALM, input, node, owner, realm_config, run_config};
    use super::*;
    use crate::jobs::service::list_owned_jobs;
    use crate::jobs::submit::SubmitJobError;
    use aruna_core::compute::{AttemptStatus, BackendError, FenceContext};
    use aruna_core::document::DocumentTarget;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::LaunchDecline;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::id::NodeId;
    use aruna_core::keyspaces::{
        BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
        FAMILY_OUTBOX_KEYSPACE, FAMILY_RECORD_KEYSPACE, S3_BUCKET_KEYSPACE, SYNC_OUTBOX_KEYSPACE,
    };
    use aruna_core::structs::execution::job::{
        InputSource, JobPayload, OutputDestination, OutputSelection, WorkspaceMode,
    };
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
    use aruna_core::structs::storage::blob::{
        BackendLocation, BackendRef, BlobHeadKey, BlobLocationKey, BlobVersion, BucketInfo,
        CurrentVersionPointer, VersionKey,
    };
    use aruna_core::structs::storage::format::Compression;
    use aruna_core::structs::storage::format::EncodingClass;
    use aruna_core::structs::storage::format::StoredFormat;
    use aruna_core::structs::storage::node_info::NodeUrls;
    use aruna_storage::FjallStorage;
    use tempfile::tempdir;
    use tokio_util::sync::CancellationToken;
    use ulid::Ulid;

    fn test_ctx(root: &str) -> DriverContext {
        DriverContext {
            storage_handle: FjallStorage::open(root).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        }
    }

    async fn write_config(ctx: &DriverContext, config: &RealmConfigDocument, local: NodeId) {
        let actor = Actor {
            node_id: local,
            user_id: UserId::nil(REALM),
            realm_id: REALM,
        };
        let target = DocumentTarget::RealmConfig { realm_id: REALM };
        let event = ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: target.storage_keyspace().to_string(),
                key: target.storage_key(),
                value: config.to_bytes(&actor).unwrap().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    #[tokio::test]
    async fn refuses_server_node() {
        // Only a device has an owner to run local jobs for.
        let dir = tempdir().unwrap();
        let ctx = test_ctx(dir.path().to_str().unwrap());
        let local = node(1);
        write_config(&ctx, &realm_config(local, RealmNodeKind::Server), local).await;

        let error = submit_local_execution(&ctx, run_config(local, owner(2)))
            .await
            .expect_err("a server node runs no local job");

        assert!(matches!(error, LocalExecutionError::NotADevice));
    }

    #[tokio::test]
    async fn refuses_foreign_caller() {
        let dir = tempdir().unwrap();
        let ctx = test_ctx(dir.path().to_str().unwrap());
        let local = node(1);
        let config = realm_config(local, RealmNodeKind::User { owner: owner(2) });
        write_config(&ctx, &config, local).await;

        let error = submit_local_execution(&ctx, run_config(local, owner(3)))
            .await
            .expect_err("only the enrolled owner runs jobs here");

        assert!(matches!(error, LocalExecutionError::NotOwner));
    }

    #[tokio::test]
    async fn refuses_while_paused() {
        // Pause stops local compute like it stops serving and syncing.
        let dir = tempdir().unwrap();
        let ctx = test_ctx(dir.path().to_str().unwrap());
        let local = node(1);
        let config = realm_config(local, RealmNodeKind::User { owner: owner(2) });
        write_config(&ctx, &config, local).await;
        crate::node::node_info::set_operator_drain(&ctx, local, REALM, true)
            .await
            .unwrap();

        let error = submit_local_execution(&ctx, run_config(local, owner(2)))
            .await
            .expect_err("a paused plane accepts nothing");

        assert!(matches!(error, LocalExecutionError::Paused));
    }

    struct StubBackend;

    #[async_trait::async_trait]
    impl aruna_compute::ExecutorBackend for StubBackend {
        fn kind(&self) -> ExecutorKind {
            ExecutorKind::Docker
        }
        fn capabilities(&self) -> aruna_compute::executor::BackendCaps {
            aruna_compute::executor::BackendCaps {
                file_staging: true,
                local_site: true,
                limits: ResourceEnvelope {
                    max_concurrent: Some(1),
                    ..ResourceEnvelope::default()
                },
                ..Default::default()
            }
        }
        fn run_identity(&self) -> aruna_core::compute::UserSpec {
            aruna_core::compute::NOBODY
        }
        async fn health(&self) -> Result<(), BackendError> {
            Ok(())
        }
        async fn resolve_image(
            &self,
            image: &str,
            _cancel: &CancellationToken,
        ) -> Result<String, BackendError> {
            Ok(image.to_string())
        }
        async fn fence(&self, _context: &FenceContext) -> Result<(), BackendError> {
            unimplemented!()
        }
        async fn submit(
            &self,
            _context: &FenceContext,
            _spec: &aruna_core::compute::TaskSpec,
            _cancel: &CancellationToken,
        ) -> Result<AttemptStatus, BackendError> {
            unimplemented!()
        }
        async fn status(&self, _context: &FenceContext) -> Result<AttemptStatus, BackendError> {
            unimplemented!()
        }
        async fn cancel(
            &self,
            _context: &FenceContext,
        ) -> Result<aruna_core::compute::CancelEvidence, BackendError> {
            unimplemented!()
        }
        async fn fetch_logs(
            &self,
            _context: &FenceContext,
            _limits: &aruna_core::compute::LogLimits,
        ) -> Result<aruna_core::compute::LogTails, BackendError> {
            unimplemented!()
        }
        async fn fetch_output(
            &self,
            _context: &FenceContext,
            _path: &str,
        ) -> Result<aruna_core::compute::TaskOutput, BackendError> {
            unimplemented!()
        }
        async fn reconcile(
            &self,
            _context: &FenceContext,
        ) -> aruna_core::compute::ReconcileEvidence {
            unimplemented!()
        }
        async fn cleanup(&self, _context: &FenceContext) -> Result<(), BackendError> {
            unimplemented!()
        }
    }

    fn device_ctx(root: &str) -> DriverContext {
        DriverContext {
            compute_handle: Some(std::sync::Arc::new(
                aruna_compute::ExecutorRegistry::new()
                    .with_backend(std::sync::Arc::new(StubBackend)),
            )),
            ..test_ctx(root)
        }
    }

    async fn rows(ctx: &DriverContext, key_space: &str) -> usize {
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: key_space.to_string(),
                prefix: None,
                start: None,
                limit: 16,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values.len(),
            other => panic!("unexpected iter result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn keeps_runs_local() {
        // The device owns the job and tells nobody: no outbox row, no family
        // record, and a second run is refused by the owner's own ceiling.
        let dir = tempdir().unwrap();
        let ctx = device_ctx(dir.path().to_str().unwrap());
        let local = node(1);
        let caller = owner(2);
        let config = realm_config(local, RealmNodeKind::User { owner: caller });
        write_config(&ctx, &config, local).await;

        let accepted = submit_local_execution(&ctx, run_config(local, caller))
            .await
            .expect("the owner's run is accepted");

        assert!(accepted.created);
        let (records, _) = list_owned_jobs(&ctx, caller, None, 8, |_| true)
            .await
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].owner_node_id, local);
        assert_eq!(records[0].created_by, caller);
        assert_eq!(rows(&ctx, SYNC_OUTBOX_KEYSPACE).await, 0);
        assert_eq!(rows(&ctx, FAMILY_RECORD_KEYSPACE).await, 0);
        assert_eq!(rows(&ctx, FAMILY_OUTBOX_KEYSPACE).await, 0);

        let error = submit_local_execution(&ctx, run_config(local, caller))
            .await
            .expect_err("the second run exceeds the device ceiling");
        assert!(matches!(
            error,
            LocalExecutionError::Submit(SubmitJobError::ActiveJobLimit { limit }) if limit == 1
        ));
    }

    #[tokio::test]
    async fn refuses_device_launch() {
        // The realm's own launch path declines a device whatever it advertises,
        // so local compute never becomes a dispatch target.
        use crate::tests::records::Family;
        let dir = tempdir().unwrap();
        let ctx = test_ctx(dir.path().to_str().unwrap());
        let local = node(1);
        let config = realm_config(local, RealmNodeKind::User { owner: owner(2) });
        write_config(&ctx, &config, local).await;
        crate::node::node_info::seed_info_document(
            &ctx,
            local,
            REALM,
            NodeUrls {
                api: None,
                s3: None,
            },
        )
        .await
        .unwrap();

        let family = Family::new([8u8; 32]);
        let spec = family.spec();
        let intent = family.launch(&spec, local, 0);
        let declined =
            crate::tests::records::local_capability(&ctx, &config, local, &intent, &spec)
                .await
                .expect_err("a device is never a launch target");

        assert!(matches!(declined, LaunchDecline::Unauthorized));
    }

    #[tokio::test]
    async fn pins_output_destination() {
        // A declared output with no endpoint fails capture mid-run, so the
        // device names itself as the writer before the job exists.
        let dir = tempdir().unwrap();
        let ctx = device_ctx(dir.path().to_str().unwrap());
        let local = node(1);
        let caller = owner(2);
        let config = realm_config(local, RealmNodeKind::User { owner: caller });
        write_config(&ctx, &config, local).await;
        let mut run = run_config(local, caller);
        run.spec.file_outputs.push(OutputSelection {
            container_path: "/out/report.txt".to_string(),
            path_prefix: None,
            destination_node_id: None,
            destination: OutputDestination::S3 {
                bucket: "results".to_string(),
                key: "report.txt".to_string(),
            },
            name: None,
            description: None,
        });

        submit_local_execution(&ctx, run)
            .await
            .expect("the owner's run is accepted");

        let (records, _) = list_owned_jobs(&ctx, caller, None, 8, |_| true)
            .await
            .unwrap();
        let JobPayload::Execution(spec) = &records[0].payload else {
            panic!("expected an execution payload");
        };
        assert_eq!(spec.file_outputs[0].destination_node_id, Some(local));
    }

    /// One readable local object, seeded straight into the keyspaces a head read
    /// walks: bucket, current version pointer, version, and blob location.
    async fn seed_object(ctx: &DriverContext, bucket: &str, key: &str, version: Ulid) {
        let user = owner(2);
        let hash = [17u8; 32];
        let location = BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/data".to_string(),
            storage_bucket: bucket.to_string(),
            backend_path: key.to_string(),
            ulid: version,
            format: StoredFormat::default(),
            created_by: user,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 4,
            hashes: std::collections::HashMap::new(),
        };
        let info = BucketInfo {
            group_id: Ulid::from_bytes([6u8; 16]),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: user,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };
        for (key_space, row_key, value) in [
            (
                S3_BUCKET_KEYSPACE,
                bucket.as_bytes().to_vec(),
                info.to_bytes().unwrap(),
            ),
            (
                BLOB_HEAD_KEYSPACE,
                BlobHeadKey::new(bucket, key).to_bytes().unwrap(),
                CurrentVersionPointer::new(version).to_bytes().unwrap(),
            ),
            (
                BLOB_VERSIONS_KEYSPACE,
                VersionKey::new(bucket, key, version).to_bytes().unwrap(),
                BlobVersion::materialized(
                    hash,
                    BackendRef::node_default(),
                    EncodingClass::Raw,
                    std::time::SystemTime::UNIX_EPOCH,
                    user,
                    None,
                )
                .to_bytes()
                .unwrap(),
            ),
            (
                BLOB_LOCATIONS_KEYSPACE,
                BlobLocationKey::new(hash, EncodingClass::Raw, BackendRef::node_default())
                    .to_bytes(),
                location.to_bytes().unwrap(),
            ),
        ] {
            let _ = ctx
                .storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: key_space.to_string(),
                    key: row_key.into(),
                    value: value.into(),
                    txn_id: None,
                })
                .await;
        }
    }

    #[tokio::test]
    async fn runs_without_bucket() {
        // A device run owns no bucket, and an input the request left open is
        // pinned to the version it resolves to at accept time.
        let dir = tempdir().unwrap();
        let ctx = device_ctx(dir.path().to_str().unwrap());
        let local = node(1);
        let caller = owner(2);
        write_config(
            &ctx,
            &realm_config(local, RealmNodeKind::User { owner: caller }),
            local,
        )
        .await;
        let version = Ulid::from_bytes([21u8; 16]);
        seed_object(&ctx, "project", "reads.fastq", version).await;
        let mut run = run_config(local, caller);
        run.spec.inputs.push(input("project", None, None));

        submit_local_execution(&ctx, run)
            .await
            .expect("the owner's run is accepted");

        let (records, _) = list_owned_jobs(&ctx, caller, None, 8, |_| true)
            .await
            .unwrap();
        assert_eq!(records[0].workspace_mode, WorkspaceMode::None);
        assert_eq!(records[0].workspace_bucket, None);
        let JobPayload::Execution(spec) = &records[0].payload else {
            panic!("expected an execution payload");
        };
        let InputSource::S3 { version_id, .. } = &spec.inputs[0].source;
        assert_eq!(version_id.as_deref(), Some(version.to_string().as_str()));
    }
}
