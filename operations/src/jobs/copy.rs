use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use aruna_core::structs::checksum::HASH_BLAKE3;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::execution::job::{CopyJobSpec, JobError, JobResultPayload};

use super::executor::{JobContext, JobRunOutcome};
use crate::driver::drive;
use crate::realm::get_config::GetConfigOperation;
use crate::s3::object::copy::{
    CopyObjectError, CopyObjectInput, CopyReferences, CopySourceConditions, copy_object_tracked,
};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::s3::object::get::GetObjectError;
use crate::s3::object::head::{HeadObjectInput, HeadObjectOperation};
use crate::s3::object::put::PutObjectError;

/// How often the bytes pulled so far reach the job's progress.
const PROGRESS_TICK: Duration = Duration::from_secs(1);

/// Copies one object as the request path would, reporting the bytes read so
/// far while the source is pulled. Cancellation drops the copy unwritten.
pub async fn run_copy_job(ctx: &JobContext, spec: &CopyJobSpec) -> JobRunOutcome {
    let source = match live_bucket(ctx, &spec.source_bucket).await {
        Ok(bucket) => bucket,
        Err(outcome) => return outcome,
    };
    let dest = match live_bucket(ctx, &spec.dest_bucket).await {
        Ok(bucket) => bucket,
        Err(outcome) => return outcome,
    };
    if source.group_id != spec.source_group_id || dest.group_id != spec.group_id {
        return permanent("a bucket changed its group after the copy was queued");
    }
    let quota_ceiling = match drive(
        GetConfigOperation::new(spec.auth_context.realm_id),
        &ctx.driver,
    )
    .await
    {
        Ok(config) => config.quota.effective_group_ceiling(&spec.group_id),
        Err(error) => return retryable(error.to_string()),
    };
    if let Some(total) = source_length(ctx, spec).await {
        ctx.progress.set_total(total);
    }

    let pulled = Arc::new(AtomicU64::new(0));
    let copy = copy_object_tracked(
        &ctx.driver,
        CopyObjectInput {
            source_bucket: spec.source_bucket.clone(),
            source_key: spec.source_key.clone(),
            source_version_id: spec.source_version_id,
            source_group_id: spec.source_group_id,
            source_auth_context: spec.auth_context.clone(),
            dest_bucket: spec.dest_bucket.clone(),
            dest_key: spec.dest_key.clone(),
            user_id: spec.auth_context.user_id,
            group_id: spec.group_id,
            realm_id: spec.auth_context.realm_id,
            node_id: spec.node_id,
            quota_ceiling,
            conditions: CopySourceConditions::default(),
            metadata: None,
            restrictions: spec.auth_context.path_restrictions.clone(),
            references: CopyReferences::Materialize,
        },
        Some(pulled.clone()),
    );
    tokio::pin!(copy);
    let mut tick = tokio::time::interval(PROGRESS_TICK);
    let result = loop {
        tokio::select! {
            result = &mut copy => break result,
            _ = tick.tick() => ctx.progress.set_current(pulled.load(Ordering::Relaxed)),
            _ = ctx.cancel.cancelled() => return JobRunOutcome::Cancelled,
            _ = ctx.shutdown.cancelled() => return JobRunOutcome::Interrupted,
        }
    };
    match result {
        Ok(copied) => {
            ctx.progress.set_current(copied.size);
            JobRunOutcome::Succeeded(JobResultPayload::CopyObject {
                version_id: copied
                    .source_version_id
                    .map(|version| version.to_string())
                    .unwrap_or_default(),
                bytes: copied.size,
                blake3: copied
                    .location
                    .as_ref()
                    .and_then(|location| location.hashes.get(HASH_BLAKE3))
                    .map(hex::encode)
                    .unwrap_or_default(),
            })
        }
        Err(error) => JobRunOutcome::Failed(copy_error(error)),
    }
}

/// The source's size as described here, so the bar has an end before the
/// first byte moves. A reference answers from its cached observation.
async fn source_length(ctx: &JobContext, spec: &CopyJobSpec) -> Option<u64> {
    let head = drive(
        HeadObjectOperation::new(HeadObjectInput {
            bucket: spec.source_bucket.clone(),
            key: spec.source_key.clone(),
            version_id: spec.source_version_id,
        }),
        &ctx.driver,
    )
    .await
    .ok()?;
    head.location
        .as_ref()
        .map(|location| location.blob_size)
        .or_else(|| {
            head.source_metadata
                .as_ref()
                .map(|metadata| metadata.content_length)
        })
}

async fn live_bucket(ctx: &JobContext, bucket: &str) -> Result<BucketInfo, JobRunOutcome> {
    match drive(GetBucketOperation::new(bucket.to_string()), &ctx.driver).await {
        Ok(info) => Ok(info),
        Err(GetBucketError::NotFound) => Err(permanent("a bucket of the copy no longer exists")),
        Err(error) => Err(retryable(error.to_string())),
    }
}

/// Storage, routing and gate failures are the node's, so the job retries;
/// anything else names the source or destination and is final.
fn copy_error(error: CopyObjectError) -> JobError {
    match &error {
        CopyObjectError::Get(GetObjectError::StorageError(_))
        | CopyObjectError::Put(PutObjectError::StorageError(_))
        | CopyObjectError::Routing(_)
        | CopyObjectError::Gate(_) => JobError::retryable(error.to_string()),
        CopyObjectError::Get(_)
        | CopyObjectError::Put(_)
        | CopyObjectError::Reference(_)
        | CopyObjectError::PreconditionFailed => JobError::permanent(error.to_string()),
    }
}

fn retryable(message: impl Into<String>) -> JobRunOutcome {
    JobRunOutcome::Failed(JobError::retryable(message.into()))
}

fn permanent(message: impl Into<String>) -> JobRunOutcome {
    JobRunOutcome::Failed(JobError::permanent(message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::DriverContext;
    use crate::jobs::executor::ProgressReporter;
    use crate::s3::object::copy::test::{
        full_context, seed_bucket, spawn_reference_server, write_version,
    };
    use crate::s3::object::get::{GetObjectInput, GetObjectOperation};
    use aruna_core::UserId;
    use aruna_core::document::DocumentTarget;
    use aruna_core::effects::StorageEffect;
    use aruna_core::id::NodeId;
    use aruna_core::structs::identity::auth::{Actor, AuthContext};
    use aruna_core::structs::storage::blob::BlobVersion;
    use aruna_core::structs::execution::job::{JobErrorKind, JobId, JobProgress};
    use aruna_core::structs::execution::staging::{
        PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
    };
    use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;
    use aruna_core::structs::execution::source_access::SourceMetadata;
    use aruna_core::types::GroupId;
    use futures_util::StreamExt;
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tokio_util::sync::CancellationToken;
    use ulid::Ulid;

    const BODY: &[u8] = b"reference-bytes";

    struct Fixture {
        realm_id: RealmId,
        group_id: GroupId,
        node_id: NodeId,
        user_id: UserId,
        server: tokio::task::JoinHandle<()>,
    }

    /// A realm with one bucket holding a reference to a small HTTP source.
    async fn fixture(context: &DriverContext) -> Fixture {
        let realm_id = RealmId::from_bytes([5u8; 32]);
        let group_id = Ulid::generate();
        let node_id = context.net_handle.as_ref().unwrap().node_id();
        let user_id = UserId::local(Ulid::generate(), realm_id);
        let target = DocumentTarget::RealmConfig { realm_id };
        let actor = Actor {
            node_id,
            user_id: UserId::nil(realm_id),
            realm_id,
        };
        let config = RealmConfigDocument::default_for_realm(realm_id, Vec::new())
            .to_bytes(&actor)
            .unwrap();
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: target.storage_keyspace().to_string(),
                key: target.storage_key(),
                value: config.into(),
                txn_id: None,
            })
            .await;
        seed_bucket(context, "bucket", group_id, user_id, Vec::new()).await;
        let (endpoint, server) = spawn_reference_server(BODY).await;
        let source = VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::Http,
                public_config: HashMap::from([("endpoint".to_string(), endpoint)]),
                source_path: "folder/file.txt".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
            },
            connector_id: Some(Ulid::generate()),
        };
        let version = BlobVersion::reference(
            source,
            SourceMetadata {
                content_length: BODY.len() as u64,
                content_type: Some("text/plain".to_string()),
                etag: Some("etag-1".to_string()),
                last_modified: Some(SystemTime::UNIX_EPOCH),
                source_version: None,
            },
            SystemTime::UNIX_EPOCH,
            user_id,
            SystemTime::UNIX_EPOCH,
        );
        write_version(context, "bucket", "ref.txt", version).await;
        Fixture {
            realm_id,
            group_id,
            node_id,
            user_id,
            server,
        }
    }

    fn job_context(context: DriverContext, node_id: NodeId) -> JobContext {
        JobContext {
            driver: Arc::new(context),
            job_id: JobId::from_bytes([7u8; 16]),
            owner_node_id: node_id,
            claim_token: Ulid::generate(),
            final_attempt: false,
            cancel: CancellationToken::new(),
            shutdown: CancellationToken::new(),
            progress: ProgressReporter::from_progress(&JobProgress::new("bytes")),
        }
    }

    fn spec(fixture: &Fixture, source_bucket: &str) -> CopyJobSpec {
        CopyJobSpec {
            auth_context: AuthContext {
                user_id: fixture.user_id,
                realm_id: fixture.realm_id,
                path_restrictions: None,
                session: None,
            },
            node_id: fixture.node_id,
            source_bucket: source_bucket.to_string(),
            source_key: "ref.txt".to_string(),
            source_version_id: None,
            source_group_id: fixture.group_id,
            dest_bucket: "bucket".to_string(),
            dest_key: "dest.txt".to_string(),
            group_id: fixture.group_id,
        }
    }

    #[tokio::test]
    async fn pulls_reference_bytes() {
        let (_temp, context) = full_context().await;
        let fixture = fixture(&context).await;
        let ctx = job_context(context, fixture.node_id);

        let outcome = run_copy_job(&ctx, &spec(&fixture, "bucket")).await;
        fixture.server.abort();
        let (bytes, blake3, version_id) = match outcome {
            JobRunOutcome::Succeeded(JobResultPayload::CopyObject {
                bytes,
                blake3,
                version_id,
            }) => (bytes, blake3, version_id),
            _ => panic!("the copy job succeeds"),
        };
        assert_eq!(bytes, BODY.len() as u64);
        assert_eq!(blake3.len(), 64);
        assert!(!version_id.is_empty(), "the source version is recorded");
        let progress = ctx.progress.snapshot();
        assert_eq!((progress.current, progress.total), (bytes, Some(bytes)));

        // The source server is gone, so the bytes must be local now.
        let mut blob = drive(
            GetObjectOperation::new(GetObjectInput {
                bucket: "bucket".to_string(),
                key: "dest.txt".to_string(),
                version_id: None,
                range: None,
                group_id: fixture.group_id,
                user_identity: fixture.user_id,
                node_id: fixture.node_id,
            }),
            &ctx.driver,
        )
        .await
        .unwrap()
        .blob;
        let mut read = Vec::new();
        while let Some(chunk) = blob.next().await {
            read.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(read, BODY);
    }

    #[tokio::test]
    async fn missing_bucket_fails() {
        let (_temp, context) = full_context().await;
        let fixture = fixture(&context).await;
        let ctx = job_context(context, fixture.node_id);

        let outcome = run_copy_job(&ctx, &spec(&fixture, "absent")).await;
        fixture.server.abort();
        match outcome {
            JobRunOutcome::Failed(error) => assert_eq!(error.kind, JobErrorKind::Permanent),
            _ => panic!("a missing bucket is a final failure"),
        }
    }

    #[tokio::test]
    async fn cancel_drops_copy() {
        let (_temp, context) = full_context().await;
        let fixture = fixture(&context).await;
        let ctx = job_context(context, fixture.node_id);
        ctx.cancel.cancel();

        let outcome = run_copy_job(&ctx, &spec(&fixture, "bucket")).await;
        fixture.server.abort();
        assert!(matches!(outcome, JobRunOutcome::Cancelled));
        let missing = drive(
            GetObjectOperation::new(GetObjectInput {
                bucket: "bucket".to_string(),
                key: "dest.txt".to_string(),
                version_id: None,
                range: None,
                group_id: fixture.group_id,
                user_identity: fixture.user_id,
                node_id: fixture.node_id,
            }),
            &ctx.driver,
        )
        .await;
        assert!(missing.is_err(), "nothing landed under the destination key");
    }
}
