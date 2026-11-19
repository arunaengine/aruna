//! Runs the copy job that duplicates one object between buckets under the quota ceiling.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::jobs::{JobEffect, JobEvent, JobReport};
use aruna_core::operation::Operation;
use aruna_core::structs::checksum::HASH_BLAKE3;
use aruna_core::structs::execution::job::{CopyJobSpec, JobError, JobId, JobResultPayload};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::routing::NodeRouting;

use super::executor::JobRunOutcome;
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::s3::object::copy::{
    CopyObjectError, CopyObjectInput, CopyObjectOperation, CopyReferences, CopyResultData,
    CopySourceConditions,
};
use crate::s3::object::get::GetObjectError;
use crate::s3::object::head::{
    HeadObjectError, HeadObjectInput, HeadObjectOperation, HeadObjectResult,
};
use crate::s3::object::put::PutObjectError;

#[derive(Debug, PartialEq)]
enum CopyStep {
    Stops,
    Source,
    Dest,
    Config,
    Head,
    Total,
    Copy,
    Copied(Box<JobResultPayload>),
}

type CopyNext = Result<Next<JobRunOutcome>, IoError>;

/// Copies one object as the request path would. The bytes pulled so far go to `pulled`, which
/// the job runner reports while the source is read; a job told to stop never starts the copy.
#[derive(Debug)]
pub struct CopyJobOperation {
    spec: CopyJobSpec,
    job_id: JobId,
    routing: NodeRouting,
    pulled: Arc<AtomicU64>,
    source: Option<BucketInfo>,
    quota_ceiling: Option<u64>,
    step: CopyStep,
}

/// Two copies are equal when they stand at the same place; the shared counter is not compared.
impl PartialEq for CopyJobOperation {
    fn eq(&self, other: &Self) -> bool {
        self.spec == other.spec
            && self.job_id == other.job_id
            && self.routing == other.routing
            && self.source == other.source
            && self.quota_ceiling == other.quota_ceiling
            && self.step == other.step
    }
}

impl CopyJobOperation {
    pub fn new(
        spec: CopyJobSpec,
        job_id: JobId,
        routing: NodeRouting,
        pulled: Arc<AtomicU64>,
    ) -> Self {
        Self {
            spec,
            job_id,
            routing,
            pulled,
            source: None,
            quota_ceiling: None,
            step: CopyStep::Stops,
        }
    }

    fn call(&mut self, step: CopyStep, call: IoCall) -> CopyNext {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn bucket_call(&mut self, step: CopyStep, bucket: &str) -> CopyNext {
        let read = GetBucketOperation::new(bucket.to_string());
        self.call(step, IoCall::child(read))
    }

    fn report(&mut self, step: CopyStep, report: JobReport) -> CopyNext {
        let effect = Effect::Job(JobEffect::Report(report));
        self.call(step, IoCall::Effect(Box::new(effect)))
    }

    fn copy(&mut self) -> CopyNext {
        let spec = &self.spec;
        let copy = CopyObjectOperation::new(
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
                quota_ceiling: self.quota_ceiling,
                conditions: CopySourceConditions::default(),
                metadata: None,
                restrictions: spec.auth_context.path_restrictions.clone(),
                references: CopyReferences::Materialize,
                job_id: Some(self.job_id.as_ulid()),
            },
            self.routing.clone(),
        )
        .with_progress(self.pulled.clone());
        self.call(CopyStep::Copy, IoCall::child(copy))
    }
}

/// A bucket of the copy that is gone fails it for good; one that cannot be read now retries.
fn bucket_outcome(read: Result<BucketInfo, GetBucketError>) -> Result<BucketInfo, JobRunOutcome> {
    match read {
        Ok(info) => Ok(info),
        Err(GetBucketError::NotFound) => Err(permanent("a bucket of the copy no longer exists")),
        Err(error) => Err(retryable(error.to_string())),
    }
}

/// The source's size as described here, so the bar has an end before the first byte moves.
/// A reference answers from its cached observation.
fn source_length(head: &HeadObjectResult) -> Option<u64> {
    (head.location.as_ref())
        .map(|location| location.blob_size)
        .or_else(|| (head.source_metadata.as_ref()).map(|metadata| metadata.content_length))
}

impl Operation for CopyJobOperation {
    type Output = JobRunOutcome;
    type Error = IoError;

    fn first(&mut self) -> CopyNext {
        let stops = Effect::Job(JobEffect::Stops);
        self.call(CopyStep::Stops, IoCall::Effect(Box::new(stops)))
    }

    fn next(&mut self, result: IoResult) -> CopyNext {
        match std::mem::replace(&mut self.step, CopyStep::Stops) {
            CopyStep::Stops => match result.event()? {
                Event::Job(JobEvent::Stops(stops)) if stops.cancelled => {
                    Ok(Next::Done(JobRunOutcome::Cancelled))
                }
                Event::Job(JobEvent::Stops(stops)) if stops.shutdown => {
                    Ok(Next::Done(JobRunOutcome::Interrupted))
                }
                _ => {
                    let bucket = self.spec.source_bucket.clone();
                    self.bucket_call(CopyStep::Source, &bucket)
                }
            },
            CopyStep::Source => match bucket_outcome(result.child()?) {
                Ok(info) => {
                    self.source = Some(info);
                    let bucket = self.spec.dest_bucket.clone();
                    self.bucket_call(CopyStep::Dest, &bucket)
                }
                Err(outcome) => Ok(Next::Done(outcome)),
            },
            CopyStep::Dest => {
                let dest = match bucket_outcome(result.child()?) {
                    Ok(info) => info,
                    Err(outcome) => return Ok(Next::Done(outcome)),
                };
                let source = self.source.as_ref().ok_or(IoError::Unfinished)?;
                if source.group_id != self.spec.source_group_id
                    || dest.group_id != self.spec.group_id
                {
                    let message = "a bucket changed its group after the copy was queued";
                    return Ok(Next::Done(permanent(message)));
                }
                let config = GetConfigOperation::new(self.spec.auth_context.realm_id);
                self.call(CopyStep::Config, IoCall::child(config))
            }
            CopyStep::Config => match result.child::<RealmConfigDocument, GetConfigError>()? {
                Ok(config) => {
                    self.quota_ceiling = config.quota.effective_group_ceiling(&self.spec.group_id);
                    let head = HeadObjectOperation::new(HeadObjectInput {
                        bucket: self.spec.source_bucket.clone(),
                        key: self.spec.source_key.clone(),
                        version_id: self.spec.source_version_id,
                    });
                    self.call(CopyStep::Head, IoCall::child(head))
                }
                Err(error) => Ok(Next::Done(retryable(error.to_string()))),
            },
            CopyStep::Head => {
                let head = result.child::<HeadObjectResult, HeadObjectError>()?;
                match head.ok().as_ref().and_then(source_length) {
                    Some(total) => {
                        let report = JobReport {
                            total: Some(total),
                            ..JobReport::default()
                        };
                        self.report(CopyStep::Total, report)
                    }
                    None => self.copy(),
                }
            }
            CopyStep::Total => {
                let _ = result.event()?;
                self.copy()
            }
            CopyStep::Copy => match result.child::<CopyResultData, CopyObjectError>()? {
                Ok(copied) => {
                    let payload = JobResultPayload::CopyObject {
                        version_id: (copied.source_version_id)
                            .map(|version| version.to_string())
                            .unwrap_or_default(),
                        bytes: copied.size,
                        blake3: (copied.location.as_ref())
                            .and_then(|location| location.hashes.get(HASH_BLAKE3))
                            .map(hex::encode)
                            .unwrap_or_default(),
                    };
                    let report = JobReport {
                        current: Some(copied.size),
                        ..JobReport::default()
                    };
                    self.report(CopyStep::Copied(Box::new(payload)), report)
                }
                Err(error) => Ok(Next::Done(JobRunOutcome::Failed(copy_error(error)))),
            },
            CopyStep::Copied(payload) => {
                let _ = result.event()?;
                Ok(Next::Done(JobRunOutcome::Succeeded(*payload)))
            }
        }
    }
}

/// Storage, routing and gate failures are the node's, so the job retries;
/// anything else names the source or destination and is final.
pub fn copy_error(error: CopyObjectError) -> JobError {
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
#[path = "copy_tests.rs"]
mod call_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive, node_routing};
    use crate::effect_adapters::job::run_hosted;
    use crate::jobs::context::JobContext;
    use crate::jobs::executor::ProgressReporter;
    use crate::s3::object::copy::test::{
        full_context, seed_bucket, spawn_reference_server, write_version,
    };
    use crate::s3::object::get::{GetObjectInput, GetObjectOperation};
    use aruna_core::UserId;
    use aruna_core::document::DocumentTarget;
    use aruna_core::effects::StorageEffect;
    use aruna_core::id::NodeId;
    use aruna_core::structs::execution::job::{JobErrorKind, JobId, JobProgress};
    use aruna_core::structs::execution::source_access::SourceMetadata;
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;
    use aruna_core::structs::execution::staging::{
        PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
    };
    use aruna_core::structs::identity::auth::{Actor, AuthContext};
    use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
    use aruna_core::structs::storage::blob::BlobVersion;
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
                namespace: None,
                root: None,
            },
            connector_id: Some(Ulid::generate()),
            mirrors: Vec::new(),
        };
        let version = BlobVersion::reference(
            source,
            SourceMetadata {
                content_length: BODY.len() as u64,
                content_type: Some("text/plain".to_string()),
                etag: Some("etag-1".to_string()),
                last_modified: Some(SystemTime::UNIX_EPOCH),
                source_version: None,
                digests: Default::default(),
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
            range_turns: Default::default(),
            handlers: Default::default(),
        }
    }

    /// Runs the copy under the host of its job context, as the job runner does.
    async fn run_copy(ctx: &JobContext, spec: &CopyJobSpec) -> JobRunOutcome {
        let pulled = Arc::new(AtomicU64::new(0));
        let routing = node_routing(&ctx.driver);
        let copy = CopyJobOperation::new(spec.clone(), ctx.job_id, routing, pulled);
        run_hosted(ctx, copy).await
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

        let outcome = run_copy(&ctx, &spec(&fixture, "bucket")).await;
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
                realm_id: fixture.user_id.realm_id,
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

        let outcome = run_copy(&ctx, &spec(&fixture, "absent")).await;
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

        let outcome = run_copy(&ctx, &spec(&fixture, "bucket")).await;
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
                realm_id: fixture.user_id.realm_id,
                node_id: fixture.node_id,
            }),
            &ctx.driver,
        )
        .await;
        assert!(missing.is_err(), "nothing landed under the destination key");
    }
}
