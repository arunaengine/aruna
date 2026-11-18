//! Materializes a staged source as a snapshot by reading it and putting the object.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::connectors::descriptor::build_source_binding;
use crate::driver::{DriverContext, node_routing};
use crate::groups::routing::{BucketRules, RoutingInputsError, SnapshotOperation};
use crate::placement::policy::{GateContext, GateContextError, GateOperation};
use crate::s3::object::put::{
    PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation, PutObjectResult,
};
use crate::staging::read_source::{
    ReadSourceError, ReadSourceInput, ReadSourceOperation, ReadSourceResult,
};
use crate::staging::snapshot_reuse::{Reusable, SnapshotReuseError, SnapshotReuseOperation};
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::checksum::ExpectedChecksum;
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::source_connector::SourceConnector;
use aruna_core::structs::execution::staging::{StagingStrategy, VersionSourceBinding};
use aruna_core::structs::identity::auth::PathRestriction;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::origin::VersionOrigin;
use aruna_core::structs::storage::routing::{NodeRouting, RoutingSnapshot};
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::GroupId;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializeSnapshotInput {
    pub group_id: GroupId,
    pub user_id: UserId,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub connector_id: Ulid,
    pub source_path: String,
    pub bucket: String,
    pub key: String,
    pub quota_ceiling: Option<u64>,
    pub retry_key: Option<String>,
    pub expected_bucket: BucketInfo,
    pub restrictions: Option<Vec<PathRestriction>>,
    /// Digests the read bytes must match before they are kept, such as manifest digests.
    pub expected_checksums: Vec<ExpectedChecksum>,
    /// The import job that copies the file in, recorded in its origin.
    pub job_id: Option<Ulid>,
}

#[derive(Debug, PartialEq)]
pub struct MaterializeSnapshotResult {
    pub connector: SourceConnector,
    pub source_metadata: SourceMetadata,
    pub version_source: VersionSourceBinding,
    pub location: BackendLocation,
    pub version_id: Ulid,
}

use aruna_core::structs::storage::blob::BackendLocation;

#[derive(Debug, Error, PartialEq)]
pub enum MaterializeSnapshotError {
    #[error(transparent)]
    Read(#[from] ReadSourceError),
    #[error(transparent)]
    Write(#[from] PutObjectError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Routing(#[from] RoutingInputsError),
    #[error(transparent)]
    Gate(#[from] GateContextError),
}

impl From<IoError> for MaterializeSnapshotError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

/// Reads a staged source and stores it as an object; a retry finds the object it already made.
pub fn stage_snapshot_blob(
    context: &DriverContext,
    input: MaterializeSnapshotInput,
) -> StageSnapshotOperation {
    StageSnapshotOperation::routed(input, &node_routing(context))
}

/// What the pending call of staging a snapshot answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SnapshotStep {
    Read,
    Reuse,
    Routing,
    Gate,
    Put,
}

#[derive(Debug, PartialEq)]
pub struct StageSnapshotOperation {
    input: MaterializeSnapshotInput,
    routing: Option<SnapshotOperation>,
    step: SnapshotStep,
    read: Option<ReadSourceResult>,
    source: Option<(SourceConnector, SourceMetadata)>,
    version_source: Option<VersionSourceBinding>,
    snapshot: Option<RoutingSnapshot>,
}

type SnapshotTurn = Result<Next<MaterializeSnapshotResult>, MaterializeSnapshotError>;

impl StageSnapshotOperation {
    /// Stages with the routing of this node, which the caller has already read.
    pub(crate) fn routed(input: MaterializeSnapshotInput, routing: &NodeRouting) -> Self {
        let bucket = BucketRules::Named(input.bucket.clone());
        let routing = SnapshotOperation::new(routing, input.group_id, bucket);
        Self {
            input,
            routing: Some(routing),
            step: SnapshotStep::Read,
            read: None,
            source: None,
            version_source: None,
            snapshot: None,
        }
    }

    fn binding(&self) -> Result<VersionSourceBinding, IoError> {
        self.version_source.clone().ok_or(IoError::Unfinished)
    }

    fn was_read(&mut self, read: ReadSourceResult) -> SnapshotTurn {
        let input = &self.input;
        let mut binding = build_source_binding(
            StagingStrategy::Snapshot,
            &read.connector,
            &read.metadata,
            input.source_path.clone(),
            Some(input.node_id),
            Some(read.connector.connector_id),
        );
        if let Some(retry_key) = input.retry_key.as_deref() {
            let capability = format!("staging_retry:{retry_key}");
            binding.descriptor.capabilities.push(capability);
        }
        let reuse = input.retry_key.is_some().then(|| {
            SnapshotReuseOperation::new(
                input.bucket.clone(),
                input.key.clone(),
                binding.clone(),
                read.metadata.content_length,
            )
        });
        self.version_source = Some(binding);
        self.read = Some(read);
        match reuse {
            Some(reuse) => {
                self.step = SnapshotStep::Reuse;
                Ok(Next::Call(IoCall::child(reuse)))
            }
            None => self.route(),
        }
    }

    fn route(&mut self) -> SnapshotTurn {
        let routing = self.routing.take().ok_or(IoError::Unfinished)?;
        self.step = SnapshotStep::Routing;
        Ok(Next::Call(IoCall::child(routing)))
    }

    fn finish(&mut self, location: BackendLocation, version_id: Ulid) -> SnapshotTurn {
        let (connector, source_metadata) = match self.read.take() {
            Some(read) => (read.connector, read.metadata),
            None => self.source.take().ok_or(IoError::Unfinished)?,
        };
        Ok(Next::Done(MaterializeSnapshotResult {
            connector,
            source_metadata,
            version_source: self.binding()?,
            location,
            version_id,
        }))
    }

    fn put(&mut self, gate: Option<GateContext>) -> SnapshotTurn {
        let read = self.read.take().ok_or(IoError::Unfinished)?;
        let routing = self.snapshot.take().ok_or(IoError::Unfinished)?;
        let input = &self.input;
        let mut put = PutObjectOperation::new(PutObjectConfig {
            user_id: input.user_id,
            group_id: input.group_id,
            realm_id: input.realm_id,
            node_id: input.node_id,
            request: PutObjectInput {
                bucket: input.bucket.clone(),
                key: input.key.clone(),
                content_length: Some(read.metadata.content_length),
                body: Some(read.stream),
            },
            expected_checksums: input.expected_checksums.clone(),
            checksum_type: None,
            exists: false,
            version_source: self.version_source.clone(),
            preassigned_version_id: None,
            quota_ceiling: input.quota_ceiling,
            routing,
        })
        .with_bucket_guard(input.expected_bucket.clone())
        .with_restrictions(input.restrictions.clone())
        .with_version_origin(Some(VersionOrigin::Source {
            job_id: input.job_id,
        }));
        if let Some(gate) = gate {
            put = put.with_gate(gate);
        }
        self.source = Some((read.connector, read.metadata));
        self.step = SnapshotStep::Put;
        Ok(Next::Call(IoCall::child(put)))
    }
}

impl Operation for StageSnapshotOperation {
    type Output = MaterializeSnapshotResult;
    type Error = MaterializeSnapshotError;

    fn first(&mut self) -> SnapshotTurn {
        let read = ReadSourceOperation::new(ReadSourceInput {
            group_id: self.input.group_id,
            connector_id: self.input.connector_id,
            source_path: self.input.source_path.clone(),
            range: None,
            expected: None,
        });
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> SnapshotTurn {
        match self.step {
            SnapshotStep::Read => {
                self.was_read(result.child::<ReadSourceResult, ReadSourceError>()??)
            }
            SnapshotStep::Reuse => match result.child::<Reusable, SnapshotReuseError>()?? {
                Some((location, version_id)) => self.finish(location, version_id),
                None => self.route(),
            },
            SnapshotStep::Routing => {
                let snapshot = result.child::<RoutingSnapshot, RoutingInputsError>()??;
                self.snapshot = Some(snapshot);
                self.step = SnapshotStep::Gate;
                // The staging source is external, so it carries no refs of its own: only the
                // destination default governs the snapshot.
                let now = unix_timestamp_millis();
                let gate = GateOperation::new(self.input.realm_id, now);
                Ok(Next::Call(IoCall::child(gate)))
            }
            SnapshotStep::Gate => {
                self.put(result.child::<Option<GateContext>, GateContextError>()??)
            }
            SnapshotStep::Put => {
                let stored = result.child::<PutObjectResult, PutObjectError>()??;
                self.finish(stored.location, stored.version_id)
            }
        }
    }
}

impl From<SnapshotReuseError> for MaterializeSnapshotError {
    fn from(error: SnapshotReuseError) -> Self {
        match error {
            SnapshotReuseError::Storage(error) => MaterializeSnapshotError::Storage(error),
            SnapshotReuseError::Conversion(error) => MaterializeSnapshotError::Conversion(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::drive;
    use crate::tests::staging::{create_http_connector, create_test_bucket, setup_driver_context};
    use axum::Router;
    use axum::routing::get;
    use tokio::net::TcpListener;

    fn plain_input(retry_key: Option<&str>) -> MaterializeSnapshotInput {
        let realm_id = RealmId::from_bytes([6; 32]);
        let user_id = UserId::new(Ulid::from_bytes([1; 16]), realm_id);
        let group_id = Ulid::from_bytes([2; 16]);
        MaterializeSnapshotInput {
            job_id: None,
            group_id,
            user_id,
            realm_id,
            node_id: NodeId::from_bytes(&[3; 32]).unwrap(),
            connector_id: Ulid::from_bytes([4; 16]),
            source_path: "file.txt".to_string(),
            bucket: "bucket-a".to_string(),
            key: "object.txt".to_string(),
            quota_ceiling: None,
            retry_key: retry_key.map(str::to_string),
            expected_bucket: BucketInfo {
                group_id,
                created_at: std::time::UNIX_EPOCH,
                created_by: user_id,
                cors_configuration: None,
                storage_routing: Vec::new(),
                placement_policies: Vec::new(),
                placement_policy_generation: 0,
                compression: aruna_core::structs::storage::format::Compression::Off,
                distribution: None,
            },
            restrictions: None,
            expected_checksums: Vec::new(),
        }
    }

    fn read_answer(input: &MaterializeSnapshotInput) -> IoResult {
        let read = ReadSourceResult {
            connector: SourceConnector {
                connector_id: input.connector_id,
                group_id: input.group_id,
                name: "source".to_string(),
                kind: aruna_core::structs::execution::source_connector::SourceConnectorKind::Http,
                public_config: Default::default(),
                created_at: std::time::UNIX_EPOCH,
                updated_at: std::time::UNIX_EPOCH,
                created_by: input.user_id,
                namespace: None,
            },
            metadata: SourceMetadata {
                content_length: 4,
                content_type: None,
                etag: None,
                last_modified: None,
                source_version: None,
                digests: Default::default(),
            },
            stream: aruna_core::stream::BackendStream(Box::pin(futures_util::stream::empty())),
        };
        child(Ok::<_, ReadSourceError>(read))
    }

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(aruna_core::calls::ChildOutcome::new(result))
    }

    fn staging(input: MaterializeSnapshotInput) -> StageSnapshotOperation {
        let bucket = BucketRules::Named(input.bucket.clone());
        let routing = aruna_core::structs::storage::routing::NodeRouting::default();
        StageSnapshotOperation {
            routing: Some(SnapshotOperation::new(&routing, input.group_id, bucket)),
            input,
            step: SnapshotStep::Read,
            read: None,
            source: None,
            version_source: None,
            snapshot: None,
        }
    }

    // Only a retry looks for the object an earlier attempt made.
    #[test]
    fn retry_checks_reuse() {
        let input = plain_input(Some("job-1"));
        let answer = read_answer(&input);
        let mut retried = staging(input);
        retried.first().unwrap();
        retried.next(answer).unwrap();
        assert_eq!(retried.step, SnapshotStep::Reuse);
        let capabilities = &retried
            .version_source
            .as_ref()
            .unwrap()
            .descriptor
            .capabilities;
        assert_eq!(capabilities, &["staging_retry:job-1".to_string()]);
        retried
            .next(child(Ok::<Reusable, SnapshotReuseError>(None)))
            .unwrap();
        assert_eq!(retried.step, SnapshotStep::Routing);

        let input = plain_input(None);
        let answer = read_answer(&input);
        let mut fresh = staging(input);
        fresh.first().unwrap();
        fresh.next(answer).unwrap();
        assert_eq!(fresh.step, SnapshotStep::Routing);
    }

    // A failed source read ends the run with that error.
    #[test]
    fn read_failure_ends() {
        let mut stage = staging(plain_input(None));
        stage.first().unwrap();
        let failed = ReadSourceError::UnexpectedEvent("gone");
        let answer = stage.next(child(Err::<ReadSourceResult, _>(failed)));
        assert!(matches!(answer, Err(MaterializeSnapshotError::Read(_))));
    }

    #[tokio::test]
    async fn snapshot_retry_scoped() {
        let test_context = setup_driver_context().await;
        let context = &test_context.driver_context;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let app = Router::new().route(
                "/folder/file.txt",
                get(|| async {
                    (
                        [("content-type", "text/plain"), ("etag", "\"snapshot-v1\"")],
                        "snapshot-data",
                    )
                }),
            );
            axum::serve(listener, app).await.unwrap();
        });
        let group_id = Ulid::generate();
        let realm_id = RealmId::from_bytes([6u8; 32]);
        let node_id = iroh::SecretKey::generate().public();
        let user_id = aruna_core::UserId::local(Ulid::generate(), realm_id);
        let expected_bucket = create_test_bucket(context, group_id, user_id, "bucket-a").await;
        let connector = create_http_connector(context, group_id, &endpoint).await;
        let input = MaterializeSnapshotInput {
            job_id: None,
            group_id,
            user_id,
            realm_id,
            node_id,
            connector_id: connector.connector_id,
            source_path: "folder/file.txt".to_string(),
            bucket: "bucket-a".to_string(),
            key: "object.txt".to_string(),
            quota_ceiling: None,
            retry_key: Some("job-1".to_string()),
            expected_bucket,
            restrictions: None,
            expected_checksums: Vec::new(),
        };

        let first = drive(stage_snapshot_blob(context, input.clone()), context)
            .await
            .unwrap();
        let second = drive(stage_snapshot_blob(context, input.clone()), context)
            .await
            .unwrap();
        let mut manual = input;
        manual.retry_key = None;
        let third = drive(stage_snapshot_blob(context, manual), context)
            .await
            .unwrap();
        server.abort();
        let _ = server.await;

        assert_eq!(second.version_id, first.version_id);
        assert_eq!(second.location, first.location);
        assert_ne!(third.version_id, first.version_id);
    }
}
