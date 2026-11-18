//! Materializes a staged source as a snapshot by reading it and putting the object.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::connectors::descriptor::build_source_binding;
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
use aruna_core::stream::BackendStream;
use aruna_core::structs::checksum::ExpectedChecksum;
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::source_connector::SourceConnector;
use aruna_core::structs::execution::staging::{StagingStrategy, VersionSourceBinding};
use aruna_core::structs::identity::auth::PathRestriction;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::origin::VersionOrigin;
use aruna_core::structs::storage::routing::{NodeRouting, RoutingSnapshot};
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
    #[error(transparent)]
    Io(#[from] IoError),
}

#[derive(Debug, PartialEq)]
enum SnapshotStep {
    Read,
    Reuse,
    Routing,
    Gate(Box<RoutingSnapshot>),
    Put,
}

/// Reads a source and keeps it as an object version; a retried read of the same job reuses the
/// version an earlier attempt wrote.
#[derive(Debug, PartialEq)]
pub struct MaterializeSnapshotOperation {
    input: MaterializeSnapshotInput,
    routing: NodeRouting,
    now_ms: u64,
    read: Option<ReadSourceResult>,
    binding: Option<VersionSourceBinding>,
    step: SnapshotStep,
}

impl MaterializeSnapshotOperation {
    pub fn new(input: MaterializeSnapshotInput, routing: NodeRouting) -> Self {
        Self {
            input,
            routing,
            now_ms: 0,
            read: None,
            binding: None,
            step: SnapshotStep::Read,
        }
    }

    fn routing_call(&mut self) -> Next<MaterializeSnapshotResult> {
        self.step = SnapshotStep::Routing;
        Next::Call(IoCall::child(SnapshotOperation::new(
            &self.routing,
            self.input.group_id,
            BucketRules::Named(self.input.bucket.clone()),
        )))
    }

    fn put_call(
        &mut self,
        routing: RoutingSnapshot,
        gate: Option<GateContext>,
    ) -> Result<Next<MaterializeSnapshotResult>, MaterializeSnapshotError> {
        let read = self.read.as_mut().ok_or_else(lost_read)?;
        let empty = BackendStream::new(futures_util::stream::empty::<
            Result<bytes::Bytes, aruna_core::stream::StreamError>,
        >());
        let body = std::mem::replace(&mut read.stream, empty);
        let input = &mut self.input;
        let mut operation = PutObjectOperation::new(PutObjectConfig {
            user_id: input.user_id,
            group_id: input.group_id,
            realm_id: input.realm_id,
            node_id: input.node_id,
            request: PutObjectInput {
                bucket: input.bucket.clone(),
                key: input.key.clone(),
                content_length: Some(read.metadata.content_length),
                body: Some(body),
            },
            expected_checksums: std::mem::take(&mut input.expected_checksums),
            checksum_type: None,
            exists: false,
            version_source: self.binding.clone(),
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
            operation = operation.with_gate(gate);
        }
        self.step = SnapshotStep::Put;
        Ok(Next::Call(IoCall::child(operation)))
    }

    fn finish(
        &mut self,
        location: BackendLocation,
        version_id: Ulid,
    ) -> Result<Next<MaterializeSnapshotResult>, MaterializeSnapshotError> {
        let read = self.read.take().ok_or_else(lost_read)?;
        let version_source = self.binding.take().ok_or_else(lost_read)?;
        Ok(Next::Done(MaterializeSnapshotResult {
            connector: read.connector,
            source_metadata: read.metadata,
            version_source,
            location,
            version_id,
        }))
    }
}

/// A step ran without the read its order guarantees.
fn lost_read() -> MaterializeSnapshotError {
    IoError::Unexpected {
        expected: "the read source",
        got: "no read",
    }
    .into()
}

impl Operation for MaterializeSnapshotOperation {
    type Output = MaterializeSnapshotResult;
    type Error = MaterializeSnapshotError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<Self::Output>, MaterializeSnapshotError> {
        Ok(Next::Call(IoCall::child(ReadSourceOperation::new(
            ReadSourceInput {
                group_id: self.input.group_id,
                connector_id: self.input.connector_id,
                source_path: self.input.source_path.clone(),
                range: None,
                expected: None,
            },
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, MaterializeSnapshotError> {
        match std::mem::replace(&mut self.step, SnapshotStep::Read) {
            SnapshotStep::Read => {
                let read = result.child::<ReadSourceResult, ReadSourceError>()??;
                let mut binding = build_source_binding(
                    StagingStrategy::Snapshot,
                    &read.connector,
                    &read.metadata,
                    self.input.source_path.clone(),
                    Some(self.input.node_id),
                    Some(read.connector.connector_id),
                );
                if let Some(retry_key) = self.input.retry_key.as_deref() {
                    let capability = format!("staging_retry:{retry_key}");
                    binding.descriptor.capabilities.push(capability);
                }
                let size = read.metadata.content_length;
                self.binding = Some(binding.clone());
                self.read = Some(read);
                if self.input.retry_key.is_none() {
                    return Ok(self.routing_call());
                }
                self.step = SnapshotStep::Reuse;
                Ok(Next::Call(IoCall::child(SnapshotReuseOperation::new(
                    self.input.bucket.clone(),
                    self.input.key.clone(),
                    binding,
                    size,
                ))))
            }
            SnapshotStep::Reuse => match result.child::<Reusable, SnapshotReuseError>()?? {
                Some((location, version_id)) => self.finish(location, version_id),
                None => Ok(self.routing_call()),
            },
            SnapshotStep::Routing => {
                let routing = result.child::<RoutingSnapshot, RoutingInputsError>()??;
                self.step = SnapshotStep::Gate(Box::new(routing));
                // The staging source is external, so it carries no refs of its own: only
                // the destination default governs the snapshot.
                let gate = GateOperation::new(self.input.realm_id, self.now_ms);
                Ok(Next::Call(IoCall::child(gate)))
            }
            SnapshotStep::Gate(routing) => {
                let gate = result.child::<Option<GateContext>, GateContextError>()??;
                self.put_call(*routing, gate)
            }
            SnapshotStep::Put => {
                let put = result.child::<PutObjectResult, PutObjectError>()??;
                self.finish(put.location, put.version_id)
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
    use crate::tests::staging::{create_http_connector, create_test_bucket, setup_driver_context};
    use axum::Router;
    use axum::routing::get;
    use tokio::net::TcpListener;

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

        let stage = |input| {
            let routing = crate::driver::node_routing(context);
            crate::driver::drive(MaterializeSnapshotOperation::new(input, routing), context)
        };
        let first = stage(input.clone()).await.unwrap();
        let second = stage(input.clone()).await.unwrap();
        let mut manual = input;
        manual.retry_key = None;
        let third = stage(manual).await.unwrap();
        server.abort();
        let _ = server.await;

        assert_eq!(second.version_id, first.version_id);
        assert_eq!(second.location, first.location);
        assert_ne!(third.version_id, first.version_id);
    }
}
