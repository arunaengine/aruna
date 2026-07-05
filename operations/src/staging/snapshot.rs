use crate::driver::{DriverContext, drive};
use crate::s3::put_object::{PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation};
use crate::staging::descriptor::build_version_source_binding;
use crate::staging::read_source::{
    ReadStagingSourceError, ReadStagingSourceInput, ReadStagingSourceOperation,
};
use aruna_core::structs::{
    RealmId, SourceConnector, SourceMetadata, StagingStrategy, VersionSourceBinding,
};
use aruna_core::types::{GroupId, NodeId, UserId};
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
}

#[derive(Debug, PartialEq)]
pub struct MaterializeSnapshotResult {
    pub connector: SourceConnector,
    pub source_metadata: SourceMetadata,
    pub version_source: VersionSourceBinding,
    pub location: BackendLocation,
    pub version_id: Ulid,
}

use aruna_core::structs::BackendLocation;

#[derive(Debug, Error, PartialEq)]
pub enum MaterializeSnapshotError {
    #[error(transparent)]
    Read(#[from] ReadStagingSourceError),
    #[error(transparent)]
    Write(#[from] PutObjectError),
}

pub async fn materialize_snapshot(
    context: &DriverContext,
    input: MaterializeSnapshotInput,
) -> Result<MaterializeSnapshotResult, MaterializeSnapshotError> {
    let read_result = drive(
        ReadStagingSourceOperation::new(ReadStagingSourceInput {
            group_id: input.group_id,
            connector_id: input.connector_id,
            source_path: input.source_path.clone(),
            range: None,
        }),
        context,
    )
    .await?;

    let version_source = build_version_source_binding(
        StagingStrategy::Snapshot,
        &read_result.connector,
        &read_result.metadata,
        input.source_path,
        Some(input.node_id),
        Some(read_result.connector.connector_id),
    );
    let put_result = drive(
        PutObjectOperation::new(PutObjectConfig {
            user_id: input.user_id,
            group_id: input.group_id,
            realm_id: input.realm_id,
            node_id: input.node_id,
            request: PutObjectInput {
                bucket: input.bucket,
                key: input.key,
                content_length: Some(read_result.metadata.content_length),
                body: Some(read_result.stream),
            },
            expected_checksums: Vec::new(),
            checksum_type: None,
            exists: false,
            version_source: Some(version_source.clone()),
            quota_ceiling: input.quota_ceiling,
        }),
        context,
    )
    .await
    .and_then(|result| result.transpose())?;
    let put_result = put_result.ok_or(PutObjectError::PutObjectFailed)?;

    Ok(MaterializeSnapshotResult {
        connector: read_result.connector,
        source_metadata: read_result.metadata,
        version_source,
        location: put_result.location,
        version_id: put_result.version_id,
    })
}

pub async fn stage_snapshot_blob(
    context: &DriverContext,
    input: MaterializeSnapshotInput,
) -> Result<MaterializeSnapshotResult, MaterializeSnapshotError> {
    materialize_snapshot(context, input).await
}
