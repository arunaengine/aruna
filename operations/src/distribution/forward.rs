//! Carries distributed bucket configuration between nodes: a member creates and admits its
//! bucket for a forwarded bearer token (D12); admin requests and statements reach the publisher.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Only the publisher accepts them (D33). Nothing is queued.

use std::sync::Arc;
use std::time::SystemTime;

use aruna_core::NodeId;
use aruna_core::errors::StorageError;
use aruna_core::structs::SyncRefusal;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::{BucketInfo, bucket_permission_path};
use aruna_core::structs::storage::distribution::capacity::RangeSize;
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionMember, DistributionRequest, DistributionRevision,
    DistributionSnapshot, SignedSnapshot, SignedStatement,
};
use aruna_core::structs::storage::format::Compression;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use super::admit::{AdmitConfig, AdmitError, AdmitOperation};
use super::publish::{PublishConfig, PublishError, PublishOperation};
use crate::driver::{DriverContext, drive};
use crate::effect_adapters::routing::node_routing;
use crate::forward::authorize::is_sync_eligible;
use crate::forward::transport::reject;
use crate::metadata::device_pull::{authorize_peer, authorize_pull};
use crate::metadata::protocol::{AuthToken, MetadataTransportMessage};
use crate::placement::process_placements::load_realm_config;
use crate::replication::bootstrap::wake_bootstrap;
use crate::s3::bucket::create::{CreateBucketError, CreateBucketOperation};
use crate::s3::bucket::delete::DeleteBucketOperation;
use crate::s3::bucket::get::GetBucketOperation;
use crate::s3::bucket::metadata::{DescriptionError, dataset_description};

/// A publish attempt retries a storage conflict this often before it gives up.
const PUBLISH_ATTEMPTS: usize = 3;

/// Why the publisher refused or could not take a request.
#[derive(Clone, Debug, Error, Eq, PartialEq, Serialize, Deserialize)]
pub enum PublishRefusal {
    #[error("the bearer token is missing or invalid")]
    Unauthorized,
    #[error("caller may not administer the distributed bucket's owner group")]
    Forbidden,
    #[error("{0}")]
    Rejected(String),
    #[error("the statement is already part of the record")]
    Replayed,
    #[error("configuration changes unavailable: {0}")]
    Unavailable(String),
}

impl From<PublishError> for PublishRefusal {
    fn from(error: PublishError) -> Self {
        use aruna_core::structs::storage::distribution::DistributionError;
        match error {
            PublishError::Unauthorized => Self::Forbidden,
            PublishError::Distribution(DistributionError::Replayed(_)) => Self::Replayed,
            PublishError::Storage(_) | PublishError::SignUnavailable(_) => {
                Self::Unavailable("the publisher could not commit the change".to_string())
            }
            other => Self::Rejected(other.to_string()),
        }
    }
}

/// Accepts one request on this node, the set's publisher, retrying storage conflicts.
pub(crate) async fn publish_local(
    context: &Arc<DriverContext>,
    set_id: Ulid,
    request_id: Ulid,
    request: DistributionRequest,
    auth_context: Option<AuthContext>,
) -> Result<DistributionRevision, PublishRefusal> {
    let net = context
        .net_handle
        .as_ref()
        .ok_or_else(|| PublishRefusal::Unavailable("net transport unavailable".to_string()))?;
    let config = PublishConfig {
        realm_id: *net.realm_id(),
        node_id: net.node_id(),
        set_id,
        request_id,
        request,
        auth_context,
    };
    let mut last = PublishRefusal::Unavailable("no attempt was made".to_string());
    for _ in 0..PUBLISH_ATTEMPTS {
        match drive(PublishOperation::new(config.clone()), context.as_ref()).await {
            Ok(revision) => {
                wake_bootstrap(context).await;
                return Ok(revision);
            }
            Err(PublishError::Storage(StorageError::TransactionConflict)) => {
                last = PublishRefusal::Unavailable("the record changed concurrently".to_string());
            }
            Err(error) => return Err(error.into()),
        }
    }
    Err(last)
}

/// The caller of a forwarded configuration request. Only the caller's own bearer token
/// counts; a peer-asserted identity never acts as a group admin (D12, K1).
async fn caller(
    context: &Arc<DriverContext>,
    peer: NodeId,
    auth_token: AuthToken,
) -> Result<AuthContext, SyncRefusal> {
    match bearer_only(&auth_token) {
        true => authorize_peer(context, peer, auth_token).await,
        false => Err(SyncRefusal::Unauthorized),
    }
}

fn bearer_only(auth_token: &AuthToken) -> bool {
    matches!(auth_token, AuthToken::Bearer(_))
}

/// The publisher's side of a forwarded admin request or member statement.
pub(crate) async fn serve_publish(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> MetadataTransportMessage {
    let result = match message {
        MetadataTransportMessage::ForwardDistributionRequest {
            auth_token,
            set_id,
            request_id,
            request,
        } => match caller(context, peer, auth_token.clone()).await {
            // Statements carry their signer's authority, never a forwarded caller's.
            Ok(_) if !request.admin() => Err(PublishRefusal::Rejected(
                "statements are submitted without a caller".to_string(),
            )),
            Ok(auth) => match readable_link(context, (&auth, auth_token), &request).await {
                Ok(()) => publish_local(context, set_id, request_id, *request, Some(auth)).await,
                Err(refusal) => Err(refusal),
            },
            Err(SyncRefusal::Unavailable) => Err(PublishRefusal::Unavailable(
                "the caller could not be checked".to_string(),
            )),
            Err(_) => Err(PublishRefusal::Unauthorized),
        },
        MetadataTransportMessage::SubmitDistributionStatement { statement } => {
            match infrastructure_peer(context, peer).await {
                Ok(()) => {
                    let (set_id, id) =
                        (statement.statement.set_id, statement.statement.statement_id);
                    let request = DistributionRequest::Statement(statement);
                    publish_local(context, set_id, id, request, None).await
                }
                Err(refusal) => Err(refusal),
            }
        }
        _ => return reject("unexpected distribution message"),
    };
    MetadataTransportMessage::DistributionAccepted { result }
}

/// Refuses a metadata change that links a dataset the caller cannot read or that is no Dataset,
/// as the metadata route does.
async fn readable_link(
    context: &DriverContext,
    (auth, token): (&AuthContext, AuthToken),
    request: &DistributionRequest,
) -> Result<(), PublishRefusal> {
    let DistributionRequest::ChangeMetadata {
        metadata: Some(metadata),
        ..
    } = request
    else {
        return Ok(());
    };
    let Some(dataset) = metadata.dataset else {
        return Ok(());
    };
    let caller = (Some(auth), Some(token));
    match dataset_description(context, auth.realm_id, dataset, caller).await {
        Ok(_) => Ok(()),
        Err(DescriptionError::Unreadable) => Err(PublishRefusal::Rejected(format!(
            "dataset {dataset} does not exist or you may not read it"
        ))),
        Err(DescriptionError::Invalid(reason)) => Err(PublishRefusal::Rejected(reason)),
        Err(DescriptionError::Unavailable) => Err(PublishRefusal::Unavailable(format!(
            "dataset {dataset} could not be read now"
        ))),
    }
}

/// Only configured realm infrastructure nodes relay statements; devices are refused.
async fn infrastructure_peer(
    context: &Arc<DriverContext>,
    peer: NodeId,
) -> Result<(), PublishRefusal> {
    let net = context
        .net_handle
        .as_ref()
        .ok_or_else(|| PublishRefusal::Unavailable("net transport unavailable".to_string()))?;
    let config = load_realm_config(context, *net.realm_id())
        .await
        .ok_or_else(|| PublishRefusal::Unavailable("realm config missing".to_string()))?;
    match is_sync_eligible(&config, peer) {
        true => Ok(()),
        false => Err(PublishRefusal::Unauthorized),
    }
}

/// The member's side of a forwarded admission: the bearer token is checked again here.
pub(crate) async fn serve_admission(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> MetadataTransportMessage {
    let MetadataTransportMessage::ForwardAdmitMember {
        auth_token,
        snapshot,
        member_id,
        sizes,
    } = message
    else {
        return reject("unexpected admission message");
    };
    let result = match caller(context, peer, auth_token).await {
        Ok(auth) => admit_member(context, &auth, &snapshot, (member_id, &sizes)).await,
        Err(refusal) => Err(refusal),
    };
    if let Err(refusal) = &result {
        warn!(%peer, %member_id, ?refusal, "Refused a distributed bucket admission");
    }
    MetadataTransportMessage::AdmittedMember {
        result: result.map(Box::new),
    }
}

/// Creates this node's member bucket for the caller, or finds it again on a retry, then
/// admits it against this node's capacity with the ranges' expected `sizes`. Any other bucket
/// of the name fails.
pub(crate) async fn admit_member(
    context: &Arc<DriverContext>,
    auth: &AuthContext,
    signed: &SignedSnapshot,
    (member_id, sizes): (Ulid, &[RangeSize]),
) -> Result<SignedStatement, SyncRefusal> {
    let net = context
        .net_handle
        .as_ref()
        .ok_or(SyncRefusal::Unavailable)?;
    let (realm_id, node_id) = (*net.realm_id(), net.node_id());
    if auth.realm_id != realm_id {
        return Err(SyncRefusal::Unauthorized);
    }
    signed
        .verify(realm_id)
        .map_err(|error| SyncRefusal::Invalid(error.to_string()))?;
    let snapshot = &signed.snapshot;
    let config = load_realm_config(context, realm_id)
        .await
        .ok_or(SyncRefusal::Unavailable)?;
    if !is_sync_eligible(&config, snapshot.root.publisher) || !is_sync_eligible(&config, node_id) {
        return Err(SyncRefusal::Invalid(
            "publisher and members must be realm infrastructure nodes".to_string(),
        ));
    }
    let member = snapshot.member(member_id).ok_or(SyncRefusal::NotFound)?;
    if member.node_id != node_id {
        return Err(SyncRefusal::Invalid(
            "the member belongs to another node".to_string(),
        ));
    }
    let bucket = snapshot.config.bucket.clone();
    let group_id = snapshot.config.owner_group;
    let path = bucket_permission_path(realm_id, group_id, node_id, &bucket);
    authorize_pull(context, auth, path, Permission::WRITE, "s3.CreateBucket").await?;
    create_member(context, auth, snapshot, member).await?;
    let admit = AdmitConfig {
        realm_id,
        node_id,
        bucket: bucket.clone(),
        set_id: snapshot.set_id(),
        member_id,
        intent: member.intent,
        ranges: snapshot
            .held_ranges(member_id)
            .into_iter()
            .cloned()
            .collect(),
        strategy: member.strategy,
        coverage: member.coverage,
        sizes: sizes.to_vec(),
        capacity: node_routing(context).catalog.capacity(),
    };
    let admitted = drive(AdmitOperation::new(admit), context.as_ref()).await;
    if let Err(
        AdmitError::CapacityUnknown
        | AdmitError::InsufficientCapacity { .. }
        | AdmitError::SizeUnknown(_),
    ) = &admitted
        && member.incarnation.is_none()
    {
        // A member never admitted holds nothing, so its bucket goes and a new create may use the name.
        remove_member(context, &bucket, snapshot.set_id(), member_id).await;
    }
    admitted.map_err(|error| match error {
        AdmitError::BindingChanged(_)
        | AdmitError::CapacityUnknown
        | AdmitError::InsufficientCapacity { .. }
        | AdmitError::SizeUnknown(_) => SyncRefusal::Invalid(error.to_string()),
        _ => SyncRefusal::Unavailable,
    })
}

/// Deletes the bucket of the name while it still carries this member's binding.
async fn remove_member(context: &DriverContext, bucket: &str, set_id: Ulid, member_id: Ulid) {
    let Ok(info) = drive(GetBucketOperation::new(bucket.to_string()), context).await else {
        return;
    };
    let Some(binding) = info
        .distribution
        .filter(|binding| (binding.set_id, binding.member_id) == (set_id, member_id))
    else {
        return;
    };
    let delete = DeleteBucketOperation::new(bucket.to_string()).bound_to(binding);
    if let Err(error) = drive(delete, context).await {
        warn!(%error, bucket, "The bucket of a refused member stayed");
    }
}

async fn create_member(
    context: &Arc<DriverContext>,
    auth: &AuthContext,
    snapshot: &DistributionSnapshot,
    member: &DistributionMember,
) -> Result<(), SyncRefusal> {
    let bucket = snapshot.config.bucket.clone();
    let info = BucketInfo {
        group_id: snapshot.config.owner_group,
        created_at: SystemTime::now(),
        created_by: auth.user_id,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: Some(DistributionBinding {
            set_id: snapshot.set_id(),
            member_id: member.member_id,
            // The creation fills in the bucket's own incarnation.
            incarnation: 0,
            revision: snapshot.revision(),
        }),
    };
    let create = CreateBucketOperation::new(bucket.clone(), info).mapped();
    match drive(create, context.as_ref()).await {
        Ok(_) => Ok(()),
        Err(error @ CreateBucketError::ProviderBucket(_)) => {
            Err(SyncRefusal::Invalid(error.to_string()))
        }
        Err(CreateBucketError::BucketAlreadyExists) => {
            match drive(GetBucketOperation::new(bucket.clone()), context.as_ref()).await {
                Ok(info) => same_member(&info, snapshot, member, &bucket),
                Err(_) => Err(SyncRefusal::Unavailable),
            }
        }
        Err(_) => Err(SyncRefusal::Unavailable),
    }
}

/// A retry succeeds only on the bucket this member created: same set, member and group, and
/// the incarnation the record admitted, if any.
fn same_member(
    info: &BucketInfo,
    snapshot: &DistributionSnapshot,
    member: &DistributionMember,
    bucket: &str,
) -> Result<(), SyncRefusal> {
    let same = info.group_id == snapshot.config.owner_group
        && info.distribution.as_ref().is_some_and(|binding| {
            binding.set_id == snapshot.set_id()
                && binding.member_id == member.member_id
                && member
                    .incarnation
                    .is_none_or(|admitted| admitted == binding.incarnation)
        });
    match same {
        true => Ok(()),
        false => Err(SyncRefusal::Invalid(format!(
            "bucket \"{bucket}\" already exists on this node and is not this member"
        ))),
    }
}

#[cfg(test)]
#[path = "forward_tests.rs"]
mod tests;
