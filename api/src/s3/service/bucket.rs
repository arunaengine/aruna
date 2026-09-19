//! Holds the bucket helpers of the S3 adapter: authorization, quota and replication setup.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::ArunaS3Service;
use super::object::restrictions_reach;
use crate::s3::auth::map_authorize_error;
use crate::s3::error::IntoS3Error;
use crate::s3::scope::resolve_scope;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::{BucketInfo, UserAccess, bucket_permission_path};
use aruna_core::structs::storage::replication::ArunaArn;
use aruna_core::structs::{SyncMode, SyncRelationship};
use aruna_operations::auth::request_authorization::{AuthorizeError, authorize};
use aruna_operations::auth::request_policy::PolicyRequestExtras;
use aruna_operations::driver::drive;
use aruna_operations::metadata::AuthToken;
use aruna_operations::realm::get_config::GetConfigOperation;
use aruna_operations::replication::queue::{LiveVersionInput, LiveVersionOperation};
use aruna_operations::s3::bucket::get::GetBucketOperation;
use aruna_operations::sync::mirror_repair::{
    SyncMirrorIntent, clear_mirror_repair, delete_sync_mirror, kick_mirror_repair,
    request_mirror_create,
};
use aruna_operations::sync::sync_relationship::{
    DeleteRelationshipOperation, ListRelationshipsOperation, StoreRelationshipOperation,
    SyncRelationshipDirection,
};
use s3s::dto::{
    DeleteMarkerReplication, DeleteMarkerReplicationStatus, Destination, ReplicationConfiguration,
    ReplicationRule, ReplicationRuleStatus,
};
use s3s::{S3Result, s3_error};
use std::collections::BTreeSet;
use tracing::{error, warn};

pub(super) const MAX_REPLICATION_TARGETS: usize = 64;

impl ArunaS3Service {
    pub(super) async fn can_access_bucket(
        &self,
        user_access: &UserAccess,
        bucket: &str,
        bucket_info: &BucketInfo,
        extras: &PolicyRequestExtras,
    ) -> S3Result<bool> {
        let bucket_path =
            bucket_permission_path(self.realm_id, bucket_info.group_id, self.node_id, bucket);
        // A prefix-scoped credential still owns the bucket holding its scope,
        // so visibility asks whether its scope lies inside this bucket.
        if !restrictions_reach(user_access.path_restrictions.as_deref(), &bucket_path) {
            return Ok(false);
        }
        match authorize(
            &self.state,
            self.realm_id,
            &AuthContext {
                user_id: user_access.user_identity,
                realm_id: user_access.user_identity.realm_id,
                path_restrictions: None,
                session: None,
            },
            &bucket_path,
            &Permission::READ,
            extras.clone(),
        )
        .await
        {
            Ok(()) => Ok(true),
            Err(AuthorizeError::CheckFailed(message)) => {
                Err(s3_error!(InternalError, "{}", message))
            }
            Err(error @ AuthorizeError::Storage(_)) => Err(map_authorize_error(error)),
            // A member whose roles reach only a folder inside this bucket still
            // owns the bucket holding it, so it stays visible.
            Err(AuthorizeError::PermissionDenied) => {
                Ok(!resolve_scope(&self.state, user_access, &bucket_path)
                    .await?
                    .is_empty())
            }
            Err(_) => Ok(false),
        }
    }

    /// Resolves the hard byte ceiling for a group's realm-wide `logical_bytes`
    /// from the realm quota config. `None` means the group is unlimited.
    pub(super) async fn resolve_quota_ceiling(
        &self,
        group_id: aruna_core::types::GroupId,
    ) -> S3Result<Option<u64>> {
        let realm_config = drive(GetConfigOperation::new(self.realm_id), &self.state)
            .await
            .map_err(|err| {
                error!(error = %err, "Failed to load realm config for quota enforcement");
                s3_error!(InternalError, "Failed to load realm quota configuration")
            })?;
        Ok(realm_config.quota.effective_group_ceiling(&group_id))
    }

    pub(super) fn parse_replication_targets(
        &self,
        bucket: &str,
        configuration: &ReplicationConfiguration,
    ) -> S3Result<Vec<(ArunaArn, bool)>> {
        if bucket.starts_with("ws-") {
            return Err(s3_error!(
                InvalidArgument,
                "Workspace buckets cannot be replication sources"
            ));
        }

        let enabled_targets = configuration
            .rules
            .iter()
            .filter(|rule| rule.status.as_str() == ReplicationRuleStatus::ENABLED)
            .count();
        if enabled_targets > MAX_REPLICATION_TARGETS {
            return Err(s3_error!(
                InvalidArgument,
                "Replication supports at most {MAX_REPLICATION_TARGETS} enabled targets"
            ));
        }

        let mut targets = Vec::new();
        let mut seen = BTreeSet::new();

        for rule in &configuration.rules {
            if rule.status.as_str() != ReplicationRuleStatus::ENABLED {
                continue;
            }
            let arn = ArunaArn::parse(&rule.destination.bucket)
                .map_err(|err| s3_error!(InvalidArgument, "{}", err.to_string()))?;
            if arn.resource_type != aruna_core::structs::storage::replication::ArunaArnType::S3 {
                return Err(s3_error!(
                    InvalidArgument,
                    "Replication target ARN must use s3 type"
                ));
            }
            if arn.realm_id != self.realm_id {
                return Err(s3_error!(
                    InvalidArgument,
                    "Replication target must be in same realm"
                ));
            }
            let target_bucket = arn.bucket().ok_or_else(|| {
                s3_error!(InvalidArgument, "Replication target ARN must use s3 type")
            })?;
            if arn.key_prefix().is_some() {
                return Err(s3_error!(
                    InvalidArgument,
                    "Replication target ARN must name a bucket, not a prefix"
                ));
            }
            if target_bucket.starts_with("ws-") {
                return Err(s3_error!(
                    InvalidArgument,
                    "Workspace buckets cannot be replication targets"
                ));
            }
            if arn.node_id == self.node_id && target_bucket == bucket {
                return Err(s3_error!(
                    InvalidArgument,
                    "Replication source and target must differ"
                ));
            }
            let replicate_delete_markers = rule
                .delete_marker_replication
                .as_ref()
                .and_then(|replication| replication.status.as_ref())
                .is_some_and(|status| status.as_str() == DeleteMarkerReplicationStatus::ENABLED);
            let target_bucket = target_bucket.to_string();
            if !seen.insert((
                arn.node_id,
                arn.realm_id,
                target_bucket.clone(),
                replicate_delete_markers,
            )) {
                continue;
            }
            targets.push((arn, replicate_delete_markers));
        }

        if targets.is_empty() {
            return Err(s3_error!(
                InvalidArgument,
                "Replication requires at least one enabled target"
            ));
        }

        Ok(targets)
    }

    /// Renders the outgoing sync relationships of one bucket as the S3
    /// replication document. Relationship target ARNs are the only source.
    pub(super) fn build_replication_configuration(
        &self,
        relationships: &[SyncRelationship],
    ) -> ReplicationConfiguration {
        let rules = relationships
            .iter()
            .enumerate()
            .map(|(index, relationship)| ReplicationRule {
                delete_marker_replication: Some(DeleteMarkerReplication {
                    status: Some(DeleteMarkerReplicationStatus::from_static(
                        if relationship.replicate_deletes {
                            DeleteMarkerReplicationStatus::ENABLED
                        } else {
                            DeleteMarkerReplicationStatus::DISABLED
                        },
                    )),
                }),
                destination: Destination {
                    access_control_translation: None,
                    account: None,
                    bucket: relationship.target.to_string(),
                    encryption_configuration: None,
                    metrics: None,
                    replication_time: None,
                    storage_class: None,
                },
                existing_object_replication: None,
                filter: None,
                id: Some(format!("aruna-target-{}", index + 1)),
                prefix: None,
                priority: Some((index + 1) as i32),
                source_selection_criteria: None,
                status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
            })
            .collect();

        ReplicationConfiguration {
            role: "arn:aruna:replication-role".to_string(),
            rules,
        }
    }

    pub(super) async fn list_xml_relationships(
        &self,
        bucket: &str,
    ) -> S3Result<Vec<SyncRelationship>> {
        let relationships = drive(
            ListRelationshipsOperation::new(
                SyncRelationshipDirection::Outgoing,
                Some(bucket.to_string()),
            ),
            &self.state,
        )
        .await
        .map_err(|error| s3_error!(InternalError, "{}", error.to_string()))?;

        Ok(relationships
            .into_iter()
            .filter(|relationship| {
                relationship.mode == SyncMode::Continuous
                    && relationship.source.realm_id == self.realm_id
                    && relationship.source.node_id == self.node_id
                    && relationship.source.bucket() == Some(bucket)
                    && relationship.source.key_prefix().is_none()
                    && relationship.target.realm_id == self.realm_id
                    && relationship.target.key_prefix().is_none()
            })
            .collect())
    }

    pub(super) async fn store_sync_relationship(
        &self,
        relationship: SyncRelationship,
        direction: SyncRelationshipDirection,
    ) -> S3Result<()> {
        drive(
            StoreRelationshipOperation::new(relationship, direction),
            &self.state,
        )
        .await
        .map(|_| ())
        .map_err(|error| s3_error!(InternalError, "{}", error.to_string()))
    }

    pub(super) async fn delete_sync_relationship(
        &self,
        relationship: SyncRelationship,
        direction: SyncRelationshipDirection,
    ) -> S3Result<()> {
        drive(
            DeleteRelationshipOperation::new(relationship, direction),
            &self.state,
        )
        .await
        .map_err(|error| s3_error!(InternalError, "{}", error.to_string()))
    }

    pub(super) async fn create_sync_mirror(
        &self,
        user_access: &UserAccess,
        relationship: &SyncRelationship,
        extras: &PolicyRequestExtras,
    ) -> S3Result<()> {
        if relationship.target.node_id == self.node_id {
            let target_bucket = relationship
                .target
                .bucket()
                .ok_or_else(|| s3_error!(InvalidArgument, "Invalid replication target ARN"))?;
            let bucket_info = drive(
                GetBucketOperation::new(target_bucket.to_string()),
                &self.state,
            )
            .await
            .map_err(IntoS3Error::into_s3_error)?;
            authorize(
                &self.state,
                self.realm_id,
                &AuthContext {
                    user_id: user_access.user_identity,
                    realm_id: user_access.user_identity.realm_id,
                    path_restrictions: user_access.path_restrictions.clone(),
                    session: None,
                },
                &bucket_permission_path(
                    self.realm_id,
                    bucket_info.group_id,
                    self.node_id,
                    target_bucket,
                ),
                &Permission::WRITE,
                extras.clone(),
            )
            .await
            .map_err(map_authorize_error)?;
            return self
                .store_sync_relationship(relationship.clone(), SyncRelationshipDirection::Incoming)
                .await;
        }

        let auth_token = AuthToken::internal(AuthContext {
            user_id: user_access.user_identity,
            realm_id: self.realm_id,
            path_restrictions: user_access.path_restrictions.clone(),
            session: None,
        });
        request_mirror_create(
            &self.state,
            relationship.target.node_id,
            auth_token,
            user_access.group_id,
            relationship.clone(),
            extras.clone(),
        )
        .await
        .map_err(|error| match error {
            aruna_core::metadata::MetadataError::HandleMissing => {
                s3_error!(InternalError, "Replication target is unreachable")
            }
            aruna_core::metadata::MetadataError::Backend(message) if message == "access_denied" => {
                s3_error!(AccessDenied, "Permission denied")
            }
            aruna_core::metadata::MetadataError::Backend(message) if message == "not_found" => {
                s3_error!(NoSuchBucket, "Replication target bucket does not exist")
            }
            error => s3_error!(InternalError, "Replication target error: {}", error),
        })
    }

    pub(super) async fn remove_sync_mirror(&self, relationship: &SyncRelationship) -> bool {
        if let Err(error) = delete_sync_mirror(&self.state, self.node_id, relationship).await {
            warn!(relationship_id = %relationship.id, %error, "Failed to remove remote sync mirror");
            return false;
        }
        true
    }

    pub(super) async fn clear_mirror_repair(
        &self,
        relationship: &SyncRelationship,
        expected: SyncMirrorIntent,
    ) {
        if let Err(error) = clear_mirror_repair(&self.state, relationship, expected).await {
            warn!(%error, relationship_id = %relationship.id, "Failed to clear sync mirror repair");
            kick_mirror_repair(&self.state).await;
        }
    }

    pub(super) async fn queue_live_replication(
        &self,
        auth_context: AuthContext,
        bucket: String,
        key: String,
        version_id: ulid::Ulid,
        delete_marker: bool,
    ) {
        let result = match drive(
            LiveVersionOperation::new(LiveVersionInput {
                local_node_id: self.node_id,
                auth_context,
                bucket: bucket.clone(),
                key: key.clone(),
                version_id,
                delete_marker,
            }),
            &self.state,
        )
        .await
        {
            Ok(result) => result,
            Err(error) => {
                warn!(
                    error = %error,
                    bucket,
                    key,
                    version_id = %version_id,
                    delete_marker,
                    "Failed to queue live replication after committed write; durable obligation remains for repair"
                );
                return;
            }
        };

        if result.queued > 0 && !result.scheduled {
            warn!(bucket, key, version_id = %version_id, queued = result.queued, "Live replication jobs persisted but drain scheduling was not acknowledged");
        }
    }
}
