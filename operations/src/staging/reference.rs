//! Materializes a staged source as a reference version that points at the source bytes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::SystemTime;

use crate::blob::records::{HeadAliasContext, build_transition_effects, write_version_effect};
use crate::connectors::repository::{
    StorageReadError, connector_reference_write, parse_secret_read, read_secret_effect,
    source_connector_key,
};
use crate::connectors::resolver::secret_fingerprint;
use crate::distribution::guard::{GuardError, authority_read, record_epoch};
use crate::distribution::inventory::{
    AddedVersion, entry_of, stamp_head, stamp_version, version_written,
};
use crate::node::usage_stats::{
    RowChange, UsageCounterUpdate, UsageUpdateError, schedule_snapshot_publish,
};
use crate::plugins::journal::journaled;
use crate::replication::queue::build_live_obligation;
use crate::s3::purge_fence::{PurgeFenceError, check_write_fence, write_fence_read};
use crate::staging::descriptor::{build_source_binding, build_source_descriptor};
use crate::staging::head_source::{
    HeadSourceError, HeadSourceInput, HeadSourceOperation, HeadSourceResult,
};
use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect, abort_effects};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, S3_BUCKET_KEYSPACE, SOURCE_INDEX_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::plugin_runs::SourceEvent;
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::source_connector::SourceConnector;
use aruna_core::structs::execution::staging::{
    SourceMirror, StagingStrategy, VersionSourceBinding,
};
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::policy::{PlacementPolicyError, PlacementPolicyRef};
use aruna_core::structs::storage::blob::{
    BlobHeadKey, BlobVersion, BlobVersionState, BucketInfo, CurrentVersionPointer, VersionKey,
};
use aruna_core::structs::storage::distribution::hint::{SetHint, with_hint};
use aruna_core::structs::storage::distribution::source_root::RootEntry;
use aruna_core::structs::storage::distribution::{DistributionBinding, OriginalEpoch};
use aruna_core::structs::storage::origin::VersionOrigin;
use aruna_core::structs::storage::usage::UsageDelta;
use aruna_core::types::{Effects, GroupId, Key, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializeReferenceInput {
    pub group_id: GroupId,
    pub user_id: UserId,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub connector_id: Ulid,
    pub source_path: String,
    pub bucket: String,
    pub key: String,
    pub expected_bucket: BucketInfo,
    /// Refs the reference carries over from an object it derives from; they
    /// are unioned with the bucket default and never dropped.
    pub inherited_policies: Vec<PlacementPolicyRef>,
    /// The writer's path restrictions, which its replication obligation keeps.
    pub restrictions: Option<Vec<PathRestriction>>,
    /// Further sources of the same file: each connector of the group with the file's path there.
    pub mirrors: Vec<(SourceConnector, String)>,
    /// The import job that registers the file, recorded in its origin.
    pub job_id: Option<Ulid>,
}

#[derive(Debug, PartialEq)]
pub struct MaterializeReferenceResult {
    pub connector: SourceConnector,
    pub source_metadata: SourceMetadata,
    pub version_source: VersionSourceBinding,
    pub version_id: Ulid,
}

#[derive(Debug, Error, PartialEq)]
pub enum MaterializeReferenceError {
    #[error(transparent)]
    Head(#[from] HeadSourceError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Usage(#[from] UsageUpdateError),
    #[error(transparent)]
    Policy(#[from] PlacementPolicyError),
    #[error(transparent)]
    Purge(#[from] PurgeFenceError),
    #[error(transparent)]
    Distribution(#[from] GuardError),
}

pub async fn stage_reference_blob(
    context: &DriverContext,
    input: MaterializeReferenceInput,
) -> Result<MaterializeReferenceResult, MaterializeReferenceError> {
    let head_result = drive(
        HeadSourceOperation::new(HeadSourceInput {
            group_id: input.group_id,
            connector_id: input.connector_id,
            source_path: input.source_path.clone(),
        }),
        context,
    )
    .await?;

    let mut version_source = build_source_binding(
        StagingStrategy::Reference,
        &head_result.connector,
        &head_result.metadata,
        input.source_path,
        Some(input.node_id),
        Some(head_result.connector.connector_id),
    );
    for (connector, path) in &input.mirrors {
        let metadata = &head_result.metadata;
        let mut descriptor =
            build_source_descriptor(connector, metadata, path.clone(), Some(input.node_id));
        // A version id names one server's copy, never another's.
        descriptor.version_selector = None;
        version_source.mirrors.push(SourceMirror {
            descriptor,
            connector_id: Some(connector.connector_id),
        });
    }
    let mirror_guards = (input.mirrors.into_iter()).map(|(connector, _)| connector);
    let (version_id, _changed) = write_reference_version(
        context,
        ReferenceWrite {
            preassigned_version_id: None,
            group_id: input.group_id,
            user_id: input.user_id,
            realm_id: input.realm_id,
            node_id: input.node_id,
            bucket: input.bucket,
            key: input.key,
            expected_bucket: Some(input.expected_bucket),
            version_source: version_source.clone(),
            metadata: head_result.metadata.clone(),
            inherited_policies: input.inherited_policies,
            restrictions: input.restrictions,
            connector_guard: Some((
                head_result.connector.clone(),
                head_result.secret_fingerprint,
            )),
            mirror_guards: mirror_guards.collect(),
            job_id: input.job_id,
        },
    )
    .await?;

    Ok(MaterializeReferenceResult {
        connector: head_result.connector,
        source_metadata: head_result.metadata,
        version_source,
        version_id,
    })
}

/// One reference version to record: freshly resolved through a connector, or
/// cloned from a version that already carries the binding.
#[derive(Clone, Debug, PartialEq)]
pub struct ReferenceWrite {
    pub preassigned_version_id: Option<Ulid>,
    pub group_id: GroupId,
    pub user_id: UserId,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub bucket: String,
    pub key: String,
    /// The bucket as the caller saw it; `None` only checks the group.
    pub expected_bucket: Option<BucketInfo>,
    pub version_source: VersionSourceBinding,
    pub metadata: SourceMetadata,
    /// Refs unioned with the bucket default and never dropped.
    pub inherited_policies: Vec<PlacementPolicyRef>,
    /// The connector the binding was resolved through, re-checked at commit.
    pub connector_guard: Option<(SourceConnector, Option<[u8; 16]>)>,
    /// The connectors of further sources, re-checked at commit as well.
    pub mirror_guards: Vec<SourceConnector>,
    /// The writer's path restrictions, which its replication obligation keeps.
    pub restrictions: Option<Vec<PathRestriction>>,
    /// The system job that writes the reference, recorded in its origin.
    pub job_id: Option<Ulid>,
}

/// Writes the reference version in one transaction and says whether anything
/// changed: the same binding with the same observation at the head is kept.
pub async fn write_reference_version(
    context: &DriverContext,
    write: ReferenceWrite,
) -> Result<(Ulid, bool), MaterializeReferenceError> {
    let now = SystemTime::now();

    let txn_id = match context
        .storage_handle
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        _ => return Err(StorageError::WriteError("unexpected event".to_string()).into()),
    };

    let result: Result<(Ulid, bool), MaterializeReferenceError> = async {
        guard_purge_fence(context, txn_id, &write.bucket, &write.key).await?;
        let (mut policies, binding) = guard_expected_bucket(
            context,
            txn_id,
            &write.bucket,
            write.group_id,
            write.expected_bucket.as_ref(),
        )
        .await?;
        let set_hint = binding.as_ref().map(DistributionBinding::hint);
        // Only the coordinator of the key's range writes, at its epoch.
        let epoch = match binding.as_ref() {
            Some(binding) => {
                // Read in the write transaction, so a fence set meanwhile conflicts it.
                let read = authority_read(binding.set_id, Some(txn_id));
                let event = send_storage_effect(context, read).await?;
                record_epoch(binding, &write.key, event)?
            }
            None => Default::default(),
        };
        if let (Some(hint), Some(planned)) = (set_hint, write.preassigned_version_id)
            && SetHint::of_version(planned) != hint
        {
            return Err(StorageError::WriteError(
                "planned reference version lacks the set hint".into(),
            )
            .into());
        }
        let version_id = write
            .preassigned_version_id
            .unwrap_or_else(|| mint_version(set_hint));
        policies.extend(write.inherited_policies.iter().copied());
        if let Some((connector, fingerprint)) = write.connector_guard.as_ref() {
            guard_connector_unchanged(context, txn_id, connector, *fingerprint).await?;
        }
        for connector in &write.mirror_guards {
            guard_record_unchanged(context, txn_id, connector).await?;
        }

        if write.preassigned_version_id.is_some()
            && let Some(existing) =
                read_blob_version(context, txn_id, &write.bucket, &write.key, version_id).await?
        {
            if !matches!(
                &existing.state,
                BlobVersionState::Reference { source, cached_metadata, .. }
                    if RootEntry::same_source(source, &write.version_source)
                        && source_metadata_matches(cached_metadata, &write.metadata)
            ) {
                return Err(
                    StorageError::WriteError("planned reference version changed".into()).into(),
                );
            }
            let commit = Effect::Storage(StorageEffect::CommitTransaction { txn_id });
            return match send_storage_effect(context, commit).await? {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    Ok((version_id, false))
                }
                _ => Err(StorageError::WriteError("unexpected commit event".into()).into()),
            };
        }

        let existing_pointer =
            read_current_pointer(context, txn_id, &write.bucket, &write.key).await?;
        let existing_version = match existing_pointer.as_ref() {
            Some(pointer) => {
                read_blob_version(
                    context,
                    txn_id,
                    &write.bucket,
                    &write.key,
                    pointer.version_id,
                )
                .await?
            }
            None => None,
        };
        if let (
            Some(pointer),
            Some(BlobVersion {
                state:
                    BlobVersionState::Reference {
                        source,
                        cached_metadata,
                        ..
                    },
                ..
            }),
        ) = (existing_pointer.as_ref(), existing_version.as_ref())
            && write.preassigned_version_id.is_none()
            && RootEntry::same_source(source, &write.version_source)
            && source_metadata_matches(cached_metadata, &write.metadata)
        {
            match context
                .storage_handle
                .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
                .await
            {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    return Ok((pointer.version_id, false));
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    return Err(MaterializeReferenceError::Storage(error));
                }
                _ => {
                    return Err(MaterializeReferenceError::Storage(
                        StorageError::WriteError("unexpected event".to_string()),
                    ));
                }
            }
        }
        // An offered directory observation is charged only while it is the head.
        let released = existing_version
            .as_ref()
            .and_then(|version| version.offered_size(&write.bucket, &write.key));
        let was_live = existing_version.is_some_and(|version| !version.is_deleted());
        let next_pointer = CurrentVersionPointer::next_for(existing_pointer.as_ref(), version_id)?;
        let next_pointer = stamp_head(next_pointer, binding.as_ref(), epoch);
        let generation = next_pointer.generation;

        for effect in build_transition_effects(
            &HeadAliasContext::new(
                write.realm_id,
                write.group_id,
                write.node_id,
                &write.bucket,
                &write.key,
            ),
            Some(next_pointer),
            None,
            Some(txn_id),
        )? {
            apply_storage_effect(context, effect).await?;
        }

        let version_key = VersionKey::new(&write.bucket, &write.key, version_id);
        let version = BlobVersion::reference(
            write.version_source.clone(),
            write.metadata.clone(),
            now,
            write.user_id,
            now,
        )
        .with_origin(Some(VersionOrigin::Source {
            job_id: write.job_id,
        }))
        .with_policies(policies)?;
        let version = stamp_version(version, binding.as_ref(), generation, epoch);
        let uploaded = SourceEvent::data_uploaded(
            (write.realm_id, write.group_id, write.node_id),
            (&write.bucket, &write.key),
            version_id,
            unix_timestamp_millis(),
        );
        let effect = write_version_effect(&version_key, &version, Some(txn_id))?;
        apply_storage_effect(context, journaled(effect, &uploaded)?).await?;
        for (key_space, key, value) in connector_reference_write(&version, &version_key)? {
            let row = StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: Some(txn_id),
            };
            apply_storage_effect(context, Effect::Storage(row)).await?;
        }
        // The obligation commits with the version, so its fan-out survives a crash.
        let auth = AuthContext {
            user_id: write.user_id,
            realm_id: write.realm_id,
            path_restrictions: write.restrictions.clone(),
            session: None,
        };
        let obligation = build_live_obligation(
            write.node_id,
            auth,
            write.bucket.clone(),
            write.key.clone(),
            version_id,
            false,
            Some(txn_id),
        )?;
        apply_storage_effect(context, obligation).await?;

        let referenced_bytes = write.metadata.content_length;
        let added = AddedVersion {
            version_id,
            epoch: version.original_epoch,
            generation,
            entry: entry_of(&version, referenced_bytes),
        };
        let inventory = version_written(binding.as_ref(), &write.key, added, true);
        let mut usage_update = UsageCounterUpdate::for_group(
            write.group_id,
            UsageDelta {
                objects: if was_live { 0 } else { 1 },
                referenced_bytes: i128::from(referenced_bytes) - i128::from(released.unwrap_or(0)),
                ..Default::default()
            },
        )
        .in_bucket(&write.bucket, RowChange::AddedVersion)
        .with_inventory(inventory);
        if !usage_update.is_noop() {
            run_usage_update(context, txn_id, &mut usage_update).await?;
        }

        match context
            .storage_handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await
        {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok((version_id, true)),
            Event::Storage(StorageEvent::Error { error }) => {
                Err(MaterializeReferenceError::Storage(error))
            }
            _ => Err(MaterializeReferenceError::Storage(
                StorageError::WriteError("unexpected event".to_string()),
            )),
        }
    }
    .await;

    if result.is_err() {
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await;
    }

    let (version_id, changed) = result?;
    if changed {
        send_snapshot_schedule(context).await;
    }
    Ok((version_id, changed))
}

async fn guard_purge_fence(
    context: &DriverContext,
    txn_id: TxnId,
    bucket: &str,
    key: &str,
) -> Result<(), MaterializeReferenceError> {
    check_write_fence(
        send_storage_effect(context, write_fence_read(bucket, Some(txn_id))).await?,
        bucket,
        key,
    )?;
    Ok(())
}

fn source_metadata_matches(left: &SourceMetadata, right: &SourceMetadata) -> bool {
    left.content_length == right.content_length
        && left.content_type == right.content_type
        && left.etag == right.etag
        && left.last_modified == right.last_modified
}

async fn apply_storage_effect(
    context: &DriverContext,
    effect: Effect,
) -> Result<(), MaterializeReferenceError> {
    match send_storage_effect(context, effect).await? {
        Event::Storage(StorageEvent::WriteResult { .. })
        | Event::Storage(StorageEvent::BatchWriteResult { .. })
        | Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(()),
        _ => Err(StorageError::WriteError("unexpected event".to_string()).into()),
    }
}

async fn send_storage_effect(
    context: &DriverContext,
    effect: Effect,
) -> Result<Event, MaterializeReferenceError> {
    let Effect::Storage(storage_effect) = effect else {
        return Err(MaterializeReferenceError::Storage(
            StorageError::InvalidEffect,
        ));
    };

    match context
        .storage_handle
        .send_storage_effect(storage_effect)
        .await
    {
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        event => Ok(event),
    }
}

async fn send_single_effect(
    context: &DriverContext,
    mut effects: Effects,
) -> Result<Event, MaterializeReferenceError> {
    if effects.len() != 1 {
        return Err(StorageError::InvalidEffect.into());
    }
    send_storage_effect(
        context,
        effects
            .pop()
            .expect("effect length was checked before popping"),
    )
    .await
}

async fn run_usage_update(
    context: &DriverContext,
    txn_id: TxnId,
    usage_update: &mut UsageCounterUpdate,
) -> Result<(), MaterializeReferenceError> {
    let mut effects = usage_update.start(txn_id);
    loop {
        let event = send_single_effect(context, effects).await?;
        effects = match usage_update.step(event, txn_id)? {
            Some(effects) => effects,
            None => return Ok(()),
        };
    }
}

async fn send_snapshot_schedule(context: &DriverContext) {
    let Effect::Task(task_effect) = schedule_snapshot_publish() else {
        return;
    };
    if persist_task_effect(&context.storage_handle, &task_effect)
        .await
        .is_err()
    {
        return;
    }
    if let Some(task_handle) = &context.task_handle {
        let _ = task_handle.send_effect(Effect::Task(task_effect)).await;
    }
}

async fn read_current_pointer(
    context: &DriverContext,
    txn_id: TxnId,
    bucket: &str,
    key: &str,
) -> Result<Option<CurrentVersionPointer>, MaterializeReferenceError> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            key: BlobHeadKey::new(bucket, key).to_bytes()?.into(),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .as_ref()
            .map(|value| CurrentVersionPointer::from_bytes(value.as_ref()))
            .transpose()
            .map_err(Into::into),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        _ => Err(StorageError::ReadError("unexpected event".to_string()).into()),
    }
}

async fn read_blob_version(
    context: &DriverContext,
    txn_id: TxnId,
    bucket: &str,
    key: &str,
    version_id: Ulid,
) -> Result<Option<BlobVersion>, MaterializeReferenceError> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: VersionKey::new(bucket, key, version_id).to_bytes()?.into(),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .as_ref()
            .map(|value| BlobVersion::from_bytes(value.as_ref()))
            .transpose()
            .map_err(Into::into),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        _ => Err(StorageError::ReadError("unexpected event".to_string()).into()),
    }
}

/// The connector record unchanged since it was resolved, read in the write transaction.
async fn guard_record_unchanged(
    context: &DriverContext,
    txn_id: TxnId,
    resolved_connector: &SourceConnector,
) -> Result<(), MaterializeReferenceError> {
    let current_connector = match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: SOURCE_INDEX_KEYSPACE.to_string(),
            key: source_connector_key(resolved_connector.group_id, resolved_connector.connector_id),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .as_ref()
            .map(|value| SourceConnector::from_bytes(value.as_ref()))
            .transpose()?,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        _ => return Err(StorageError::ReadError("unexpected event".to_string()).into()),
    };

    if current_connector.as_ref() != Some(resolved_connector) {
        return Err(StorageError::TransactionConflict.into());
    }
    Ok(())
}

async fn guard_connector_unchanged(
    context: &DriverContext,
    txn_id: TxnId,
    resolved_connector: &SourceConnector,
    resolved_secret_fingerprint: Option<[u8; 16]>,
) -> Result<(), MaterializeReferenceError> {
    guard_record_unchanged(context, txn_id, resolved_connector).await?;
    let connector_id = resolved_connector.connector_id;
    let event = context
        .storage_handle
        .send_effect(read_secret_effect(connector_id, Some(txn_id)))
        .await;
    let current_secret = match parse_secret_read(event, connector_id) {
        Ok(secret) => secret,
        Err(StorageReadError::Storage(error)) => return Err(error.into()),
        Err(StorageReadError::Conversion(error)) => return Err(error.into()),
    };

    let current_secret_fingerprint = current_secret.as_ref().map(secret_fingerprint);
    if current_secret_fingerprint != resolved_secret_fingerprint {
        return Err(StorageError::TransactionConflict.into());
    }

    Ok(())
}

async fn guard_expected_bucket(
    context: &DriverContext,
    txn_id: TxnId,
    bucket: &str,
    group_id: GroupId,
    expected: Option<&BucketInfo>,
) -> Result<(Vec<PlacementPolicyRef>, Option<DistributionBinding>), MaterializeReferenceError> {
    let current = match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: bucket.as_bytes().into(),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .as_ref()
            .map(|value| BucketInfo::from_bytes(value.as_ref()))
            .transpose()?,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        _ => return Err(StorageError::ReadError("unexpected event".to_string()).into()),
    };
    let unchanged = match expected {
        Some(expected) => {
            expected.group_id == group_id
                && current.as_ref().map(BucketInfo::identity) == Some(expected.identity())
        }
        None => current
            .as_ref()
            .is_some_and(|bucket| bucket.group_id == group_id),
    };
    if !unchanged {
        return Err(StorageError::TransactionConflict.into());
    }
    let binding = current
        .as_ref()
        .and_then(|bucket| bucket.distribution.clone());
    Ok((
        current
            .map(|bucket| bucket.placement_policies)
            .unwrap_or_default(),
        binding,
    ))
}

/// Heads the staged source and records it as a reference version: the head's connector and
/// its mirrors are re-checked when the version commits.
#[derive(Debug, PartialEq)]
pub struct MaterializeReferenceOperation {
    input: Option<MaterializeReferenceInput>,
    now: SystemTime,
    fresh: Ulid,
    head: Option<HeadSourceOperation>,
    write: Option<WriteReferenceOperation>,
    found: Option<(SourceConnector, SourceMetadata, VersionSourceBinding)>,
    output: Option<Result<MaterializeReferenceResult, MaterializeReferenceError>>,
}

impl MaterializeReferenceOperation {
    /// `now` stamps the version and `fresh` names it unless the bucket's set hints another id.
    pub fn new(input: MaterializeReferenceInput, now: SystemTime, fresh: Ulid) -> Self {
        Self {
            input: Some(input),
            now,
            fresh,
            head: None,
            write: None,
            found: None,
            output: None,
        }
    }

    fn fail(&mut self, error: MaterializeReferenceError) -> Effects {
        self.head = None;
        self.write = None;
        self.output = Some(Err(error));
        smallvec![]
    }

    /// Builds the reference write from the head, or hands the head error back.
    fn headed(&mut self, head: HeadSourceResult) -> Effects {
        let Some(input) = self.input.take() else {
            return self.fail(StorageError::InvalidEffect.into());
        };
        let mut version_source = build_source_binding(
            StagingStrategy::Reference,
            &head.connector,
            &head.metadata,
            input.source_path,
            Some(input.node_id),
            Some(head.connector.connector_id),
        );
        for (connector, path) in &input.mirrors {
            let mut descriptor = build_source_descriptor(
                connector,
                &head.metadata,
                path.clone(),
                Some(input.node_id),
            );
            // A version id names one server's copy, never another's.
            descriptor.version_selector = None;
            version_source.mirrors.push(SourceMirror {
                descriptor,
                connector_id: Some(connector.connector_id),
            });
        }
        let mirror_guards = (input.mirrors.into_iter()).map(|(connector, _)| connector);
        let write = ReferenceWrite {
            preassigned_version_id: None,
            group_id: input.group_id,
            user_id: input.user_id,
            realm_id: input.realm_id,
            node_id: input.node_id,
            bucket: input.bucket,
            key: input.key,
            expected_bucket: Some(input.expected_bucket),
            version_source: version_source.clone(),
            metadata: head.metadata.clone(),
            inherited_policies: input.inherited_policies,
            restrictions: input.restrictions,
            connector_guard: Some((head.connector.clone(), head.secret_fingerprint)),
            mirror_guards: mirror_guards.collect(),
            job_id: input.job_id,
        };
        self.found = Some((head.connector, head.metadata, version_source));
        let mut operation = WriteReferenceOperation::new(write, self.now, self.fresh);
        let effects = operation.start();
        self.write_step(operation, effects)
    }

    fn write_step(&mut self, operation: WriteReferenceOperation, effects: Effects) -> Effects {
        if !operation.is_complete() {
            self.write = Some(operation);
            return effects;
        }
        match (operation.finalize(), self.found.take()) {
            (Ok((version_id, _)), Some((connector, source_metadata, version_source))) => {
                self.output = Some(Ok(MaterializeReferenceResult {
                    connector,
                    source_metadata,
                    version_source,
                    version_id,
                }));
                effects
            }
            (Ok(_), None) => self.fail(StorageError::InvalidEffect.into()),
            (Err(error), _) => self.fail(error),
        }
    }

    fn head_step(&mut self, operation: HeadSourceOperation, effects: Effects) -> Effects {
        if !operation.is_complete() {
            self.head = Some(operation);
            return effects;
        }
        match operation.finalize() {
            Ok(head) => self.headed(head),
            Err(error) => self.fail(error.into()),
        }
    }
}

impl Operation for MaterializeReferenceOperation {
    type Output = MaterializeReferenceResult;
    type Error = MaterializeReferenceError;

    fn start(&mut self) -> Effects {
        let Some(input) = self.input.as_ref() else {
            return self.fail(StorageError::InvalidEffect.into());
        };
        let mut head = HeadSourceOperation::new(HeadSourceInput {
            group_id: input.group_id,
            connector_id: input.connector_id,
            source_path: input.source_path.clone(),
        });
        let effects = head.start();
        self.head_step(head, effects)
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Some(mut head) = self.head.take() {
            let effects = head.step(event);
            return self.head_step(head, effects);
        }
        if let Some(mut write) = self.write.take() {
            let effects = write.step(event);
            return self.write_step(write, effects);
        }
        let label = event.label();
        self.fail(StorageError::ReadError(format!("unexpected event {label}")).into())
    }

    fn is_complete(&self) -> bool {
        self.output.is_some() && self.head.is_none() && self.write.is_none()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(StorageError::ReadError(
            "the reference was not written".into(),
        )
        .into()))
    }

    fn abort(&mut self) -> Effects {
        match (self.head.as_mut(), self.write.as_mut()) {
            (Some(head), _) => head.abort(),
            (_, Some(write)) => write.abort(),
            _ => smallvec![],
        }
    }
}

/// Where a reference write is.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WriteState {
    Init,
    Begin,
    Fence,
    Bucket,
    Authority,
    Connector,
    Secret,
    Mirror,
    Planned,
    Pointer,
    Current,
    Rows,
    Usage,
    Commit,
    Schedule,
    Abort,
    Finish,
    Error,
}

/// Writes one reference version in one transaction. Outputs the version id and whether
/// anything changed: the same binding with the same observation at the head is kept.
#[derive(Debug, PartialEq)]
pub struct WriteReferenceOperation {
    write: ReferenceWrite,
    now: SystemTime,
    fresh: Ulid,
    state: WriteState,
    txn_id: Option<TxnId>,
    policies: Vec<PlacementPolicyRef>,
    binding: Option<DistributionBinding>,
    epoch: OriginalEpoch,
    version_id: Ulid,
    mirror: usize,
    pointer: Option<CurrentVersionPointer>,
    rows: VecDeque<Effect>,
    usage: Option<UsageCounterUpdate>,
    result: Option<(Ulid, bool)>,
    output: Option<Result<(Ulid, bool), MaterializeReferenceError>>,
}

impl WriteReferenceOperation {
    /// `now` stamps the version and `fresh` names it unless the bucket's set hints another id.
    pub fn new(write: ReferenceWrite, now: SystemTime, fresh: Ulid) -> Self {
        Self {
            write,
            now,
            fresh,
            state: WriteState::Init,
            txn_id: None,
            policies: Vec::new(),
            binding: None,
            epoch: OriginalEpoch::default(),
            version_id: fresh,
            mirror: 0,
            pointer: None,
            rows: VecDeque::new(),
            usage: None,
            result: None,
            output: None,
        }
    }

    fn txn(&self) -> Result<TxnId, MaterializeReferenceError> {
        self.txn_id.ok_or(StorageError::TransactionNotFound.into())
    }

    fn read(
        &mut self,
        state: WriteState,
        key_space: &str,
        key: Key,
    ) -> Result<Effects, MaterializeReferenceError> {
        let txn_id = self.txn()?;
        self.state = state;
        Ok(smallvec![Effect::Storage(StorageEffect::Read {
            key_space: key_space.to_string(),
            key,
            txn_id: Some(txn_id),
        })])
    }

    /// Ends with `error`, aborting the open transaction first.
    fn fail(&mut self, error: MaterializeReferenceError) -> Effects {
        self.output = Some(Err(error));
        match self.txn_id.take() {
            Some(txn_id) => {
                self.state = WriteState::Abort;
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            None => {
                self.state = WriteState::Error;
                smallvec![]
            }
        }
    }

    /// Commits and ends with `result`; a changed head schedules the usage snapshot afterwards.
    fn commit(&mut self, result: (Ulid, bool)) -> Result<Effects, MaterializeReferenceError> {
        let txn_id = self
            .txn_id
            .take()
            .ok_or(StorageError::TransactionNotFound)?;
        self.result = Some(result);
        self.state = WriteState::Commit;
        Ok(smallvec![Effect::Storage(
            StorageEffect::CommitTransaction { txn_id }
        )])
    }

    /// The bucket as read in the transaction must be the one the caller saw.
    fn bucket_read(&mut self, value: Option<Value>) -> Result<Effects, MaterializeReferenceError> {
        let current = (value.as_ref())
            .map(|value| BucketInfo::from_bytes(value.as_ref()))
            .transpose()?;
        let write = &self.write;
        let unchanged = match write.expected_bucket.as_ref() {
            Some(expected) => {
                expected.group_id == write.group_id
                    && current.as_ref().map(BucketInfo::identity) == Some(expected.identity())
            }
            None => (current.as_ref()).is_some_and(|bucket| bucket.group_id == write.group_id),
        };
        if !unchanged {
            return Err(StorageError::TransactionConflict.into());
        }
        self.binding = (current.as_ref()).and_then(|bucket| bucket.distribution.clone());
        self.policies = current
            .map(|bucket| bucket.placement_policies)
            .unwrap_or_default();
        match self.binding.as_ref() {
            // Only the coordinator of the key's range writes, at its epoch; the read in the
            // write transaction conflicts with a fence set meanwhile.
            Some(binding) => {
                let read = authority_read(binding.set_id, Some(self.txn()?));
                self.state = WriteState::Authority;
                Ok(smallvec![read])
            }
            None => self.named(),
        }
    }

    /// Names the version and starts the connector checks.
    fn named(&mut self) -> Result<Effects, MaterializeReferenceError> {
        let hint = self.binding.as_ref().map(DistributionBinding::hint);
        if let (Some(hint), Some(planned)) = (hint, self.write.preassigned_version_id)
            && SetHint::of_version(planned) != hint
        {
            return Err(StorageError::WriteError(
                "planned reference version lacks the set hint".into(),
            )
            .into());
        }
        self.version_id = match (self.write.preassigned_version_id, hint) {
            (Some(planned), _) => planned,
            (None, Some(hint)) => with_hint(self.fresh, hint),
            (None, None) => self.fresh,
        };
        let inherited = self.write.inherited_policies.iter().copied();
        self.policies.extend(inherited);
        match self.write.connector_guard.as_ref() {
            Some((connector, _)) => {
                let key = source_connector_key(connector.group_id, connector.connector_id);
                self.read(WriteState::Connector, SOURCE_INDEX_KEYSPACE, key)
            }
            None => self.next_mirror(),
        }
    }

    /// Re-reads the next mirror connector, or goes on to the head once all are checked.
    fn next_mirror(&mut self) -> Result<Effects, MaterializeReferenceError> {
        let Some(connector) = self.write.mirror_guards.get(self.mirror) else {
            return self.versions();
        };
        let key = source_connector_key(connector.group_id, connector.connector_id);
        self.mirror += 1;
        self.read(WriteState::Mirror, SOURCE_INDEX_KEYSPACE, key)
    }

    /// A planned version is read first; otherwise the current head.
    fn versions(&mut self) -> Result<Effects, MaterializeReferenceError> {
        let (bucket, key) = (&self.write.bucket, &self.write.key);
        match self.write.preassigned_version_id {
            Some(_) => {
                let row = VersionKey::new(bucket, key, self.version_id).to_bytes()?;
                self.read(WriteState::Planned, BLOB_VERSIONS_KEYSPACE, row.into())
            }
            None => self.pointer(),
        }
    }

    fn pointer(&mut self) -> Result<Effects, MaterializeReferenceError> {
        let head = BlobHeadKey::new(&self.write.bucket, &self.write.key).to_bytes()?;
        self.read(WriteState::Pointer, BLOB_HEAD_KEYSPACE, head.into())
    }

    /// Whether `version` already references the same source with the same observation.
    fn same_reference(&self, version: &BlobVersion) -> bool {
        matches!(
            &version.state,
            BlobVersionState::Reference { source, cached_metadata, .. }
                if RootEntry::same_source(source, &self.write.version_source)
                    && source_metadata_matches(cached_metadata, &self.write.metadata)
        )
    }

    /// The rows of a new head version, in write order, and its usage update.
    fn plan_rows(
        &mut self,
        existing: Option<BlobVersion>,
    ) -> Result<Effects, MaterializeReferenceError> {
        let txn_id = self.txn()?;
        let write = &self.write;
        // An offered directory observation is charged only while it is the head.
        let released =
            (existing.as_ref()).and_then(|version| version.offered_size(&write.bucket, &write.key));
        let was_live = existing.is_some_and(|version| !version.is_deleted());
        let version_id = self.version_id;
        let next_pointer = CurrentVersionPointer::next_for(self.pointer.as_ref(), version_id)?;
        let next_pointer = stamp_head(next_pointer, self.binding.as_ref(), self.epoch);
        let generation = next_pointer.generation;
        let alias = HeadAliasContext::new(
            write.realm_id,
            write.group_id,
            write.node_id,
            &write.bucket,
            &write.key,
        );
        let mut rows: VecDeque<Effect> =
            build_transition_effects(&alias, Some(next_pointer), None, Some(txn_id))?
                .into_iter()
                .collect();
        let version_key = VersionKey::new(&write.bucket, &write.key, version_id);
        let version = BlobVersion::reference(
            write.version_source.clone(),
            write.metadata.clone(),
            self.now,
            write.user_id,
            self.now,
        )
        .with_origin(Some(VersionOrigin::Source {
            job_id: write.job_id,
        }))
        .with_policies(std::mem::take(&mut self.policies))?;
        let version = stamp_version(version, self.binding.as_ref(), generation, self.epoch);
        let now_ms = self
            .now
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |since| {
                u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
            });
        let uploaded = SourceEvent::data_uploaded(
            (write.realm_id, write.group_id, write.node_id),
            (&write.bucket, &write.key),
            version_id,
            now_ms,
        );
        let effect = write_version_effect(&version_key, &version, Some(txn_id))?;
        rows.push_back(journaled(effect, &uploaded)?);
        for (key_space, key, value) in connector_reference_write(&version, &version_key)? {
            rows.push_back(Effect::Storage(StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: Some(txn_id),
            }));
        }
        // The obligation commits with the version, so its fan-out survives a crash.
        let auth = AuthContext {
            user_id: write.user_id,
            realm_id: write.realm_id,
            path_restrictions: write.restrictions.clone(),
            session: None,
        };
        rows.push_back(build_live_obligation(
            write.node_id,
            auth,
            write.bucket.clone(),
            write.key.clone(),
            version_id,
            false,
            Some(txn_id),
        )?);
        let referenced_bytes = write.metadata.content_length;
        let added = AddedVersion {
            version_id,
            epoch: version.original_epoch,
            generation,
            entry: entry_of(&version, referenced_bytes),
        };
        let inventory = version_written(self.binding.as_ref(), &write.key, added, true);
        let usage = UsageCounterUpdate::for_group(
            write.group_id,
            UsageDelta {
                objects: if was_live { 0 } else { 1 },
                referenced_bytes: i128::from(referenced_bytes) - i128::from(released.unwrap_or(0)),
                ..Default::default()
            },
        )
        .in_bucket(&write.bucket, RowChange::AddedVersion)
        .with_inventory(inventory);
        self.usage = (!usage.is_noop()).then_some(usage);
        self.rows = rows;
        self.next_row()
    }

    /// Writes the next planned row, then runs the usage update, then commits.
    fn next_row(&mut self) -> Result<Effects, MaterializeReferenceError> {
        if let Some(row) = self.rows.pop_front() {
            self.state = WriteState::Rows;
            return Ok(smallvec![row]);
        }
        let txn_id = self.txn()?;
        match self.usage.as_mut() {
            Some(usage) => {
                self.state = WriteState::Usage;
                Ok(usage.start(txn_id))
            }
            None => self.commit((self.version_id, true)),
        }
    }

    fn step_inner(&mut self, event: Event) -> Result<Effects, MaterializeReferenceError> {
        let (bucket, key) = (self.write.bucket.clone(), self.write.key.clone());
        match (self.state, event) {
            (WriteState::Begin, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                self.state = WriteState::Fence;
                Ok(smallvec![write_fence_read(&bucket, Some(txn_id))])
            }
            (_, Event::Storage(StorageEvent::Error { error })) => Err(error.into()),
            (WriteState::Fence, event) => {
                check_write_fence(event, &bucket, &key)?;
                self.read(
                    WriteState::Bucket,
                    S3_BUCKET_KEYSPACE,
                    bucket.as_bytes().into(),
                )
            }
            (WriteState::Bucket, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.bucket_read(value)
            }
            (WriteState::Authority, event) => {
                let binding = self.binding.as_ref().ok_or(StorageError::InvalidEffect)?;
                self.epoch = record_epoch(binding, &key, event)?;
                self.named()
            }
            (WriteState::Connector, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                let (connector, _) =
                    (self.write.connector_guard.as_ref()).ok_or(StorageError::InvalidEffect)?;
                guard_record(connector, value)?;
                let connector_id = connector.connector_id;
                self.state = WriteState::Secret;
                Ok(smallvec![read_secret_effect(
                    connector_id,
                    Some(self.txn()?)
                )])
            }
            (WriteState::Secret, event) => {
                let (connector, fingerprint) =
                    (self.write.connector_guard.as_ref()).ok_or(StorageError::InvalidEffect)?;
                let current = match parse_secret_read(event, connector.connector_id) {
                    Ok(secret) => secret,
                    Err(StorageReadError::Storage(error)) => return Err(error.into()),
                    Err(StorageReadError::Conversion(error)) => return Err(error.into()),
                };
                if current.as_ref().map(secret_fingerprint) != *fingerprint {
                    return Err(StorageError::TransactionConflict.into());
                }
                self.next_mirror()
            }
            (WriteState::Mirror, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                let checked = self.mirror.checked_sub(1);
                let connector = (checked.and_then(|index| self.write.mirror_guards.get(index)))
                    .ok_or(StorageError::InvalidEffect)?;
                guard_record(connector, value)?;
                self.next_mirror()
            }
            (WriteState::Planned, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                let existing = (value.as_ref())
                    .map(|value| BlobVersion::from_bytes(value.as_ref()))
                    .transpose()?;
                match existing {
                    Some(existing) if self.same_reference(&existing) => {
                        self.commit((self.version_id, false))
                    }
                    Some(_) => Err(StorageError::WriteError(
                        "planned reference version changed".into(),
                    )
                    .into()),
                    None => self.pointer(),
                }
            }
            (WriteState::Pointer, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.pointer = (value.as_ref())
                    .map(|value| CurrentVersionPointer::from_bytes(value.as_ref()))
                    .transpose()?;
                match self.pointer.as_ref() {
                    Some(pointer) => {
                        let row = VersionKey::new(&bucket, &key, pointer.version_id).to_bytes()?;
                        self.read(WriteState::Current, BLOB_VERSIONS_KEYSPACE, row.into())
                    }
                    None => self.plan_rows(None),
                }
            }
            (WriteState::Current, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                let existing = (value.as_ref())
                    .map(|value| BlobVersion::from_bytes(value.as_ref()))
                    .transpose()?;
                let kept = (self.write.preassigned_version_id.is_none())
                    .then(|| {
                        existing
                            .as_ref()
                            .filter(|version| self.same_reference(version))
                    })
                    .flatten()
                    .and(self.pointer.as_ref().map(|pointer| pointer.version_id));
                match kept {
                    Some(version_id) => self.commit((version_id, false)),
                    None => self.plan_rows(existing),
                }
            }
            (
                WriteState::Rows,
                Event::Storage(
                    StorageEvent::WriteResult { .. }
                    | StorageEvent::BatchWriteResult { .. }
                    | StorageEvent::DeleteResult { .. },
                ),
            ) => self.next_row(),
            (WriteState::Usage, event) => {
                let txn_id = self.txn()?;
                let usage = self.usage.as_mut().ok_or(StorageError::InvalidEffect)?;
                match usage.step(event, txn_id)? {
                    Some(effects) => Ok(effects),
                    None => {
                        self.usage = None;
                        self.commit((self.version_id, true))
                    }
                }
            }
            (WriteState::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                let result = self.result.ok_or(StorageError::InvalidEffect)?;
                if !result.1 {
                    self.state = WriteState::Finish;
                    self.output = Some(Ok(result));
                    return Ok(smallvec![]);
                }
                self.state = WriteState::Schedule;
                Ok(smallvec![schedule_snapshot_publish()])
            }
            // A missed snapshot timer only delays the usage snapshot; the commit stands.
            (WriteState::Schedule, Event::Task(_)) => {
                self.state = WriteState::Finish;
                self.output = self.result.map(Ok);
                Ok(smallvec![])
            }
            (state, event) => Err(StorageError::ReadError(format!(
                "unexpected event {} in state {state:?}",
                event.label()
            ))
            .into()),
        }
    }
}

/// The connector record must be unchanged since it was resolved.
fn guard_record(
    resolved: &SourceConnector,
    value: Option<Value>,
) -> Result<(), MaterializeReferenceError> {
    let current = (value.as_ref())
        .map(|value| SourceConnector::from_bytes(value.as_ref()))
        .transpose()?;
    match current.as_ref() == Some(resolved) {
        true => Ok(()),
        false => Err(StorageError::TransactionConflict.into()),
    }
}

impl Operation for WriteReferenceOperation {
    type Output = (Ulid, bool);
    type Error = MaterializeReferenceError;

    fn start(&mut self) -> Effects {
        self.state = WriteState::Begin;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if self.state == WriteState::Abort {
            self.state = WriteState::Error;
            return smallvec![];
        }
        if self.state == WriteState::Commit
            && let Event::Storage(StorageEvent::Error { error }) = &event
        {
            // A failed commit leaves no transaction to abort.
            let error = error.clone();
            self.state = WriteState::Error;
            self.output = Some(Err(error.into()));
            return smallvec![];
        }
        match self.step_inner(event) {
            Ok(effects) => effects,
            Err(error) => self.fail(error),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, WriteState::Finish | WriteState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            WriteState::Finish | WriteState::Error => self.output.unwrap_or(Err(
                StorageError::WriteError("the reference write did not finish".into()).into(),
            )),
            _ => Err(StorageError::WriteError("the reference write did not finish".into()).into()),
        }
    }

    fn abort(&mut self) -> Effects {
        abort_effects(&mut self.txn_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::repository::{delete_secret_effect, write_secret_effect};
    use crate::driver::drive;
    use crate::node::usage_stats::RebuildStatsOperation;
    use crate::replication::queue::{LiveObligationRecord, live_obligation_key};
    use crate::s3::object::put::{PutObjectConfig, PutObjectInput, PutObjectOperation};
    use crate::tests::staging::{create_http_connector, create_test_bucket, setup_driver_context};
    use aruna_core::effects::StorageEffect;
    use aruna_core::keyspaces::{
        BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, PATHS_INDEX_KEYSPACE, PURGE_FENCE_KEYSPACE,
        REPLICATION_OBLIGATION_KEYSPACE, SOURCE_INDEX_KEYSPACE, USAGE_STATS_KEYSPACE,
    };
    use aruna_core::node_vault::NodeVaultKey;
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::execution::job::JobId;
    use aruna_core::structs::execution::source_connector::{
        SourceConnectorKind, SourceConnectorSecret,
    };
    use aruna_core::structs::storage::blob::{
        BlobHeadKey, BlobVersion, CurrentVersionPointer, HashIndex,
    };
    use aruna_core::structs::storage::format::Compression;
    use aruna_core::structs::storage::routing::RoutingSnapshot;
    use aruna_core::structs::storage::storage_purge::{StoragePurgeFence, StoragePurgeScope};
    use aruna_core::structs::storage::usage::{UsageCounters, global_group_key, usage_group_key};
    use aruna_storage::storage;
    use axum::{Router, routing::get};
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tempfile::{TempDir, tempdir};
    use tokio::net::TcpListener;

    fn test_context() -> (TempDir, DriverContext) {
        let tempdir = tempdir().expect("tempdir must be created");
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        storage_handle.open_vault(NodeVaultKey::random());
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        (tempdir, context)
    }

    fn connector() -> SourceConnector {
        SourceConnector::new(
            Ulid::from_bytes([2u8; 16]),
            Ulid::from_bytes([1u8; 16]),
            "s3-source".to_string(),
            SourceConnectorKind::S3,
            HashMap::from([
                ("bucket".to_string(), "data".to_string()),
                ("region".to_string(), "eu-central-1".to_string()),
            ]),
            SystemTime::UNIX_EPOCH,
            SystemTime::UNIX_EPOCH,
            Default::default(),
        )
    }

    fn secret(value: &str) -> SourceConnectorSecret {
        SourceConnectorSecret::new(
            Ulid::from_bytes([2u8; 16]),
            HashMap::from([("access_key_id".to_string(), value.to_string())]),
            SystemTime::UNIX_EPOCH,
        )
        .expect("secret must be present")
    }

    async fn write_connector(context: &DriverContext, connector: &SourceConnector) {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: SOURCE_INDEX_KEYSPACE.to_string(),
                key: source_connector_key(connector.group_id, connector.connector_id),
                value: connector.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    async fn write_secret(context: &DriverContext, secret: &SourceConnectorSecret) {
        let event = context
            .storage_handle
            .send_effect(write_secret_effect(secret, None).unwrap())
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    async fn delete_connector(context: &DriverContext, connector: &SourceConnector) {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Delete {
                key_space: SOURCE_INDEX_KEYSPACE.to_string(),
                key: source_connector_key(connector.group_id, connector.connector_id),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::DeleteResult { .. })
        ));
    }

    async fn delete_secret(context: &DriverContext, connector_id: Ulid) {
        let event = context
            .storage_handle
            .send_effect(delete_secret_effect(connector_id, None))
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::DeleteResult { .. })
        ));
    }

    async fn start_write_transaction(context: &DriverContext) -> TxnId {
        match context
            .storage_handle
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        {
            Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
            event => panic!("unexpected event: {event:?}"),
        }
    }

    fn assert_conflict(result: Result<(), MaterializeReferenceError>) {
        assert_eq!(
            result,
            Err(MaterializeReferenceError::Storage(
                StorageError::TransactionConflict,
            )),
        );
    }

    async fn read_value(
        context: &DriverContext,
        key_space: &str,
        key: Vec<u8>,
    ) -> Option<aruna_core::types::Value> {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: key_space.to_string(),
                key: key.into(),
                txn_id: None,
            })
            .await
        else {
            panic!("unexpected storage read result");
        };

        value
    }

    async fn read_usage_counters(context: &DriverContext, key: Vec<u8>) -> UsageCounters {
        read_value(context, USAGE_STATS_KEYSPACE, key)
            .await
            .map(|value| UsageCounters::from_bytes(value.as_ref()).unwrap())
            .unwrap_or_default()
    }

    async fn spawn_reference_server(body: &'static str) -> (tokio::task::JoinHandle<()>, String) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let app =
                Router::new().route(
                    "/folder/file.txt",
                    get(move || async move {
                        ([("etag", "etag-1"), ("content-type", "text/plain")], body)
                    }),
                );
            axum::serve(listener, app).await.unwrap();
        });
        (server, format!("http://{addr}"))
    }

    #[tokio::test]
    async fn guard_rejects_deleted() {
        let (_tempdir, context) = test_context();
        let connector = connector();
        let resolved_secret = secret("old-key");
        write_connector(&context, &connector).await;
        write_secret(&context, &resolved_secret).await;

        delete_connector(&context, &connector).await;
        let txn_id = start_write_transaction(&context).await;

        assert_conflict(
            guard_connector_unchanged(
                &context,
                txn_id,
                &connector,
                Some(secret_fingerprint(&resolved_secret)),
            )
            .await,
        );
    }

    #[tokio::test]
    async fn guard_rejects_changed() {
        let (_tempdir, context) = test_context();
        let connector = connector();
        let resolved_secret = secret("old-key");
        write_connector(&context, &connector).await;
        write_secret(&context, &resolved_secret).await;

        write_secret(&context, &secret("new-key")).await;
        let txn_id = start_write_transaction(&context).await;

        assert_conflict(
            guard_connector_unchanged(
                &context,
                txn_id,
                &connector,
                Some(secret_fingerprint(&resolved_secret)),
            )
            .await,
        );
    }

    #[tokio::test]
    async fn guard_rejects_removed() {
        let (_tempdir, context) = test_context();
        let connector = connector();
        let resolved_secret = secret("old-key");
        write_connector(&context, &connector).await;
        write_secret(&context, &resolved_secret).await;

        delete_secret(&context, connector.connector_id).await;
        let txn_id = start_write_transaction(&context).await;

        assert_conflict(
            guard_connector_unchanged(
                &context,
                txn_id,
                &connector,
                Some(secret_fingerprint(&resolved_secret)),
            )
            .await,
        );
    }

    #[tokio::test]
    async fn guard_rejects_added() {
        let (_tempdir, context) = test_context();
        let connector = connector();
        write_connector(&context, &connector).await;

        write_secret(&context, &secret("new-key")).await;
        let txn_id = start_write_transaction(&context).await;

        assert_conflict(guard_connector_unchanged(&context, txn_id, &connector, None).await);
    }

    #[tokio::test]
    async fn bucket_rejects_recreate() {
        let (_tempdir, context) = test_context();
        let realm_id = RealmId::from_bytes([6u8; 32]);
        let group_id = Ulid::generate();
        let expected = BucketInfo {
            group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: aruna_core::UserId::local(Ulid::generate(), realm_id),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };
        let recreated = BucketInfo {
            created_at: SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1),
            ..expected.clone()
        };
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_BUCKET_KEYSPACE.to_string(),
                key: b"bucket-a".to_vec().into(),
                value: recreated.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
        let txn_id = start_write_transaction(&context).await;

        assert_conflict(
            guard_expected_bucket(&context, txn_id, "bucket-a", group_id, Some(&expected))
                .await
                .map(|_| ()),
        );
    }

    #[tokio::test]
    async fn reference_keeps_history() {
        // The prior version's hash-path entry survives while the new reference
        // version is written.
        let test_context = setup_driver_context().await;
        let context = &test_context.driver_context;
        let group_id = Ulid::generate();
        let realm_id = RealmId::from_bytes([9u8; 32]);
        let node_id = iroh::SecretKey::generate().public();
        let user_id = aruna_core::UserId::local(Ulid::generate(), realm_id);
        let expected_bucket = create_test_bucket(context, group_id, user_id, "bucket-a").await;

        let initial = drive(
            PutObjectOperation::new(PutObjectConfig {
                user_id,
                group_id,
                realm_id,
                node_id,
                request: PutObjectInput {
                    bucket: "bucket-a".to_string(),
                    key: "object.txt".to_string(),
                    content_length: Some(5),
                    body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(
                        &b"hello"[..],
                    ))),
                },
                expected_checksums: vec![],
                checksum_type: None,
                exists: false,
                version_source: None,
                preassigned_version_id: None,
                quota_ceiling: None,
                routing: RoutingSnapshot::single(group_id),
            }),
            context,
        )
        .await
        .unwrap();
        let initial_hash: [u8; 32] = initial.location.get_blake3().unwrap().try_into().unwrap();

        let (server, endpoint) = spawn_reference_server("ref-data").await;
        let connector = create_http_connector(context, group_id, &endpoint).await;

        let result = stage_reference_blob(
            context,
            MaterializeReferenceInput {
                job_id: None,
                group_id,
                user_id,
                realm_id,
                node_id,
                connector_id: connector.connector_id,
                source_path: "folder/file.txt".to_string(),
                bucket: "bucket-a".to_string(),
                key: "object.txt".to_string(),
                expected_bucket,
                inherited_policies: Vec::new(),
                restrictions: None,
                mirrors: Vec::new(),
            },
        )
        .await
        .unwrap();

        server.abort();
        let _ = server.await;

        let blob_head_value = read_value(
            context,
            BLOB_HEAD_KEYSPACE,
            BlobHeadKey::new("bucket-a", "object.txt")
                .to_bytes()
                .unwrap(),
        )
        .await
        .expect("missing blob head entry");
        assert_eq!(
            CurrentVersionPointer::from_bytes(blob_head_value.as_ref()).unwrap(),
            CurrentVersionPointer::new_with_generation(result.version_id, 2)
        );

        let blob_version_value = read_value(
            context,
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new("bucket-a", "object.txt", result.version_id)
                .to_bytes()
                .unwrap(),
        )
        .await
        .expect("missing blob version entry");
        let blob_version = BlobVersion::from_bytes(blob_version_value.as_ref()).unwrap();
        assert!(!blob_version.is_materialized());
        assert!(!blob_version.is_deleted());
        assert_eq!(blob_version.source_binding(), Some(&result.version_source));

        let historical_hash_path = read_value(
            context,
            PATHS_INDEX_KEYSPACE,
            HashIndex::new(
                initial_hash,
                initial.version_id,
                realm_id,
                group_id,
                node_id,
                "bucket-a",
                "object.txt",
            )
            .to_bytes()
            .unwrap(),
        )
        .await
        .expect("missing historical materialized hash path entry");
        assert!(historical_hash_path.is_empty());

        let group_usage = read_usage_counters(context, usage_group_key(group_id)).await;
        assert_eq!(group_usage.objects, 1);
        assert_eq!(group_usage.logical_bytes, 5);
        assert_eq!(group_usage.referenced_bytes, 8);
        let global_usage = read_usage_counters(context, global_group_key(group_id)).await;
        assert_eq!(global_usage.objects, 1);
        assert_eq!(global_usage.logical_bytes, 5);
        assert_eq!(global_usage.referenced_bytes, 8);

        // A recount from the stored rows books the same charges.
        drive(RebuildStatsOperation::new(), context).await.unwrap();
        assert_eq!(
            read_usage_counters(context, usage_group_key(group_id)).await,
            group_usage
        );
        assert_eq!(
            read_usage_counters(context, global_group_key(group_id)).await,
            global_usage
        );
    }

    #[tokio::test]
    async fn reference_bypasses_quota() {
        let test_context = setup_driver_context().await;
        let context = &test_context.driver_context;
        let group_id = Ulid::generate();
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let node_id = iroh::SecretKey::generate().public();
        let user_id = aruna_core::UserId::local(Ulid::generate(), realm_id);
        let expected_bucket = create_test_bucket(context, group_id, user_id, "bucket-a").await;
        let (server, endpoint) = spawn_reference_server("ref-data").await;
        let connector = create_http_connector(context, group_id, &endpoint).await;

        let result = stage_reference_blob(
            context,
            MaterializeReferenceInput {
                job_id: None,
                group_id,
                user_id,
                realm_id,
                node_id,
                connector_id: connector.connector_id,
                source_path: "folder/file.txt".to_string(),
                bucket: "bucket-a".to_string(),
                key: "object.txt".to_string(),
                expected_bucket,
                inherited_policies: Vec::new(),
                restrictions: None,
                mirrors: Vec::new(),
            },
        )
        .await;

        server.abort();
        let _ = server.await;

        assert!(result.is_ok());
        let usage = read_usage_counters(context, usage_group_key(group_id)).await;
        assert_eq!(usage.logical_bytes, 0);
        assert_eq!(usage.referenced_bytes, 8);
    }

    #[tokio::test]
    async fn reference_obeys_fence() {
        let (_tempdir, context) = test_context();
        let fence = StoragePurgeFence {
            job_id: JobId::from_bytes([11; 16]),
            scope: StoragePurgeScope::File {
                bucket: "bucket-a".to_string(),
                key: "object.txt".to_string(),
            },
        };
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: PURGE_FENCE_KEYSPACE.to_string(),
                key: b"bucket-a".to_vec().into(),
                value: fence.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));

        let txn_id = start_write_transaction(&context).await;
        let result = guard_purge_fence(&context, txn_id, "bucket-a", "object.txt").await;

        assert_eq!(
            result,
            Err(MaterializeReferenceError::Purge(PurgeFenceError::Suspended))
        );
    }

    #[tokio::test]
    async fn reference_reuses_version() {
        let test_context = setup_driver_context().await;
        let context = &test_context.driver_context;
        let group_id = Ulid::generate();
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let node_id = iroh::SecretKey::generate().public();
        let user_id = aruna_core::UserId::local(Ulid::generate(), realm_id);
        let expected_bucket = create_test_bucket(context, group_id, user_id, "bucket-a").await;
        let (server, endpoint) = spawn_reference_server("ref-data").await;
        let connector = create_http_connector(context, group_id, &endpoint).await;
        let input = MaterializeReferenceInput {
            job_id: None,
            group_id,
            user_id,
            realm_id,
            node_id,
            connector_id: connector.connector_id,
            source_path: "folder/file.txt".to_string(),
            bucket: "bucket-a".to_string(),
            key: "object.txt".to_string(),
            expected_bucket,
            inherited_policies: Vec::new(),
            restrictions: None,
            mirrors: Vec::new(),
        };

        let first = stage_reference_blob(context, input.clone()).await.unwrap();
        let second = stage_reference_blob(context, input).await.unwrap();
        server.abort();
        let _ = server.await;

        assert_eq!(second.version_id, first.version_id);
        let pointer = read_value(
            context,
            BLOB_HEAD_KEYSPACE,
            BlobHeadKey::new("bucket-a", "object.txt")
                .to_bytes()
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            CurrentVersionPointer::from_bytes(pointer.as_ref()).unwrap(),
            CurrentVersionPointer::new_with_generation(first.version_id, 1)
        );
        let usage = read_usage_counters(context, usage_group_key(group_id)).await;
        assert_eq!(usage.referenced_bytes, 8);
    }

    // An item's further sources join its binding without a version id; a further source's
    // connector that changed before the write commits refuses the write.
    #[tokio::test]
    async fn mirror_connector_guarded() {
        let test_context = setup_driver_context().await;
        let context = &test_context.driver_context;
        let group_id = Ulid::generate();
        let realm_id = RealmId::from_bytes([7; 32]);
        let node_id = iroh::SecretKey::generate().public();
        let user_id = aruna_core::UserId::local(Ulid::generate(), realm_id);
        let bucket = create_test_bucket(context, group_id, user_id, "bucket-a").await;
        let (server, endpoint) = spawn_reference_server("ref-data").await;
        let connector = create_http_connector(context, group_id, &endpoint).await;
        let mirror = create_http_connector(context, group_id, &endpoint).await;
        let input = |key: &str, mirror: SourceConnector| MaterializeReferenceInput {
            job_id: None,
            group_id,
            user_id,
            realm_id,
            node_id,
            connector_id: connector.connector_id,
            source_path: "folder/file.txt".into(),
            bucket: "bucket-a".into(),
            key: key.into(),
            expected_bucket: bucket.clone(),
            inherited_policies: Vec::new(),
            restrictions: None,
            mirrors: vec![(mirror, "mirror/file.txt".into())],
        };
        let staged = stage_reference_blob(context, input("a.txt", mirror.clone()))
            .await
            .unwrap();
        let added = &staged.version_source.mirrors[0];
        assert_eq!(added.connector_id, Some(mirror.connector_id));
        assert_eq!(added.descriptor.source_path, "mirror/file.txt");
        assert_eq!(added.descriptor.version_selector, None);
        let mut replaced = mirror.clone();
        replaced.name = "replaced".to_string();
        let refused = stage_reference_blob(context, input("b.txt", replaced)).await;
        assert!(
            matches!(
                refused,
                Err(MaterializeReferenceError::Storage(
                    StorageError::TransactionConflict
                ))
            ),
            "{refused:?}"
        );
        server.abort();
    }

    #[tokio::test]
    async fn reference_resumes_version() {
        let test_context = setup_driver_context().await;
        let context = &test_context.driver_context;
        let group_id = Ulid::generate();
        let realm_id = RealmId::from_bytes([7; 32]);
        let node_id = iroh::SecretKey::generate().public();
        let user_id = aruna_core::UserId::local(Ulid::generate(), realm_id);
        let bucket = create_test_bucket(context, group_id, user_id, "bucket-a").await;
        let (server, endpoint) = spawn_reference_server("ref-data").await;
        let connector = create_http_connector(context, group_id, &endpoint).await;
        let original = stage_reference_blob(
            context,
            MaterializeReferenceInput {
                job_id: None,
                group_id,
                user_id,
                realm_id,
                node_id,
                connector_id: connector.connector_id,
                source_path: "folder/file.txt".into(),
                bucket: "bucket-a".into(),
                key: "object.txt".into(),
                expected_bucket: bucket.clone(),
                inherited_policies: Vec::new(),
                restrictions: None,
                mirrors: Vec::new(),
            },
        )
        .await
        .unwrap();
        server.abort();
        let _ = server.await;
        let planned = Ulid::generate();
        let input = |size| ReferenceWrite {
            job_id: None,
            preassigned_version_id: Some(planned),
            group_id,
            user_id,
            realm_id,
            node_id,
            bucket: "bucket-a".into(),
            key: "object.txt".into(),
            expected_bucket: Some(bucket.clone()),
            version_source: original.version_source.clone(),
            metadata: SourceMetadata {
                content_length: size,
                ..original.source_metadata.clone()
            },
            inherited_policies: Vec::new(),
            connector_guard: None,
            mirror_guards: Vec::new(),
            restrictions: None,
        };
        assert_eq!(
            write_reference_version(context, input(8)).await.unwrap(),
            (planned, true)
        );
        assert_ne!(planned, original.version_id);
        // The written version commits with its replication obligation.
        let record = LiveObligationRecord::new(
            node_id,
            AuthContext {
                user_id,
                realm_id,
                path_restrictions: None,
                session: None,
            },
            "bucket-a".into(),
            "object.txt".into(),
            planned,
            false,
        );
        let key = live_obligation_key(&record).unwrap();
        let obligation = read_value(context, REPLICATION_OBLIGATION_KEYSPACE, key.to_vec()).await;
        assert!(obligation.is_some(), "the reference has no obligation");
        assert_eq!(
            write_reference_version(context, input(8)).await.unwrap(),
            (planned, false)
        );
        assert!(write_reference_version(context, input(9)).await.is_err());
        assert_eq!(
            read_usage_counters(context, usage_group_key(group_id))
                .await
                .referenced_bytes,
            16
        );
    }

    #[tokio::test]
    async fn reference_releases_observation() {
        // A reference written over a head observation takes its place, so the
        // observation stops being charged and a recount agrees.
        use crate::staging::offered_directory::{OfferDirectoryInput, offer_directory};
        use aruna_core::structs::execution::staging::PortableSourceDescriptor;

        let test_context = setup_driver_context().await;
        let context = &test_context.driver_context;
        let root = tempdir().unwrap();
        std::fs::write(root.path().join("data.bin"), b"offered").unwrap();
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let offer = OfferDirectoryInput {
            bucket: "offered".to_string(),
            root: root.path().to_str().unwrap().to_string(),
            group_id: Ulid::generate(),
            realm_id,
            node_id: iroh::SecretKey::generate().public(),
            user_id: aruna_core::UserId::new(Ulid::generate(), realm_id),
        };
        offer_directory(context, offer.clone()).await.unwrap();

        let write = ReferenceWrite {
            job_id: None,
            preassigned_version_id: None,
            group_id: offer.group_id,
            user_id: offer.user_id,
            realm_id,
            node_id: offer.node_id,
            bucket: offer.bucket.clone(),
            key: "data.bin".to_string(),
            expected_bucket: None,
            version_source: VersionSourceBinding {
                strategy: StagingStrategy::Reference,
                descriptor: PortableSourceDescriptor {
                    kind: SourceConnectorKind::Http,
                    public_config: HashMap::new(),
                    source_path: "folder/file.txt".to_string(),
                    version_selector: None,
                    capabilities: Vec::new(),
                    origin_node_id: None,
                    namespace: None,
                    root: None,
                },
                connector_id: Some(Ulid::generate()),
                mirrors: Vec::new(),
            },
            metadata: SourceMetadata {
                content_length: 4,
                content_type: None,
                etag: None,
                last_modified: None,
                source_version: None,
                digests: Default::default(),
            },
            inherited_policies: Vec::new(),
            connector_guard: None,
            restrictions: None,
            mirror_guards: Vec::new(),
        };
        write_reference_version(context, write).await.unwrap();

        let usage = read_usage_counters(context, usage_group_key(offer.group_id)).await;
        assert_eq!((usage.objects, usage.referenced_bytes), (1, 4));
        drive(RebuildStatsOperation::new(), context).await.unwrap();
        assert_eq!(
            read_usage_counters(context, usage_group_key(offer.group_id)).await,
            usage
        );
    }
}
