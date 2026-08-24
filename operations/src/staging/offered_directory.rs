//! Offering a directory as a read-only bucket on the device that holds it.
//!
//! The bucket's objects are observations: a reference version per file, bound
//! to the device-local registration and never to a path. Writes to such a
//! bucket are refused; the files change only on the owner's own filesystem.

use crate::blob::blob_keyspace_helper::{
    HeadAliasContext, build_head_transition_effects, write_blob_version_effect,
};
use crate::driver::{DriverContext, drive};
use crate::s3::create_bucket::{CreateBucketError, CreateBucketOperation};
use crate::usage_stats::{UsageCounterUpdate, UsageUpdateError};
use aruna_core::effects::{Effect, IterStart, StagingSourceEffect, StorageEffect};
use aruna_core::errors::{ConversionError, StagingSourceError, StorageError};
use aruna_core::events::{Event, StagingSourceEvent, StorageEvent};
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, OFFERED_DIRECTORY_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::structs::{
    BlobHeadKey, BlobVersion, BlobVersionState, BucketInfo, CurrentVersionPointer,
    OFFERED_DIRECTORY_BUCKET, OFFERED_DIRECTORY_ROOT, OfferedDirectory, PortableSourceDescriptor,
    RealmId, ResolvedSourceAccess, SourceConnectorKind, SourceEntry, SourceMetadata,
    StagingStrategy, UsageDelta, VersionKey, VersionSourceBinding,
};
use aruna_core::types::{GroupId, Key, NodeId, TxnId, UserId};
use std::collections::{BTreeSet, HashMap};
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

/// Entries fetched per list round. The walk pages so a large directory does not
/// build one unbounded response.
const LIST_PAGE: usize = 512;

/// Upper bound on the files one offer registers. A directory beyond it is
/// refused whole rather than offered half.
const MAX_OFFERED_FILES: usize = 100_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfferDirectoryInput {
    pub bucket: String,
    /// Root as the owner gives it. It is stored device-locally and resolved on
    /// every access; nothing derived from it is ever published.
    pub root: String,
    pub group_id: GroupId,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub user_id: UserId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfferDirectoryResult {
    pub bucket: String,
    pub files: usize,
    /// Observations tombstoned because their file is gone from the root.
    pub removed: usize,
}

/// The parts of an offer every observation write needs. The root is absent on
/// purpose: an observation is bound to the registration, never to a path.
#[derive(Clone, Debug, Eq, PartialEq)]
struct ObservationScope {
    bucket: String,
    group_id: GroupId,
    realm_id: RealmId,
    node_id: NodeId,
    user_id: UserId,
}

impl OfferDirectoryInput {
    fn scope(&self) -> ObservationScope {
        ObservationScope {
            bucket: self.bucket.clone(),
            group_id: self.group_id,
            realm_id: self.realm_id,
            node_id: self.node_id,
            user_id: self.user_id,
        }
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum OfferedDirectoryError {
    #[error("bucket `{0}` already exists and is not an offered directory")]
    BucketTaken(String),
    #[error("bucket `{0}` is an offered directory and is read-only")]
    ReadOnly(String),
    #[error("the offered directory holds more than {0} files")]
    TooManyFiles(usize),
    #[error("this node cannot read local directories")]
    HandleMissing,
    #[error(transparent)]
    Source(#[from] StagingSourceError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Bucket(#[from] CreateBucketError),
    #[error(transparent)]
    Usage(#[from] UsageUpdateError),
}

/// Refuses a write addressed to an offered bucket. Called on the write path so
/// the read-only rule lives with the registration, not in a transport handler.
pub async fn guard_bucket_write(
    context: &DriverContext,
    bucket: &str,
) -> Result<(), OfferedDirectoryError> {
    match read_offered(context, bucket, None).await? {
        Some(_) => Err(OfferedDirectoryError::ReadOnly(bucket.to_string())),
        None => Ok(()),
    }
}

/// Registers `root` as a read-only bucket and mints one reference version per
/// file it currently holds. Re-offering the same bucket refreshes the inventory:
/// unchanged files keep their version, changed ones gain a successor and files
/// that vanished are tombstoned.
pub async fn offer_directory(
    context: &DriverContext,
    input: OfferDirectoryInput,
) -> Result<OfferDirectoryResult, OfferedDirectoryError> {
    check_root(context, &input.root).await?;
    // The whole walk happens before anything is written, so an offer over the
    // file cap is refused whole instead of leaving a bucket and half an
    // inventory behind.
    let entries = walk_root(context, &input.root).await?;
    register_bucket(context, &input).await?;

    let scope = input.scope();
    for entry in &entries {
        write_observation(context, &scope, entry).await?;
    }
    let offered = entries
        .iter()
        .map(|entry| entry.path.clone())
        .collect::<BTreeSet<_>>();
    let removed = sweep_observations(context, &scope, &offered).await?;

    Ok(OfferDirectoryResult {
        bucket: input.bucket,
        files: entries.len(),
        removed,
    })
}

/// Every file under the offered root, refusing a root beyond the cap before a
/// single observation is written.
async fn walk_root(
    context: &DriverContext,
    root: &str,
) -> Result<Vec<SourceEntry>, OfferedDirectoryError> {
    let mut entries = Vec::new();
    let mut offset = 0usize;
    loop {
        let (page, truncated) = list_page(context, root, offset).await?;
        if over_cap(entries.len(), page.len()) {
            return Err(OfferedDirectoryError::TooManyFiles(MAX_OFFERED_FILES));
        }
        offset += page.len().max(1);
        entries.extend(page);
        if !truncated {
            return Ok(entries);
        }
    }
}

/// Whether one more page would take the offer past the file cap.
fn over_cap(collected: usize, page: usize) -> bool {
    collected.saturating_add(page) > MAX_OFFERED_FILES
}

/// The binding a served object carries: the offered bucket and the file's path
/// inside it. The root is deliberately absent.
fn offered_binding(bucket: &str, path: &str, node_id: NodeId) -> VersionSourceBinding {
    VersionSourceBinding {
        strategy: StagingStrategy::Reference,
        descriptor: PortableSourceDescriptor {
            kind: SourceConnectorKind::LocalDirectory,
            public_config: HashMap::from([(
                OFFERED_DIRECTORY_BUCKET.to_string(),
                bucket.to_string(),
            )]),
            source_path: path.to_string(),
            version_selector: None,
            capabilities: Vec::new(),
            origin_node_id: Some(node_id),
        },
        connector_id: None,
    }
}

fn entry_metadata(entry: &SourceEntry) -> SourceMetadata {
    let size = entry.size.unwrap_or_default();
    SourceMetadata {
        content_length: size,
        content_type: None,
        etag: Some(aruna_core::structs::weak_fingerprint(size, entry.modified)),
        last_modified: entry.modified,
        source_version: None,
    }
}

fn local_access(root: &str, path: &str) -> ResolvedSourceAccess {
    ResolvedSourceAccess::OpenDal {
        kind: SourceConnectorKind::LocalDirectory,
        config: HashMap::from([(OFFERED_DIRECTORY_ROOT.to_string(), root.to_string())]),
        path: path.to_string(),
        version: None,
    }
}

async fn check_root(context: &DriverContext, root: &str) -> Result<(), OfferedDirectoryError> {
    let event = send_source_effect(
        context,
        StagingSourceEffect::Check {
            access: local_access(root, ""),
        },
    )
    .await?;
    match event {
        StagingSourceEvent::CheckResult => Ok(()),
        StagingSourceEvent::Error { error } => Err(error.into()),
        _ => Err(StagingSourceError::InvalidEffect.into()),
    }
}

async fn list_page(
    context: &DriverContext,
    root: &str,
    offset: usize,
) -> Result<(Vec<SourceEntry>, bool), OfferedDirectoryError> {
    let event = send_source_effect(
        context,
        StagingSourceEffect::List {
            access: local_access(root, ""),
            offset,
            limit: LIST_PAGE,
            recursive: true,
            files_only: true,
        },
    )
    .await?;
    match event {
        StagingSourceEvent::ListResult { entries, truncated } => Ok((entries, truncated)),
        StagingSourceEvent::Error { error } => Err(error.into()),
        _ => Err(StagingSourceError::InvalidEffect.into()),
    }
}

async fn send_source_effect(
    context: &DriverContext,
    effect: StagingSourceEffect,
) -> Result<StagingSourceEvent, OfferedDirectoryError> {
    let blob_handle = context
        .blob_handle
        .as_ref()
        .ok_or(OfferedDirectoryError::HandleMissing)?;
    match blob_handle.send_staging_source_effect(effect).await {
        Event::StagingSource(event) => Ok(event),
        _ => Err(StagingSourceError::InvalidEffect.into()),
    }
}

/// Claims the bucket for this offer. An existing bucket that is not already
/// this offer's is left alone: an ordinary bucket must not become read-only.
async fn register_bucket(
    context: &DriverContext,
    input: &OfferDirectoryInput,
) -> Result<(), OfferedDirectoryError> {
    let existing = read_offered(context, &input.bucket, None).await?;
    if existing.is_none() && read_bucket(context, &input.bucket).await?.is_some() {
        return Err(OfferedDirectoryError::BucketTaken(input.bucket.clone()));
    }
    let now = SystemTime::now();
    if existing.is_none() {
        let operation = CreateBucketOperation::new(
            input.bucket.clone(),
            BucketInfo {
                group_id: input.group_id,
                created_at: now,
                created_by: input.user_id,
                cors_configuration: None,
                storage_routing: Vec::new(),
                placement_policies: Vec::new(),
                placement_policy_generation: 0,
            },
        );
        match drive(operation, context).await {
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(error))) | Err(error) => {
                return Err(OfferedDirectoryError::Bucket(error));
            }
            Ok(None) => {
                return Err(
                    StorageError::WriteError("bucket creation did not finish".to_string()).into(),
                );
            }
        }
    }
    let record = OfferedDirectory {
        bucket: input.bucket.clone(),
        group_id: input.group_id,
        root: input.root.clone(),
        created_at: existing.map(|record| record.created_at).unwrap_or(now),
        created_by: input.user_id,
    };
    apply(
        context,
        Effect::Storage(StorageEffect::Write {
            key_space: OFFERED_DIRECTORY_KEYSPACE.to_string(),
            key: input.bucket.as_bytes().into(),
            value: record.to_bytes()?.into(),
            txn_id: None,
        }),
    )
    .await?;
    Ok(())
}

/// One file's observation. An unchanged file keeps its version, so re-offering
/// a directory does not churn the identities realm nodes already reference.
async fn write_observation(
    context: &DriverContext,
    scope: &ObservationScope,
    entry: &SourceEntry,
) -> Result<(), OfferedDirectoryError> {
    let binding = offered_binding(&scope.bucket, &entry.path, scope.node_id);
    let metadata = entry_metadata(entry);
    let txn_id = start_transaction(context).await?;

    let result = observe(context, scope, entry, &binding, &metadata, txn_id).await;
    if result.is_err() {
        abort(context, txn_id).await;
        return result;
    }
    commit(context, txn_id).await
}

async fn observe(
    context: &DriverContext,
    input: &ObservationScope,
    entry: &SourceEntry,
    binding: &VersionSourceBinding,
    metadata: &SourceMetadata,
    txn_id: TxnId,
) -> Result<(), OfferedDirectoryError> {
    let pointer = read_pointer(context, &input.bucket, &entry.path, txn_id).await?;
    let existing = match pointer.as_ref() {
        Some(pointer) => read_version(context, &input.bucket, &entry.path, pointer, txn_id).await?,
        None => None,
    };
    if let Some(BlobVersion {
        state:
            BlobVersionState::Reference {
                source,
                cached_metadata,
                ..
            },
        ..
    }) = existing.as_ref()
        && source == binding
        && cached_metadata.observation_fingerprint() == metadata.observation_fingerprint()
    {
        return Ok(());
    }

    let version_id = Ulid::generate();
    let now = SystemTime::now();
    let next = CurrentVersionPointer::next_for(pointer.as_ref(), version_id)?;
    for effect in build_head_transition_effects(
        &HeadAliasContext::new(
            input.realm_id,
            input.group_id,
            input.node_id,
            &input.bucket,
            &entry.path,
        ),
        Some(next),
        None,
        Some(txn_id),
    )? {
        apply(context, effect).await?;
    }
    apply(
        context,
        write_blob_version_effect(
            &VersionKey::new(&input.bucket, &entry.path, version_id),
            &BlobVersion::reference(binding.clone(), metadata.clone(), now, input.user_id, now),
            Some(txn_id),
        )?,
    )
    .await?;

    // An offered bucket charges what it currently offers: the observation this
    // one replaces describes content the file no longer has, so its bytes are
    // released instead of staying charged.
    let live = existing.filter(|version| !version.is_deleted());
    let mut usage = UsageCounterUpdate::for_group(
        input.group_id,
        UsageDelta {
            objects: i128::from(u8::from(live.is_none())),
            referenced_bytes: i128::from(metadata.content_length)
                - i128::from(live.as_ref().map_or(0, observed_bytes)),
            ..Default::default()
        },
    );
    if !usage.is_noop() {
        run_usage_update(context, txn_id, &mut usage).await?;
    }
    Ok(())
}

/// Bytes a stored observation charges to its group.
fn observed_bytes(version: &BlobVersion) -> u64 {
    match &version.state {
        BlobVersionState::Reference {
            cached_metadata, ..
        } => cached_metadata.content_length,
        BlobVersionState::Materialized { .. } | BlobVersionState::Deleted => 0,
    }
}

/// Tombstones every observation whose file the walk did not see. A file the
/// owner deleted or moved must stop being offered instead of pointing at
/// something that is no longer there.
async fn sweep_observations(
    context: &DriverContext,
    scope: &ObservationScope,
    offered: &BTreeSet<String>,
) -> Result<usize, OfferedDirectoryError> {
    let prefix = BlobHeadKey::bucket_prefix(&scope.bucket)?;
    let mut start: Option<Key> = None;
    let mut removed = 0usize;
    loop {
        let (keys, next) = read_head_page(context, &prefix, start).await?;
        for key in &keys {
            if offered.contains(key) {
                continue;
            }
            if tombstone_observation(context, scope, key).await? {
                removed += 1;
            }
        }
        match next {
            Some(next) => start = Some(next),
            None => return Ok(removed),
        }
    }
}

/// One page of the bucket's object keys and the cursor of the next page.
async fn read_head_page(
    context: &DriverContext,
    prefix: &[u8],
    start: Option<Key>,
) -> Result<(Vec<String>, Option<Key>), OfferedDirectoryError> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            prefix: Some(prefix.into()),
            start: start.map(IterStart::After),
            limit: LIST_PAGE,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => {
            let mut keys = Vec::with_capacity(values.len());
            for (key, _) in values {
                keys.push(BlobHeadKey::from_bytes(key.as_ref())?.key);
            }
            Ok((keys, next_start_after))
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        _ => Err(StorageError::ReadError("unexpected event".to_string()).into()),
    }
}

/// Writes a delete marker over one observation. Answers whether it removed a
/// live object.
async fn tombstone_observation(
    context: &DriverContext,
    scope: &ObservationScope,
    key: &str,
) -> Result<bool, OfferedDirectoryError> {
    let txn_id = start_transaction(context).await?;
    match tombstone(context, scope, key, txn_id).await {
        Ok(true) => {
            commit(context, txn_id).await?;
            Ok(true)
        }
        Ok(false) => {
            abort(context, txn_id).await;
            Ok(false)
        }
        Err(error) => {
            abort(context, txn_id).await;
            Err(error)
        }
    }
}

async fn tombstone(
    context: &DriverContext,
    scope: &ObservationScope,
    key: &str,
    txn_id: TxnId,
) -> Result<bool, OfferedDirectoryError> {
    let pointer = read_pointer(context, &scope.bucket, key, txn_id).await?;
    let existing = match pointer.as_ref() {
        Some(pointer) => read_version(context, &scope.bucket, key, pointer, txn_id).await?,
        None => None,
    };
    let Some(live) = existing.filter(|version| !version.is_deleted()) else {
        return Ok(false);
    };

    let version_id = Ulid::generate();
    let now = SystemTime::now();
    let next = CurrentVersionPointer::next_for(pointer.as_ref(), version_id)?;
    for effect in build_head_transition_effects(
        &HeadAliasContext::new(
            scope.realm_id,
            scope.group_id,
            scope.node_id,
            &scope.bucket,
            key,
        ),
        Some(next),
        None,
        Some(txn_id),
    )? {
        apply(context, effect).await?;
    }
    apply(
        context,
        write_blob_version_effect(
            &VersionKey::new(&scope.bucket, key, version_id),
            &BlobVersion::deleted(now, scope.user_id),
            Some(txn_id),
        )?,
    )
    .await?;

    let mut usage = UsageCounterUpdate::for_group(
        scope.group_id,
        UsageDelta {
            objects: -1,
            referenced_bytes: -i128::from(observed_bytes(&live)),
            ..Default::default()
        },
    );
    if !usage.is_noop() {
        run_usage_update(context, txn_id, &mut usage).await?;
    }
    Ok(true)
}

async fn run_usage_update(
    context: &DriverContext,
    txn_id: TxnId,
    usage: &mut UsageCounterUpdate,
) -> Result<(), OfferedDirectoryError> {
    let mut effects = usage.start(txn_id);
    loop {
        let Some(effect) = effects.pop() else {
            return Err(StorageError::WriteError("empty usage effects".to_string()).into());
        };
        let Effect::Storage(effect) = effect else {
            return Err(StorageError::WriteError("unexpected usage effect".to_string()).into());
        };
        let event = context.storage_handle.send_storage_effect(effect).await;
        effects = match usage.step(event, txn_id)? {
            Some(effects) => effects,
            None => return Ok(()),
        };
    }
}

async fn read_offered(
    context: &DriverContext,
    bucket: &str,
    txn_id: Option<TxnId>,
) -> Result<Option<OfferedDirectory>, OfferedDirectoryError> {
    let value = read(
        context,
        OFFERED_DIRECTORY_KEYSPACE,
        bucket.as_bytes(),
        txn_id,
    )
    .await?;
    value
        .map(|value| OfferedDirectory::from_bytes(&value))
        .transpose()
        .map_err(OfferedDirectoryError::Conversion)
}

async fn read_bucket(
    context: &DriverContext,
    bucket: &str,
) -> Result<Option<BucketInfo>, OfferedDirectoryError> {
    let value = read(context, S3_BUCKET_KEYSPACE, bucket.as_bytes(), None).await?;
    value
        .map(|value| BucketInfo::from_bytes(&value))
        .transpose()
        .map_err(OfferedDirectoryError::Conversion)
}

async fn read_pointer(
    context: &DriverContext,
    bucket: &str,
    key: &str,
    txn_id: TxnId,
) -> Result<Option<CurrentVersionPointer>, OfferedDirectoryError> {
    let head = BlobHeadKey::new(bucket, key).to_bytes()?;
    let value = read(
        context,
        aruna_core::keyspaces::BLOB_HEAD_KEYSPACE,
        &head,
        Some(txn_id),
    )
    .await?;
    value
        .map(|value| CurrentVersionPointer::from_bytes(&value))
        .transpose()
        .map_err(OfferedDirectoryError::Conversion)
}

async fn read_version(
    context: &DriverContext,
    bucket: &str,
    key: &str,
    pointer: &CurrentVersionPointer,
    txn_id: TxnId,
) -> Result<Option<BlobVersion>, OfferedDirectoryError> {
    let version_key = VersionKey::new(bucket, key, pointer.version_id).to_bytes()?;
    let value = read(
        context,
        aruna_core::keyspaces::BLOB_VERSIONS_KEYSPACE,
        &version_key,
        Some(txn_id),
    )
    .await?;
    value
        .map(|value| BlobVersion::from_bytes(&value))
        .transpose()
        .map_err(OfferedDirectoryError::Conversion)
}

async fn read(
    context: &DriverContext,
    key_space: &str,
    key: &[u8],
    txn_id: Option<TxnId>,
) -> Result<Option<byteview::ByteView>, OfferedDirectoryError> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: key_space.to_string(),
            key: key.into(),
            txn_id,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        _ => Err(StorageError::ReadError("unexpected event".to_string()).into()),
    }
}

async fn apply(context: &DriverContext, effect: Effect) -> Result<(), OfferedDirectoryError> {
    let Effect::Storage(effect) = effect else {
        return Err(StorageError::WriteError("unexpected effect".to_string()).into());
    };
    match context.storage_handle.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        Event::Storage(_) => Ok(()),
        _ => Err(StorageError::WriteError("unexpected event".to_string()).into()),
    }
}

async fn start_transaction(context: &DriverContext) -> Result<TxnId, OfferedDirectoryError> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => Ok(txn_id),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        _ => Err(StorageError::WriteError("unexpected event".to_string()).into()),
    }
}

async fn abort(context: &DriverContext, txn_id: TxnId) {
    context
        .storage_handle
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await;
}

async fn commit(context: &DriverContext, txn_id: TxnId) -> Result<(), OfferedDirectoryError> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        _ => Err(StorageError::WriteError("unexpected event".to_string()).into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::get_object::{GetObjectInput, GetObjectOperation};
    use crate::staging::test_utils::setup_driver_context;
    use aruna_core::structs::{UsageCounters, usage_group_key};
    use futures_util::StreamExt;

    fn input(bucket: &str, root: &str) -> OfferDirectoryInput {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        OfferDirectoryInput {
            bucket: bucket.to_string(),
            root: root.to_string(),
            group_id: Ulid::from_bytes([2u8; 16]),
            realm_id,
            node_id: NodeId::from_bytes(&[3u8; 32]).expect("node id must parse"),
            user_id: UserId::new(Ulid::from_bytes([4u8; 16]), realm_id),
        }
    }

    // The offered file must be readable as an object without any byte of it
    // ever being copied into the node's own blob store.
    #[tokio::test]
    async fn serves_offered_file() {
        let fixture = setup_driver_context().await;
        let context = &fixture.driver_context;
        let root = tempfile::tempdir().expect("root must be created");
        std::fs::write(root.path().join("note.txt"), b"offered").expect("file must be written");
        let offer = input("offered", root.path().to_str().expect("utf-8 root"));

        let result = offer_directory(context, offer.clone())
            .await
            .expect("offer must succeed");
        assert_eq!(result.files, 1);

        let read = drive(
            GetObjectOperation::new(GetObjectInput {
                bucket: "offered".to_string(),
                key: "note.txt".to_string(),
                version_id: None,
                range: None,
                group_id: offer.group_id,
                user_identity: offer.user_id,
                node_id: offer.node_id,
            }),
            context,
        )
        .await
        .expect("get must run")
        .expect("get must finish")
        .expect("get must succeed");

        let mut body = Vec::new();
        let mut blob = read.blob.0;
        while let Some(chunk) = blob.next().await {
            body.extend_from_slice(&chunk.expect("chunk must read"));
        }
        assert_eq!(body, b"offered");
    }

    #[tokio::test]
    async fn refuses_writes() {
        let fixture = setup_driver_context().await;
        let context = &fixture.driver_context;
        let root = tempfile::tempdir().expect("root must be created");
        let offer = input("locked", root.path().to_str().expect("utf-8 root"));
        offer_directory(context, offer)
            .await
            .expect("offer must succeed");

        assert_eq!(
            guard_bucket_write(context, "locked").await,
            Err(OfferedDirectoryError::ReadOnly("locked".to_string()))
        );
        assert_eq!(guard_bucket_write(context, "other").await, Ok(()));
    }

    // A bucket that is not this offer's must keep its ordinary write semantics.
    #[tokio::test]
    async fn refuses_taken_bucket() {
        let fixture = setup_driver_context().await;
        let context = &fixture.driver_context;
        let root = tempfile::tempdir().expect("root must be created");
        let offer = input("taken", root.path().to_str().expect("utf-8 root"));
        crate::staging::test_utils::create_test_bucket(
            context,
            offer.group_id,
            offer.user_id,
            "taken",
        )
        .await;

        assert_eq!(
            offer_directory(context, offer).await,
            Err(OfferedDirectoryError::BucketTaken("taken".to_string()))
        );
    }

    // Re-offering an unchanged directory keeps the identities realm nodes may
    // already reference; a rewritten file mints a successor.
    #[tokio::test]
    async fn reoffer_tracks_changes() {
        let fixture = setup_driver_context().await;
        let context = &fixture.driver_context;
        let root = tempfile::tempdir().expect("root must be created");
        let file = root.path().join("data.bin");
        std::fs::write(&file, b"one").expect("file must be written");
        let offer = input("refresh", root.path().to_str().expect("utf-8 root"));

        offer_directory(context, offer.clone())
            .await
            .expect("first offer must succeed");
        let first = current_version(context, "refresh", "data.bin").await;
        offer_directory(context, offer.clone())
            .await
            .expect("second offer must succeed");
        assert_eq!(current_version(context, "refresh", "data.bin").await, first);

        std::fs::write(&file, b"one-changed").expect("file must be rewritten");
        offer_directory(context, offer)
            .await
            .expect("third offer must succeed");
        assert_ne!(current_version(context, "refresh", "data.bin").await, first);
    }

    #[test]
    fn refuses_over_cap() {
        // The cap is decided while walking, before a single write.
        assert!(!over_cap(MAX_OFFERED_FILES - 1, 1));
        assert!(over_cap(MAX_OFFERED_FILES, 1));
    }

    // The group is charged for what the directory currently offers, so a
    // rewritten file replaces its predecessor's bytes instead of adding to them.
    #[tokio::test]
    async fn charges_current_inventory() {
        let fixture = setup_driver_context().await;
        let context = &fixture.driver_context;
        let root = tempfile::tempdir().expect("root must be created");
        let file = root.path().join("data.bin");
        std::fs::write(&file, b"one").expect("file must be written");
        let offer = input("usage", root.path().to_str().expect("utf-8 root"));

        offer_directory(context, offer.clone())
            .await
            .expect("first offer must succeed");
        let usage = group_usage(context, offer.group_id).await;
        assert_eq!((usage.objects, usage.referenced_bytes), (1, 3));

        std::fs::write(&file, b"one-changed").expect("file must be rewritten");
        offer_directory(context, offer.clone())
            .await
            .expect("second offer must succeed");
        let usage = group_usage(context, offer.group_id).await;
        assert_eq!((usage.objects, usage.referenced_bytes), (1, 11));
    }

    // A file the owner removed stops being offered: the object becomes a delete
    // marker and its bytes are released.
    #[tokio::test]
    async fn tombstones_vanished_files() {
        let fixture = setup_driver_context().await;
        let context = &fixture.driver_context;
        let root = tempfile::tempdir().expect("root must be created");
        std::fs::write(root.path().join("keep.txt"), b"keep").expect("file must be written");
        std::fs::write(root.path().join("gone.txt"), b"gone").expect("file must be written");
        let offer = input("sweep", root.path().to_str().expect("utf-8 root"));

        let first = offer_directory(context, offer.clone())
            .await
            .expect("first offer must succeed");
        assert_eq!((first.files, first.removed), (2, 0));

        std::fs::remove_file(root.path().join("gone.txt")).expect("file must be removed");
        let second = offer_directory(context, offer.clone())
            .await
            .expect("second offer must succeed");
        assert_eq!((second.files, second.removed), (1, 1));

        let usage = group_usage(context, offer.group_id).await;
        assert_eq!((usage.objects, usage.referenced_bytes), (1, 4));
        assert!(
            head_version(context, "sweep", "gone.txt")
                .await
                .is_deleted()
        );
    }

    async fn group_usage(context: &DriverContext, group_id: GroupId) -> UsageCounters {
        read(
            context,
            aruna_core::keyspaces::USAGE_STATS_KEYSPACE,
            &usage_group_key(group_id),
            None,
        )
        .await
        .expect("usage counters must read")
        .map(|value| UsageCounters::from_bytes(value.as_ref()).expect("counters must decode"))
        .unwrap_or_default()
    }

    async fn head_version(context: &DriverContext, bucket: &str, key: &str) -> BlobVersion {
        let txn_id = start_transaction(context).await.expect("txn must start");
        let pointer = read_pointer(context, bucket, key, txn_id)
            .await
            .expect("pointer must read")
            .expect("pointer must exist");
        let version = read_version(context, bucket, key, &pointer, txn_id)
            .await
            .expect("version must read")
            .expect("version must exist");
        commit(context, txn_id).await.expect("txn must commit");
        version
    }

    async fn current_version(context: &DriverContext, bucket: &str, key: &str) -> Ulid {
        let txn_id = start_transaction(context).await.expect("txn must start");
        let pointer = read_pointer(context, bucket, key, txn_id)
            .await
            .expect("pointer must read")
            .expect("pointer must exist");
        commit(context, txn_id).await.expect("txn must commit");
        pointer.version_id
    }
}
