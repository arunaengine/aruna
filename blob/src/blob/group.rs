//! Builds operators for tenant group backends and tracks holds so idle ones can be released.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{BlobHandler, NodeBackend};
use crate::opendal::build_group_service;
use crate::s3::{self, check_keys};
use aruna_core::effects::{BlobEffect, StorageEffect};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::STORAGE_BACKEND_KEYSPACE;
use aruna_core::structs::storage::blob::{Backend, BackendConfig, BackendRef};
use aruna_core::structs::storage::group_backend::{
    BUCKET_DELETE, BUCKET_PREFIX, BUCKET_STATE, BUCKET_TEMPLATE, GroupStorage, GroupStorageSecret,
    LAYOUT_BUCKET, PROBE_PREFIX,
};
use aruna_core::types::Key;
use opendal::ErrorKind;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, Mutex};
use ulid::Ulid;

/// Uniform tenant write chunk. azblob and azdls declare no minimum, so without
/// one they emit a block per stream chunk against a 50,000-block ceiling.
pub(super) const GROUP_WRITE_CHUNK: usize = 8 * 1024 * 1024;

/// Synthesizes the registry entry a stored `BackendRef::Group` resolves to.
/// `multipart_bucket` names the tenant's container or default bucket, which also keeps the
/// backend out of the minted-bucket stats accounting.
pub(super) fn group_entry(
    record: &GroupStorage,
    secret: &GroupStorageSecret,
    timeouts: aruna_core::structs::storage::blob::BlobTimeoutConfig,
) -> Result<NodeBackend, BlobError> {
    let mut service_config: HashMap<String, String> = record.public_config.clone();
    // The default bucket stays for bucket selection; `operator_for` drops it.
    service_config.retain(|key, _| {
        !key.starts_with(BUCKET_STATE)
            && !key.starts_with(LAYOUT_BUCKET)
            && ![BUCKET_PREFIX, BUCKET_TEMPLATE, BUCKET_DELETE].contains(&key.as_str())
    });
    service_config.extend(secret.secret_config.clone());
    let root = service_config.remove("root").unwrap_or_default();
    // Without a container each path names its bucket; Aruna's layout goes to the default
    // bucket and parts to the bucket `parts_bucket` names.
    let container = match record.container() {
        Some(container) => Some(container.to_string()),
        None if record.splits() => None,
        None => {
            return Err(BlobError::OperatorCreationFailed(format!(
                "group backend {} is missing its container",
                record.backend_id
            )));
        }
    };
    // Bucket selection reads this key for every kind; opendal drops it where the
    // service does not know it.
    if let Some(container) = &container {
        service_config.insert("bucket".to_string(), container.clone());
    }
    let multipart_bucket = container.or_else(|| record.parts_bucket().map(str::to_string));

    // Invariant: the kind stays `Backend::Group`; `Backend::S3` would build
    // through the unguarded operator and bypass the egress guard.
    Ok(NodeBackend::new(
        BackendConfig {
            backend_type: Backend::Group(record.kind),
            root,
            service_config,
            bucket_prefix: None,
            max_bucket_size: None,
            multipart_bucket,
            timeouts,
        },
        None,
    ))
}

/// Group ids an effect can reach. Hidden and job blobs never route to a tenant
/// backend, but their stamped refs are read here all the same.
fn group_ids(effect: &BlobEffect) -> Vec<Ulid> {
    fn push(ids: &mut Vec<Ulid>, backend: &BackendRef) {
        if let BackendRef::Group(backend_id) = backend {
            ids.push(*backend_id);
        }
    }

    let mut ids = Vec::new();
    match effect {
        BlobEffect::Abe(effect) => {
            if let aruna_core::structs::storage::abe::AbeEffect::Write { resolved, .. } = effect.as_ref() {
                push(&mut ids, &resolved.backend);
            }
        }
        BlobEffect::Write { resolved, .. }
        | BlobEffect::WritePart { resolved, .. }
        | BlobEffect::OpenUpload { resolved, .. }
        | BlobEffect::HandleReplication { resolved, .. } => push(&mut ids, &resolved.backend),
        BlobEffect::CompleteUpload { backend_upload, .. }
        | BlobEffect::AbortUpload { backend_upload } => {
            push(&mut ids, &backend_upload.location.backend);
        }
        BlobEffect::Compose {
            resolved, parts, ..
        } => {
            push(&mut ids, &resolved.backend);
            for part in parts {
                push(&mut ids, &part.backend);
            }
        }
        BlobEffect::Read { location }
        | BlobEffect::ReadRange { location, .. }
        | BlobEffect::Delete { location }
        | BlobEffect::ReadHiddenRange { location, .. }
        | BlobEffect::Replicate { location, .. }
        | BlobEffect::ServeRead { location, .. } => push(&mut ids, &location.backend),
        BlobEffect::ReleaseReservation { .. } => {}
        BlobEffect::DeleteHidden { key } => push(&mut ids, &key.backend),
        // An observation is served straight from the owner's filesystem, so it
        // claims no tenant backend at all.
        BlobEffect::ServeSourceRead { .. }
        | BlobEffect::SpoolHidden { .. }
        | BlobEffect::ListHidden { .. }
        | BlobEffect::OpenConnection { .. }
        | BlobEffect::SendMessage { .. }
        | BlobEffect::ReadMessage { .. }
        | BlobEffect::CloseConnection { .. }
        | BlobEffect::ReceiveRead { .. }
        | BlobEffect::CheckGroupBackend { .. }
        // Key effects touch no backend; the read that uses a lease claims its own.
        | BlobEffect::GenerateBucketKey
        | BlobEffect::SealHolderCopies { .. }
        | BlobEffect::SealUnlocked { .. }
        | BlobEffect::PrepareKey { .. }
        | BlobEffect::ActivateKey { .. }
        | BlobEffect::DiscardKey { .. }
        | BlobEffect::ReadKeyStatus { .. }
        | BlobEffect::ExtendKey { .. }
        | BlobEffect::LockKey { .. }
        | BlobEffect::AdmitRead { .. } => {}
        BlobEffect::ReadUnlockedKey { .. } => {}
        BlobEffect::RewriteCopy { source, target, .. } => {
            push(&mut ids, &source.backend);
            push(&mut ids, &target.backend);
        }
        BlobEffect::WritePiece { resolved, .. } => push(&mut ids, &resolved.backend),
        BlobEffect::ComposePieces {
            resolved, parts, ..
        } => {
            push(&mut ids, &resolved.backend);
            for part in parts {
                push(&mut ids, &part.location.backend);
            }
        }
        BlobEffect::HashArchive { location, .. } => push(&mut ids, &location.backend),
        BlobEffect::ReadSealed { location, .. } => push(&mut ids, &location.backend),
        BlobEffect::ServeSealedRead { location, .. } => push(&mut ids, &location.backend),
        BlobEffect::ReserveCompose { .. } => {}
        BlobEffect::ReplicateLeased { location, .. } => push(&mut ids, &location.backend),
        BlobEffect::ListExternal { backend, .. } => push(&mut ids, backend),
        BlobEffect::HashExternal { resolved, .. } => push(&mut ids, &resolved.backend),
        BlobEffect::WritePlain { resolved, .. } => push(&mut ids, &resolved.backend),
        BlobEffect::CopyPlain {
            resolved, source, ..
        } => {
            push(&mut ids, &resolved.backend);
            push(&mut ids, &source.backend);
        }
        BlobEffect::StatPlain { backend, .. } | BlobEffect::DropReservation { backend, .. } => {
            push(&mut ids, backend)
        }
        BlobEffect::StatExternal { resolved, .. } => push(&mut ids, &resolved.backend),
        BlobEffect::SettleCopy { upload, .. } => {
            if let Some((location, _)) = upload {
                push(&mut ids, &location.backend);
            }
        }
        BlobEffect::StatCopy { location } | BlobEffect::DeletePlain { location, .. } => {
            push(&mut ids, &location.backend)
        }
        BlobEffect::EnsureBucket { backend, .. }
        | BlobEffect::HeadBucket { backend, .. }
        | BlobEffect::DeleteEmptyBucket { backend, .. } => push(&mut ids, backend),
    }
    ids
}

/// Tenant backend use: hold count, claim flag and hold generation.
/// Holds and claims are mutually exclusive, which keeps credentials alive
/// while anything can still need them; entries are never dropped.
#[derive(Debug, Default)]
pub(super) struct GroupBackendUse {
    held: usize,
    claimed: bool,
    generation: u64,
}

/// Keeps every tenant backend an operation named off the removable list. The
/// driver holds it for the whole operation, so it also covers the metadata
/// transaction that commits after the effect returns and that commit's rollback.
pub struct GroupHold {
    counts: Arc<Mutex<HashMap<Ulid, GroupBackendUse>>>,
    ids: Vec<Ulid>,
}

impl Drop for GroupHold {
    fn drop(&mut self) {
        let Ok(mut counts) = self.counts.lock() else {
            return;
        };
        for backend_id in &self.ids {
            if let Entry::Occupied(mut entry) = counts.entry(*backend_id) {
                let usage = entry.get_mut();
                usage.held = usage.held.saturating_sub(1);
            }
        }
    }
}

/// Removal's exclusive reservation on one tenant backend.
pub struct BackendClaim {
    counts: Arc<Mutex<HashMap<Ulid, GroupBackendUse>>>,
    backend_id: Ulid,
}

impl Drop for BackendClaim {
    fn drop(&mut self) {
        let Ok(mut counts) = self.counts.lock() else {
            return;
        };
        if let Entry::Occupied(mut entry) = counts.entry(self.backend_id) {
            entry.get_mut().claimed = false;
        }
    }
}

impl BlobHandler {
    /// Taken before the credentials are read, so a claimed backend is refused
    /// before any bytes exist and a held one cannot be claimed.
    pub(super) fn hold_backends(
        &self,
        effect: &BlobEffect,
    ) -> Result<Option<GroupHold>, BlobError> {
        let ids = group_ids(effect);
        if ids.is_empty() {
            return Ok(None);
        }
        let Ok(mut counts) = self.group_effects.lock() else {
            return Err(BlobError::UnknownBackend(
                "tenant backend registry is unusable".to_string(),
            ));
        };
        if let Some(claimed) = ids
            .iter()
            .find(|id| counts.get(*id).is_some_and(|usage| usage.claimed))
        {
            return Err(BlobError::UnknownBackend(format!(
                "group backend {claimed} is being removed"
            )));
        }
        for backend_id in &ids {
            let usage = counts.entry(*backend_id).or_default();
            usage.held = usage.held.saturating_add(1);
            usage.generation = usage.generation.wrapping_add(1);
        }
        drop(counts);
        Ok(Some(GroupHold {
            counts: self.group_effects.clone(),
            ids,
        }))
    }

    /// The backend's hold generation while nothing holds or claims it. Removal
    /// reads it before its scan and hands it back to `claim_backend`.
    pub(super) fn idle_generation(&self, backend_id: Ulid) -> Option<u64> {
        let counts = self.group_effects.lock().ok()?;
        match counts.get(&backend_id) {
            None => Some(0),
            Some(usage) if usage.held == 0 && !usage.claimed => Some(usage.generation),
            Some(_) => None,
        }
    }

    /// Granted only when the backend stayed idle since `generation`; it
    /// refuses every hold while alive, keeping removal's scan result true
    /// until the record is gone.
    pub(super) fn claim_backend(&self, backend_id: Ulid, generation: u64) -> Option<BackendClaim> {
        let mut counts = self.group_effects.lock().ok()?;
        let usage = counts.entry(backend_id).or_default();
        if usage.held > 0 || usage.claimed || usage.generation != generation {
            return None;
        }
        usage.claimed = true;
        drop(counts);
        Some(BackendClaim {
            counts: self.group_effects.clone(),
            backend_id,
        })
    }

    /// Loads every tenant backend the effect names into a handler of its own.
    /// The snapshot never enters shared state, so a concurrent replacement or
    /// deletion cannot swap the backend an executing effect resolves.
    pub(super) async fn with_group_backends(&self, effect: &BlobEffect) -> Result<Self, BlobError> {
        let mut groups = HashMap::new();
        for backend_id in group_ids(effect) {
            if groups.contains_key(&backend_id) {
                continue;
            }
            groups.insert(
                backend_id,
                Arc::new(self.read_group_backend(backend_id).await?),
            );
        }
        if groups.is_empty() {
            return Ok(self.clone());
        }
        Ok(Self {
            registry: self.registry.with_groups(groups),
            ..self.clone()
        })
    }

    /// Both records are read from one snapshot: a replacement committing between
    /// separate reads would pair the old endpoint with the new credentials.
    async fn read_group_backend(&self, backend_id: Ulid) -> Result<NodeBackend, BlobError> {
        let unreadable =
            || BlobError::ReadError(format!("group backend {backend_id} could not be read"));
        let snapshot = self
            .storage
            .start_transaction(true)
            .await
            .map_err(|_| unreadable())?;
        let txn_id = snapshot.id();
        let key: Key = backend_id.to_bytes().to_vec().into();
        let record = self
            .storage
            .send_storage_effect(StorageEffect::Read {
                key_space: STORAGE_BACKEND_KEYSPACE.to_string(),
                key,
                txn_id,
            })
            .await;
        let entry = GroupStorageSecret::vault_entry(backend_id);
        let secret = self
            .storage
            .send_storage_effect(StorageEffect::VaultRead { entry, txn_id })
            .await;
        // Dropping the owner aborts the read snapshot.
        drop(snapshot);
        let (
            Event::Storage(StorageEvent::ReadResult { value: record, .. }),
            Event::Storage(StorageEvent::VaultResult { secret, .. }),
        ) = (record, secret)
        else {
            return Err(unreadable());
        };
        let (Some(record), Some(secret)) = (record, secret) else {
            return Err(BlobError::UnknownBackend(format!(
                "group backend {backend_id} is not registered"
            )));
        };
        let record =
            GroupStorage::from_bytes(record.as_ref()).map_err(BlobError::ConversionError)?;
        let secret =
            GroupStorageSecret::from_bytes(secret.expose()).map_err(BlobError::ConversionError)?;
        group_entry(&record, &secret, self.registry.timeouts())
    }

    /// Create-time proof that the credentials work and the endpoint is
    /// reachable through the guard: a sentinel object is written and removed.
    pub(super) async fn check_group_backend(
        &self,
        record: GroupStorage,
        secret: GroupStorageSecret,
    ) -> BlobEvent {
        let entry = match group_entry(&record, &secret, self.registry.timeouts()) {
            Ok(entry) => entry,
            Err(error) => return BlobEvent::Error(error),
        };
        // Without a bucket only the keys are checked; each rule's bucket is checked at save.
        if record.splits() {
            return match check_keys(&entry.config.service_config, &self.egress).await {
                Ok(()) => BlobEvent::GroupBackendChecked,
                Err(error) => BlobEvent::Error(error),
            };
        }
        let mut config = entry.config.service_config.clone();
        config.insert("root".to_string(), entry.config.root.clone());
        let operator = match build_group_service(record.kind, config, &self.egress) {
            Ok(operator) => operator,
            Err(error) => return BlobEvent::Error(BlobError::OperatorCreationFailed(error)),
        };

        let (label, name) = container_label(&record);
        match probe(&operator, record.backend_id, label, name).await {
            Ok(()) => BlobEvent::GroupBackendChecked,
            Err(error) => BlobEvent::Error(error),
        }
    }

    /// Finds or creates a rule's bucket on a storage without a bucket, then proves that
    /// Aruna may write and delete there.
    pub(super) async fn ensure_bucket(&self, backend: &BackendRef, bucket: &str) -> BlobEvent {
        let (backend_id, config) = match self.split_config(backend) {
            Ok(found) => found,
            Err(error) => return BlobEvent::Error(error),
        };
        let created = match s3::ensure_bucket(&config.service_config, bucket, &self.egress).await {
            Ok(created) => created,
            Err(error) => return BlobEvent::Error(error),
        };
        let operator = match self
            .registry
            .operator_for(backend, &config.root, bucket, &self.egress)
        {
            Ok(operator) => operator,
            Err(error) => return BlobEvent::Error(error),
        };
        match probe(&operator, backend_id, "bucket", bucket).await {
            Ok(()) => BlobEvent::BucketReady { created },
            Err(error) => BlobEvent::Error(error),
        }
    }

    /// Whether `bucket` exists on a storage without a bucket and, if so, whether Aruna may
    /// write and delete there; nothing is created.
    pub(super) async fn head_bucket(&self, backend: &BackendRef, bucket: &str) -> BlobEvent {
        let (backend_id, config) = match self.split_config(backend) {
            Ok(found) => found,
            Err(error) => return BlobEvent::Error(error),
        };
        match s3::bucket_exists(&config.service_config, bucket, &self.egress).await {
            Ok(true) => {}
            Ok(false) => return bucket_answer(None),
            Err(error) => return BlobEvent::Error(error),
        }
        let operator = match self
            .registry
            .operator_for(backend, &config.root, bucket, &self.egress)
        {
            Ok(operator) => operator,
            Err(error) => return BlobEvent::Error(error),
        };
        bucket_answer(Some(probe(&operator, backend_id, "bucket", bucket).await))
    }

    /// Deletes `bucket` on a storage without a bucket; a bucket with objects stays.
    pub(super) async fn delete_bucket(&self, backend: &BackendRef, bucket: &str) -> BlobEvent {
        let (backend_id, config) = match self.split_config(backend) {
            Ok(found) => found,
            Err(error) => return BlobEvent::Error(error),
        };
        match s3::delete_empty(&config.service_config, bucket, &self.egress).await {
            Ok(true) => {
                tracing::info!(%backend_id, bucket, "deleted empty provider bucket");
                BlobEvent::BucketDeleted { deleted: true }
            }
            Ok(false) => {
                tracing::info!(%backend_id, bucket, "kept provider bucket that holds objects");
                BlobEvent::BucketDeleted { deleted: false }
            }
            Err(error) => {
                tracing::warn!(%backend_id, bucket, error = %error, "provider bucket not deleted");
                BlobEvent::Error(error)
            }
        }
    }

    fn split_config(&self, backend: &BackendRef) -> Result<(Ulid, BackendConfig), BlobError> {
        let (BackendRef::Group(backend_id), true) = (backend, self.registry.splits(backend)) else {
            let message = "only a storage without a fixed bucket takes rule buckets";
            return Err(BlobError::OperatorCreationFailed(message.to_string()));
        };
        Ok((*backend_id, self.registry.config_for(backend)?))
    }
}

/// The answer of a bucket check: no probe for a missing bucket, and a refused probe write or
/// delete makes an existing bucket not writable.
fn bucket_answer(probed: Option<Result<(), BlobError>>) -> BlobEvent {
    BlobEvent::BucketExists {
        exists: probed.is_some(),
        writable: probed.map(|result| result.is_ok()),
    }
}

/// Writes and removes a sentinel object. Credentials that cannot delete break object
/// deletion, multipart abort and cleanup later, and leak the probe object now.
async fn probe(
    operator: &opendal::Operator,
    backend_id: Ulid,
    label: &str,
    name: &str,
) -> Result<(), BlobError> {
    let probe = format!("{PROBE_PREFIX}/{}", Ulid::generate());
    if let Err(error) = operator.write(&probe, b"aruna".to_vec()).await {
        tracing::warn!(%backend_id, kind = ?error.kind(), "group backend test write failed");
        return Err(BlobError::WriteError(probe_message(
            label, name, &error, false,
        )));
    }
    if let Err(error) = operator.delete(&probe).await {
        tracing::warn!(%backend_id, kind = ?error.kind(), "group backend test delete failed");
        return Err(BlobError::DeleteError(probe_message(
            label, name, &error, true,
        )));
    }
    Ok(())
}

/// The config key and name of a storage's fixed container.
fn container_label(record: &GroupStorage) -> (&'static str, &str) {
    ["bucket", "container", "filesystem"]
        .into_iter()
        .find_map(|key| {
            record
                .public_config
                .get(key)
                .map(|name| (key, name.as_str()))
        })
        .unwrap_or(("bucket", ""))
}

/// A short user-facing reason for a failed probe; the full error goes to the node log.
fn probe_message(label: &str, name: &str, error: &opendal::Error, delete: bool) -> String {
    match (error.kind(), delete) {
        (ErrorKind::PermissionDenied, false) => format!(
            "The storage refused a test write to {label} `{name}`: access denied. Check that the \
             key may write and delete objects there."
        ),
        (ErrorKind::PermissionDenied, true) => format!(
            "The storage refused to delete a test object in {label} `{name}`: the key may write \
             but not delete objects; Aruna needs both."
        ),
        (ErrorKind::NotFound, false) => {
            format!("The {label} `{name}` does not exist at this endpoint.")
        }
        (ErrorKind::ConfigInvalid, _) => {
            "The backend settings are invalid. Check endpoint, bucket and region.".to_string()
        }
        (_, false) => "The storage could not be reached or refused the test write. The node log \
                       has details."
            .to_string(),
        (_, true) => {
            "The storage could not delete the test object. The node log has details.".to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{bucket_answer, container_label, group_entry, group_ids, probe, probe_message};
    use aruna_core::effects::BlobEffect;
    use aruna_core::errors::BlobError;
    use aruna_core::events::BlobEvent;
    use aruna_core::structs::storage::blob::{
        Backend, BackendRef, BlobTimeoutConfig, ResolvedBackend,
    };
    use aruna_core::structs::storage::group_backend::{
        GroupBackendKind, GroupStorage, GroupStorageSecret,
    };
    use std::collections::HashMap;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn record(kind: GroupBackendKind, public: &[(&str, &str)]) -> GroupStorage {
        GroupStorage {
            backend_id: Ulid::from_bytes([1u8; 16]),
            group_id: Ulid::from_bytes([2u8; 16]),
            name: "tenant".to_string(),
            kind,
            public_config: public
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: aruna_core::UserId::default(),
            disabled: false,
            cleanup: aruna_core::structs::storage::cleanup::CleanupStrategy::Retain,
        }
    }

    fn secret() -> GroupStorageSecret {
        GroupStorageSecret {
            backend_id: Ulid::from_bytes([1u8; 16]),
            secret_config: HashMap::from([("account_key".to_string(), "canary-a3e6".to_string())]),
            updated_at: SystemTime::UNIX_EPOCH,
        }
    }

    #[test]
    fn probe_hides_details() {
        let record = record(GroupBackendKind::Azblob, &[("container", "data")]);
        let raw = "PUT https://acct.blob.core.windows.net/data <Error>AuthorizationFailure</Error>";
        let denied = opendal::Error::new(opendal::ErrorKind::PermissionDenied, raw);
        let (label, name) = container_label(&record);

        let write = probe_message(label, name, &denied, false);
        let delete = probe_message(label, name, &denied, true);

        assert!(write.contains("container `data`: access denied"), "{write}");
        assert!(delete.contains("not delete objects"), "{delete}");
        assert!(!write.contains("Error") && !delete.contains("https://"));
    }

    #[test]
    fn entry_pins_container() {
        // Azure names its container differently, but bucket selection is uniform.
        let entry = group_entry(
            &record(
                GroupBackendKind::Azblob,
                &[
                    ("container", "data"),
                    ("endpoint", "https://acct.blob.core.windows.net"),
                    ("root", "tenant/"),
                ],
            ),
            &secret(),
            BlobTimeoutConfig::default(),
        )
        .unwrap();

        assert_eq!(
            entry.config.backend_type,
            Backend::Group(GroupBackendKind::Azblob)
        );
        assert_eq!(entry.config.root, "tenant/");
        assert_eq!(entry.config.multipart_bucket.as_deref(), Some("data"));
        assert_eq!(entry.config.max_bucket_size, None);
        assert_eq!(
            entry
                .config
                .service_config
                .get("bucket")
                .map(String::as_str),
            Some("data")
        );
        assert_eq!(
            entry
                .config
                .service_config
                .get("account_key")
                .map(String::as_str),
            Some("canary-a3e6")
        );
        let rendered = format!("{entry:?}");
        assert!(!rendered.contains("canary"), "{rendered}");
    }

    #[test]
    fn entry_without_bucket() {
        // Aruna's own keys never reach the service config.
        let mut storage = record(GroupBackendKind::S3, &[("endpoint", "https://s3.lab")]);
        let entry = group_entry(&storage, &secret(), BlobTimeoutConfig::default()).unwrap();
        assert!(!entry.config.service_config.contains_key("bucket"));
        assert_eq!(entry.config.multipart_bucket, None);

        // Without a default rule the parts wait in a checked rule bucket.
        storage
            .public_config
            .insert("bucket:lab-raw".to_string(), "created".to_string());
        let entry = group_entry(&storage, &secret(), BlobTimeoutConfig::default()).unwrap();
        assert_eq!(entry.config.multipart_bucket.as_deref(), Some("lab-raw"));
        storage
            .public_config
            .insert("default_bucket".to_string(), "lab-default".to_string());
        storage
            .public_config
            .insert("layout:lab-old".to_string(), "true".to_string());
        let entry = group_entry(&storage, &secret(), BlobTimeoutConfig::default()).unwrap();
        assert_eq!(
            entry.config.multipart_bucket.as_deref(),
            Some("lab-default")
        );
        let keys = entry.config.service_config.keys().collect::<Vec<_>>();
        assert!(
            keys.iter()
                .all(|key| !key.contains("bucket") || *key == "default_bucket"),
            "{keys:?}"
        );
        // Other kinds still need their container.
        let azure = record(GroupBackendKind::Azblob, &[("endpoint", "https://az")]);
        assert!(group_entry(&azure, &secret(), BlobTimeoutConfig::default()).is_err());
    }

    #[test]
    fn collects_group_refs() {
        let backend_id = Ulid::from_bytes([7u8; 16]);
        let effect = BlobEffect::HandleReplication {
            replication_id: None,
            stream_id: Ulid::from_bytes([8u8; 16]),
            resolved: ResolvedBackend::new(BackendRef::Group(backend_id), None),
            keep_alive: false,
            object: None,
        };

        assert_eq!(group_ids(&effect), vec![backend_id]);
        // A copy upload is settled on the storage it was opened on.
        let location = aruna_core::structs::storage::blob::BackendLocation {
            backend: BackendRef::Group(backend_id),
            storage_class: None,
            root: String::new(),
            storage_bucket: String::new(),
            backend_path: "raw/a".to_string(),
            ulid: Ulid::from_bytes([9u8; 16]),
            format: Default::default(),
            created_by: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 1,
            hashes: Default::default(),
        };
        let settle = BlobEffect::SettleCopy {
            attempt: Ulid::from_bytes([8u8; 16]),
            upload: Some((Box::new(location), "upload".to_string())),
        };
        assert_eq!(group_ids(&settle), vec![backend_id]);
        assert!(
            group_ids(&BlobEffect::ListHidden {
                namespace: None,
                cursor: None,
            })
            .is_empty()
        );
    }

    #[tokio::test]
    async fn checks_bucket_access() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let operator = opendal::Operator::new(opendal::services::Fs::default().root(root)).unwrap();
        let probed = probe(&operator, Ulid::nil(), "bucket", "team-a-raw").await;
        let answer = |exists, writable| BlobEvent::BucketExists { exists, writable };
        assert_eq!(bucket_answer(Some(probed)), answer(true, Some(true)));
        // The probe leaves no file behind.
        let left = files_below(dir.path());
        assert!(left.is_empty(), "{left:?}");
        let refused = Err(BlobError::WriteError("denied".to_string()));
        assert_eq!(bucket_answer(Some(refused)), answer(true, Some(false)));
        assert_eq!(bucket_answer(None), answer(false, None));
    }

    fn files_below(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut files = Vec::new();
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            match path.is_dir() {
                true => files.extend(files_below(&path)),
                false => files.push(path),
            }
        }
        files
    }
}
