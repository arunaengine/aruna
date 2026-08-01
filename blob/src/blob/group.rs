use super::{BlobHandler, NodeBackend};
use crate::opendal::build_group_service;
use aruna_core::effects::{BlobEffect, StorageEffect};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    GROUP_STORAGE_BACKEND_KEYSPACE, GROUP_STORAGE_BACKEND_SECRET_KEYSPACE,
};
use aruna_core::structs::{
    Backend, BackendConfig, BackendRef, GroupBackendKind, GroupStorageBackend,
    GroupStorageBackendSecret,
};
use aruna_core::types::Key;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use ulid::Ulid;

/// Uniform tenant write chunk. azblob and azdls declare no minimum, so without
/// one they emit a block per stream chunk against a 50,000-block ceiling.
pub(super) const GROUP_WRITE_CHUNK: usize = 8 * 1024 * 1024;

const PROBE_KEY: &str = "_aruna_backend_probe";

/// The tenant's own container is the only one; nothing is ever minted.
fn container_key(kind: GroupBackendKind) -> &'static str {
    match kind {
        GroupBackendKind::S3 | GroupBackendKind::Gcs | GroupBackendKind::B2 => "bucket",
        GroupBackendKind::Azblob => "container",
        GroupBackendKind::Azdls => "filesystem",
    }
}

/// Synthesizes the registry entry a stored `BackendRef::Group` resolves to.
/// `multipart_bucket` names the tenant's own container, which also keeps the
/// backend out of the minted-bucket stats accounting.
pub(super) fn group_entry(
    record: &GroupStorageBackend,
    secret: &GroupStorageBackendSecret,
    timeouts: aruna_core::structs::BlobTimeoutConfig,
) -> Result<NodeBackend, BlobError> {
    let mut service_config: HashMap<String, String> = record.public_config.clone();
    service_config.extend(secret.secret_config.clone());
    let root = service_config.remove("root").unwrap_or_default();
    let container = service_config
        .get(container_key(record.kind))
        .cloned()
        .ok_or_else(|| {
            BlobError::OperatorCreationFailed(format!(
                "group backend {} is missing its container",
                record.backend_id
            ))
        })?;
    // Bucket selection reads this key for every kind; opendal drops it where the
    // service does not know it.
    service_config.insert("bucket".to_string(), container.clone());

    // Invariant: the kind stays `Backend::Group` here. `Backend::S3` would build
    // through the unguarded operator path and the raw AWS SDK client, both of
    // which bypass the egress guard a tenant endpoint depends on.
    Ok(NodeBackend::new(
        BackendConfig {
            backend_type: Backend::Group(record.kind),
            root,
            service_config,
            bucket_prefix: None,
            max_bucket_size: None,
            multipart_bucket: Some(container),
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
        BlobEffect::Write { resolved, .. }
        | BlobEffect::WritePart { resolved, .. }
        | BlobEffect::HandleReplication { resolved, .. } => push(&mut ids, &resolved.backend),
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
        BlobEffect::DeleteHidden { key } => push(&mut ids, &key.backend),
        BlobEffect::SpoolHidden { .. }
        | BlobEffect::ListHidden { .. }
        | BlobEffect::OpenConnection { .. }
        | BlobEffect::SendMessage { .. }
        | BlobEffect::ReadMessage { .. }
        | BlobEffect::CloseConnection { .. }
        | BlobEffect::ReceiveRead { .. }
        | BlobEffect::CheckGroupBackend { .. } => {}
    }
    ids
}

/// How a tenant backend is being used: how many holds it carries, and whether
/// removal has claimed it. The two are mutually exclusive, which is what keeps
/// the credentials alive for as long as anything can still need them.
#[derive(Debug, Default)]
pub(super) struct GroupBackendUse {
    held: usize,
    claimed: bool,
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
                if usage.held == 0 && !usage.claimed {
                    entry.remove();
                }
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
            if entry.get().held == 0 {
                entry.remove();
            }
        }
    }
}

impl BlobHandler {
    /// Taken before the credentials are read, so a claimed backend is refused
    /// before any bytes exist and a held one cannot be claimed.
    pub(super) fn hold_backends(&self, effect: &BlobEffect) -> Result<Option<GroupHold>, BlobError> {
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
        }
        drop(counts);
        Ok(Some(GroupHold {
            counts: self.group_effects.clone(),
            ids,
        }))
    }

    /// Refused while any operation holds the backend, and refusing every new
    /// hold while it lives. Removal takes it before it reads anything, so the
    /// durable state it then observes can no longer change.
    pub(super) fn claim_backend(&self, backend_id: Ulid) -> Option<BackendClaim> {
        let mut counts = self.group_effects.lock().ok()?;
        if counts
            .get(&backend_id)
            .is_some_and(|usage| usage.held > 0 || usage.claimed)
        {
            return None;
        }
        counts.entry(backend_id).or_default().claimed = true;
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
        let key: Key = backend_id.to_bytes().to_vec().into();
        let event = self
            .storage
            .send_storage_effect(StorageEffect::BatchRead {
                reads: vec![
                    (GROUP_STORAGE_BACKEND_KEYSPACE.to_string(), key.clone()),
                    (GROUP_STORAGE_BACKEND_SECRET_KEYSPACE.to_string(), key),
                ],
                txn_id: None,
            })
            .await;
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return Err(BlobError::ReadError(format!(
                "group backend {backend_id} could not be read"
            )));
        };
        let [(_, record), (_, secret)] = values.as_slice() else {
            return Err(BlobError::ReadError(format!(
                "group backend {backend_id} returned an incomplete read"
            )));
        };
        let (Some(record), Some(secret)) = (record, secret) else {
            return Err(BlobError::UnknownBackend(format!(
                "group backend {backend_id} is not registered"
            )));
        };
        let record =
            GroupStorageBackend::from_bytes(record.as_ref()).map_err(BlobError::ConversionError)?;
        let secret = GroupStorageBackendSecret::from_bytes(secret.as_ref())
            .map_err(BlobError::ConversionError)?;
        group_entry(&record, &secret, self.registry.timeouts())
    }

    /// Create-time proof that the credentials work and the endpoint is
    /// reachable through the guard: a sentinel object is written and removed.
    pub(super) async fn check_group_backend(
        &self,
        record: GroupStorageBackend,
        secret: GroupStorageBackendSecret,
    ) -> BlobEvent {
        let entry = match group_entry(&record, &secret, self.registry.timeouts()) {
            Ok(entry) => entry,
            Err(error) => return BlobEvent::Error(error),
        };
        let mut config = entry.config.service_config.clone();
        config.insert("root".to_string(), entry.config.root.clone());
        let operator = match build_group_service(record.kind, config, &self.egress) {
            Ok(operator) => operator,
            Err(error) => return BlobEvent::Error(BlobError::OperatorCreationFailed(error)),
        };

        let probe = format!("{PROBE_KEY}/{}", Ulid::generate());
        if let Err(error) = operator.write(&probe, b"aruna".to_vec()).await {
            return BlobEvent::Error(BlobError::WriteError(error.to_string()));
        }
        // Credentials that cannot delete break object deletion, multipart abort
        // and cleanup later, and leak the probe object now.
        if let Err(error) = operator.delete(&probe).await {
            return BlobEvent::Error(BlobError::DeleteError(format!(
                "group backend probe object could not be removed: {error}"
            )));
        }
        BlobEvent::GroupBackendChecked
    }
}

#[cfg(test)]
mod tests {
    use super::{group_entry, group_ids};
    use aruna_core::effects::BlobEffect;
    use aruna_core::structs::{
        Backend, BackendRef, BlobTimeoutConfig, GroupBackendKind, GroupStorageBackend,
        GroupStorageBackendSecret, ResolvedBackend,
    };
    use std::collections::HashMap;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn record(kind: GroupBackendKind, public: &[(&str, &str)]) -> GroupStorageBackend {
        GroupStorageBackend {
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
            cleanup: aruna_core::structs::CleanupStrategy::Retain,
        }
    }

    fn secret() -> GroupStorageBackendSecret {
        GroupStorageBackendSecret {
            backend_id: Ulid::from_bytes([1u8; 16]),
            secret_config: HashMap::from([("account_key".to_string(), "key".to_string())]),
            updated_at: SystemTime::UNIX_EPOCH,
        }
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
            Some("key")
        );
    }

    #[test]
    fn collects_group_refs() {
        let backend_id = Ulid::from_bytes([7u8; 16]);
        let effect = BlobEffect::HandleReplication {
            replication_id: None,
            stream_id: Ulid::from_bytes([8u8; 16]),
            resolved: ResolvedBackend::new(BackendRef::Group(backend_id), None),
            keep_alive: false,
        };

        assert_eq!(group_ids(&effect), vec![backend_id]);
        assert!(group_ids(&BlobEffect::ListHidden { namespace: None }).is_empty());
    }
}
