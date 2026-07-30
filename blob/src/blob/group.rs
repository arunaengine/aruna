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
use std::collections::HashMap;
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

impl BlobHandler {
    /// Refreshes any tenant backend the effect names before the synchronous
    /// lookup path needs it. Records change while the process runs, so this
    /// re-reads rather than caching: a deleted record drops the entry and the
    /// effect then fails loudly instead of using stale credentials.
    pub(super) async fn load_group_backends(&self, effect: &BlobEffect) {
        for backend_id in group_ids(effect) {
            match self.read_group_backend(backend_id).await {
                Ok(Some(entry)) => self.registry.insert_group(backend_id, entry),
                Ok(None) => {
                    self.registry.forget_group(&backend_id);
                    tracing::warn!(%backend_id, "group storage backend record is missing");
                }
                Err(error) => {
                    self.registry.forget_group(&backend_id);
                    tracing::warn!(%backend_id, error = %error, "failed to load group backend");
                }
            }
        }
    }

    async fn read_group_backend(&self, backend_id: Ulid) -> Result<Option<NodeBackend>, BlobError> {
        let Some(record) = self
            .read_record(GROUP_STORAGE_BACKEND_KEYSPACE, backend_id)
            .await
        else {
            return Ok(None);
        };
        let record =
            GroupStorageBackend::from_bytes(record.as_ref()).map_err(BlobError::ConversionError)?;
        let secret = match self
            .read_record(GROUP_STORAGE_BACKEND_SECRET_KEYSPACE, backend_id)
            .await
        {
            Some(value) => GroupStorageBackendSecret::from_bytes(value.as_ref())
                .map_err(BlobError::ConversionError)?,
            None => {
                return Err(BlobError::OperatorCreationFailed(format!(
                    "group backend {backend_id} has no stored credentials"
                )));
            }
        };
        group_entry(&record, &secret, self.registry.timeouts()).map(Some)
    }

    async fn read_record(
        &self,
        key_space: &str,
        backend_id: Ulid,
    ) -> Option<aruna_core::types::Value> {
        let event = self
            .storage
            .send_storage_effect(StorageEffect::Read {
                key_space: key_space.to_string(),
                key: backend_id.to_bytes().to_vec().into(),
                txn_id: None,
            })
            .await;
        match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
            other => {
                tracing::warn!(key_space, event = ?other, "group backend read failed");
                None
            }
        }
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
        if let Err(error) = operator.delete(&probe).await {
            tracing::warn!(error = %error, "failed to remove group backend probe object");
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
