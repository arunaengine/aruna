//! Destination side of an export from another realm: the checks an import intent stands for,
//! and the node-local record that binds an upload to its intent and grant.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::NodeId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::federation::Signed;
use aruna_core::keyspaces::FEDERATION_KEYSPACE;
use aruna_core::structs::execution::job::{ImportRoCrateSource, ImportRoCrateSpec};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::bucket_permission_path;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::transfer::{ExportGrant, ImportDestination, ImportIntent};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::auth::request_authorization::authorize;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::driver::{DriverContext, drive};
use crate::jobs::key_wake::read_row;
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};

pub const IMPORT_OPERATION: &str = "federation.import";

/// Binds an upload of another realm's artifact to the intent and grant it arrived with.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportRecord {
    pub intent: Signed<ImportIntent>,
    pub grant: Signed<ExportGrant>,
}

#[derive(Debug, Error, PartialEq)]
pub enum ImportError {
    #[error("the import into this destination is not allowed")]
    Denied,
    #[error("the import does not match its intent")]
    Unbound,
    #[error("the destination bucket does not exist")]
    NoBucket,
    #[error("import storage failed: {0}")]
    Storage(String),
}

fn record_key(upload_id: Ulid) -> Vec<u8> {
    [&b"import/"[..], &upload_id.to_bytes()].concat()
}

/// WRITE on the destination bucket and metadata path, and the deny-only `federation.import`
/// policies for an import from `source`.
pub async fn authorize_import(
    context: &DriverContext,
    auth: &AuthContext,
    destination: &ImportDestination,
    source: RealmId,
    node_id: NodeId,
) -> Result<(), ImportError> {
    let bucket = match drive(GetBucketOperation::new(destination.bucket.clone()), context).await {
        Ok(bucket) => bucket,
        Err(GetBucketError::NotFound) => return Err(ImportError::NoBucket),
        Err(error) => return Err(ImportError::Storage(error.to_string())),
    };
    let realm_id = auth.realm_id;
    let metadata = MetadataRegistryRecord::normalize_document_path(&destination.metadata_path);
    let paths = [
        bucket_permission_path(realm_id, bucket.group_id, node_id, &destination.bucket),
        format!("/{realm_id}/g/{}/meta/{metadata}", destination.group_id),
    ];
    for path in paths {
        let extras = PolicyRequestExtras {
            operation: IMPORT_OPERATION.to_string(),
            params: BTreeMap::from([("source_realm".to_string(), source.to_string())]),
            ..Default::default()
        };
        authorize(context, realm_id, auth, &path, &Permission::WRITE, extras)
            .await
            .map_err(|_| ImportError::Denied)?;
    }
    Ok(())
}

fn upload_key(import_key: &str) -> Vec<u8> {
    [&b"upload/"[..], import_key.as_bytes()].concat()
}

/// Binds a verified upload to its intent and grant, and the import key to that upload, so a
/// retried transfer reuses it.
pub async fn write_import(
    context: &DriverContext,
    import_key: &str,
    upload_id: Ulid,
    record: &ImportRecord,
) -> Result<(), ImportError> {
    let value = postcard::to_allocvec(record).map_err(|e| ImportError::Storage(e.to_string()))?;
    let entry =
        |key: Vec<u8>, value: Vec<u8>| (FEDERATION_KEYSPACE.to_string(), key.into(), value.into());
    let effect = StorageEffect::BatchWrite {
        writes: vec![
            entry(record_key(upload_id), value),
            entry(upload_key(import_key), upload_id.to_bytes().to_vec()),
        ],
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(()),
        other => Err(ImportError::Storage(format!("{other:?}"))),
    }
}

/// The upload an import key was bound to by an earlier transfer.
pub async fn bound_upload(
    context: &DriverContext,
    import_key: &str,
) -> Result<Option<Ulid>, ImportError> {
    let row = read_row(
        &context.storage_handle,
        FEDERATION_KEYSPACE,
        upload_key(import_key),
    )
    .await
    .map_err(ImportError::Storage)?;
    row.map(|row| {
        let bytes = <[u8; 16]>::try_from(row.as_slice())
            .map_err(|_| ImportError::Storage("invalid upload binding".to_string()))?;
        Ok(Ulid::from_bytes(bytes))
    })
    .transpose()
}

pub async fn read_import(
    context: &DriverContext,
    upload_id: Ulid,
) -> Result<Option<ImportRecord>, ImportError> {
    let row = read_row(
        &context.storage_handle,
        FEDERATION_KEYSPACE,
        record_key(upload_id),
    )
    .await
    .map_err(ImportError::Storage)?;
    row.map(|row| postcard::from_bytes(&row).map_err(|e| ImportError::Storage(e.to_string())))
        .transpose()
}

/// Whether an import job still matches its intent's principal and destination.
fn bound(spec: &ImportRoCrateSpec, intent: &ImportIntent) -> bool {
    let destination = &intent.destination;
    let normalized = MetadataRegistryRecord::normalize_document_path;
    spec.auth_context.user_id == intent.principal
        && spec.target.bucket == destination.bucket
        && spec.target.prefix == destination.prefix
        && spec.metadata.group_id == destination.group_id
        && normalized(&spec.metadata.path) == normalized(&destination.metadata_path)
}

/// Rechecks an import of another realm's artifact at every step; other imports pass. Expiry
/// of the intent does not stop a running import, it was authorized when the job began.
pub async fn recheck_import(
    context: &DriverContext,
    spec: &ImportRoCrateSpec,
    node_id: NodeId,
) -> Result<(), ImportError> {
    let ImportRoCrateSource::Upload { upload_id } = &spec.source else {
        return Ok(());
    };
    let Some(record) = read_import(context, *upload_id).await? else {
        return Ok(());
    };
    let intent = &record.intent.payload;
    if !bound(spec, intent) {
        return Err(ImportError::Unbound);
    }
    let source = record.grant.payload.source;
    authorize_import(
        context,
        &spec.auth_context,
        &intent.destination,
        source,
        node_id,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
    use aruna_core::structs::execution::job::{
        ImportMetadataTarget, ImportRoCrateTarget, RoCrateLimits,
    };
    use aruna_core::structs::identity::auth::NodeCapabilities;
    use aruna_core::structs::storage::blob::BucketInfo;
    use aruna_core::transfer::MAX_TRANSFER_SECS;
    use ed25519_dalek::SigningKey;
    use std::time::SystemTime;
    use tempfile::{TempDir, tempdir};
    use url::Url;

    fn context() -> (TempDir, DriverContext) {
        let dir = tempdir().unwrap();
        let context = DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(dir.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        (dir, context)
    }

    fn realm(seed: u8) -> RealmId {
        RealmId::from_bytes(
            SigningKey::from_bytes(&[seed; 32])
                .verifying_key()
                .to_bytes(),
        )
    }

    fn signer(seed: u8) -> NodeCapabilities {
        NodeCapabilities::management_node(SigningKey::from_bytes(&[seed; 32])).unwrap()
    }

    fn principal() -> UserId {
        UserId::new(Ulid::from_bytes([3; 16]), realm(1))
    }

    fn record() -> ImportRecord {
        let intent = ImportIntent {
            realm_id: realm(2),
            descriptor_digest: String::new(),
            principal: principal(),
            destination: ImportDestination {
                group_id: Ulid::from_bytes([1; 16]),
                bucket: "lab".to_string(),
                prefix: "imports".to_string(),
                metadata_path: "datasets/run".to_string(),
            },
            max_bytes: 100,
            nonce: String::new(),
            issued_at: 1,
            expires_at: 1 + MAX_TRANSFER_SECS,
            intent_id: Ulid::from_bytes([4; 16]),
        };
        let grant = ExportGrant {
            source: realm(1),
            audience: realm(2),
            intent_digest: String::new(),
            export_job_id: Ulid::from_bytes([5; 16]),
            document_id: Ulid::from_bytes([6; 16]),
            source_revision: Ulid::from_bytes([7; 16]),
            dataset_digest: String::new(),
            selection_digest: String::new(),
            artifact_url: Url::parse("https://a.example.org/artifact").unwrap(),
            artifact_blake3: String::new(),
            artifact_size: 10,
            issued_at: 1,
            expires_at: 1 + MAX_TRANSFER_SECS,
        };
        ImportRecord {
            intent: Signed::sign(intent, &signer(2)).unwrap(),
            grant: Signed::sign(grant, &signer(1)).unwrap(),
        }
    }

    fn spec(upload_id: Ulid) -> ImportRoCrateSpec {
        ImportRoCrateSpec {
            auth_context: AuthContext {
                user_id: principal(),
                realm_id: realm(2),
                path_restrictions: None,
                session: None,
            },
            source: ImportRoCrateSource::Upload { upload_id },
            target: ImportRoCrateTarget {
                bucket: "lab".to_string(),
                prefix: "imports".to_string(),
            },
            metadata: ImportMetadataTarget {
                group_id: Ulid::from_bytes([1; 16]),
                path: "datasets/run".to_string(),
                public: false,
            },
            limits: RoCrateLimits::default(),
            document_id: Ulid::nil(),
        }
    }

    fn node() -> NodeId {
        iroh::SecretKey::from_bytes(&[8; 32]).public()
    }

    #[tokio::test]
    async fn destination_binding_kept() {
        let (_dir, context) = context();
        let upload_id = Ulid::from_bytes([9; 16]);
        // An ordinary upload import has no record and is not rechecked here.
        assert_eq!(
            recheck_import(&context, &spec(upload_id), node()).await,
            Ok(())
        );
        write_import(&context, "key", upload_id, &record())
            .await
            .unwrap();
        assert_eq!(bound_upload(&context, "key").await, Ok(Some(upload_id)));
        let mut moved = spec(upload_id);
        moved.target.prefix = "elsewhere".to_string();
        let checked = recheck_import(&context, &moved, node()).await;
        assert_eq!(checked, Err(ImportError::Unbound));
        let mut other = spec(upload_id);
        other.auth_context.user_id = UserId::new(Ulid::from_bytes([4; 16]), realm(2));
        let checked = recheck_import(&context, &other, node()).await;
        assert_eq!(checked, Err(ImportError::Unbound));
    }

    #[tokio::test]
    async fn resumed_import_denied() {
        // A principal without WRITE on the destination is refused when the job resumes.
        let (_dir, context) = context();
        let upload_id = Ulid::from_bytes([9; 16]);
        write_import(&context, "key", upload_id, &record())
            .await
            .unwrap();
        let info = BucketInfo {
            group_id: Ulid::from_bytes([1; 16]),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: principal(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Default::default(),
        };
        let effect = StorageEffect::Write {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: b"lab".to_vec().into(),
            value: info.to_bytes().unwrap().into(),
            txn_id: None,
        };
        context.storage_handle.send_storage_effect(effect).await;
        let checked = recheck_import(&context, &spec(upload_id), node()).await;
        assert_eq!(checked, Err(ImportError::Denied));
    }
}
