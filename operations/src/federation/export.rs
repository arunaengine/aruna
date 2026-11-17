//! Source side of an export into another realm: the grant for a finished export artifact, its
//! node-local record with a revoked flag, and the checks every artifact read repeats.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;

use aruna_core::UserId;
use aruna_core::effects::BlobEffect;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::federation::Signed;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    BLOB_VERSIONS_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE, FEDERATION_KEYSPACE,
};
use aruna_core::structs::execution::job::{ExportRoCrateSpec, ExportSelection, JobId};
use aruna_core::structs::identity::auth::{AuthContext, NodeCapabilities, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{BlobVersion, VersionKey};
use aruna_core::structs::storage::encryption::{BucketEncryption, BucketKeyRef};
use aruna_core::transfer::{ExportGrant, GrantError, GrantRecord, PinnedSource, check_issued};
use ulid::Ulid;
use url::Url;

use crate::auth::bearer_token::realm_user_cutoff;
use crate::auth::request_authorization::authorize;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::driver::{DriverContext, drive, drive_retry};
use crate::federation::grant::{IssueGrantConfig, IssueGrantOperation, record_key};
use crate::federation::records::{RecordChange, RecordOperation, RecordOutcome};
use crate::jobs::export::{crate_jsonld, unheld_bucket};
use crate::jobs::key_wake::read_row;
use crate::replication::plaintext::is_holder;

const EXPORT_OPERATION: &str = "federation.export";

fn principal(user_id: UserId, realm_id: RealmId) -> AuthContext {
    AuthContext {
        user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    }
}

/// READ on the document and the deny-only `federation.export` policies for `audience`.
pub async fn authorize_export(
    context: &DriverContext,
    auth: &AuthContext,
    document_path: &str,
    audience: RealmId,
    with_files: bool,
) -> Result<(), GrantError> {
    let extras = PolicyRequestExtras {
        operation: EXPORT_OPERATION.to_string(),
        params: BTreeMap::from([
            ("destination_realm".to_string(), audience.to_string()),
            ("with_files".to_string(), with_files.to_string()),
        ]),
        ..Default::default()
    };
    authorize(
        context,
        auth.realm_id,
        auth,
        document_path,
        &Permission::READ,
        extras,
    )
    .await
    .map_err(|_| GrantError::Denied)
}

/// Refuses a file export at consent when a selected file has copies on this node and each needs
/// a bucket key the caller does not hold now. The export job applies the same rule.
pub async fn check_holder(
    context: &Arc<DriverContext>,
    spec: &ExportRoCrateSpec,
    node_id: NodeId,
) -> Result<(), GrantError> {
    let (auth, limit) = (&spec.auth_context, spec.limits.metadata_bytes);
    let (jsonld, _) = crate_jsonld(context, auth, spec.document_id, limit)
        .await
        .map_err(|error| GrantError::Storage(error.to_string()))?;
    match unheld_bucket(context, spec, node_id, &jsonld).await {
        Ok(None) => Ok(()),
        Ok(Some(bucket)) => Err(GrantError::NotHolder(bucket)),
        Err(error) => Err(GrantError::Storage(error)),
    }
}

/// Whether bucket key generation `key` is unlocked on this node.
async fn unlocked(context: &DriverContext, key: BucketKeyRef) -> Result<bool, GrantError> {
    let blob = context.blob_handle.as_ref().ok_or(GrantError::Denied)?;
    let effect = BlobEffect::ReadKeyStatus {
        bucket_id: key.bucket_id,
    };
    match blob.send_blob_effect(effect).await {
        Event::Blob(BlobEvent::KeyStatus { generations }) => Ok(generations
            .iter()
            .any(|status| status.key == key && status.active)),
        other => Err(GrantError::Storage(format!(
            "key status failed: {}",
            other.label()
        ))),
    }
}

/// The active key of `bucket`, which a plain copy in an encrypting bucket is read under.
async fn active_key(
    context: &DriverContext,
    bucket: &str,
) -> Result<Option<BucketKeyRef>, GrantError> {
    let row = bucket.as_bytes().to_vec();
    let row = read_row(&context.storage_handle, BUCKET_ENCRYPTION_KEYSPACE, row)
        .await
        .map_err(GrantError::Storage)?;
    let settings = BucketEncryption::from_row(row.as_deref())
        .map_err(|error| GrantError::Storage(error.to_string()))?;
    Ok(settings.active_key())
}

/// Whether the pinned version still exists with its hash.
async fn pinned(context: &DriverContext, source: &PinnedSource) -> Result<bool, GrantError> {
    let key = VersionKey::new(source.bucket.clone(), source.key.clone(), source.version_id);
    let key = key
        .to_bytes()
        .map_err(|e| GrantError::Storage(e.to_string()))?;
    let row = read_row(&context.storage_handle, BLOB_VERSIONS_KEYSPACE, key)
        .await
        .map_err(GrantError::Storage)?;
    let Some(row) = row else {
        return Ok(false);
    };
    let version = BlobVersion::from_bytes(&row).map_err(|e| GrantError::Storage(e.to_string()))?;
    Ok(version.blob_hash() == Some(&source.blake3))
}

/// Current READ on the document and every pinned version, the export policies, the pinned
/// versions themselves, and for a copy read under a bucket key that key unlocked and held.
async fn recheck(
    context: &DriverContext,
    auth: &AuthContext,
    record: &GrantRecord,
) -> Result<(), GrantError> {
    let cutoff = realm_user_cutoff(&context.storage_handle, auth.realm_id, &record.principal)
        .await
        .map_err(|error| GrantError::Storage(error.to_string()))?;
    // A credential cutoff of the consenting user also ends grants issued before it.
    if cutoff.is_some_and(|cutoff| record.grant.payload.issued_at < cutoff) {
        return Err(GrantError::Denied);
    }
    let audience = record.grant.payload.audience;
    let path = &record.document_path;
    authorize_export(context, auth, path, audience, record.with_files).await?;
    for source in &record.sources {
        authorize_export(context, auth, &source.path, audience, true).await?;
        if !pinned(context, source).await? {
            return Err(GrantError::Denied);
        }
        let key = match source.key_ref {
            Some(key) => Some(key),
            None => active_key(context, &source.bucket).await?,
        };
        if let Some(key) = key {
            let holder = is_holder(context, auth.realm_id, &source.bucket, auth.user_id)
                .await
                .map_err(GrantError::Storage)?;
            if !holder || !unlocked(context, key).await? {
                return Err(GrantError::Denied);
            }
        }
    }
    Ok(())
}

/// Runs one transactional change of a grant record.
async fn change_record(
    context: &DriverContext,
    change: RecordChange,
) -> Result<RecordOutcome, GrantError> {
    let run = || RecordOperation::new(change.clone());
    drive_retry(run, context, change.key())
        .await
        .map_err(|error| GrantError::Storage(error.to_string()))
}

/// The grant record of `job_id`, if one was issued.
pub async fn read_record(
    context: &DriverContext,
    job_id: JobId,
) -> Result<Option<GrantRecord>, GrantError> {
    let row = read_row(
        &context.storage_handle,
        FEDERATION_KEYSPACE,
        record_key(job_id),
    )
    .await
    .map_err(GrantError::Storage)?;
    row.map(|row| postcard::from_bytes(&row).map_err(|e| GrantError::Storage(e.to_string())))
        .transpose()
}

/// Inputs of a grant for a finished export into another realm.
pub struct GrantRequest<'a> {
    pub auth: &'a AuthContext,
    pub job_id: JobId,
    pub document_id: Ulid,
    pub document_path: String,
    pub selection: &'a ExportSelection,
    pub artifact_url: Url,
    pub capabilities: &'a NodeCapabilities,
    pub now: u64,
}

/// Issues the grant of a finished export after the current checks and stores its record. A
/// repeated request returns the stored grant while it is valid; an expired grant needs a new
/// intent and export.
pub async fn issue_grant(
    context: &DriverContext,
    request: GrantRequest<'_>,
) -> Result<Signed<ExportGrant>, GrantError> {
    let auth = request.auth;
    let issue = IssueGrantOperation::new(IssueGrantConfig {
        realm_id: auth.realm_id,
        principal: auth.user_id,
        job_id: request.job_id,
        document_id: request.document_id,
        document_path: request.document_path,
        selection: request.selection.clone(),
        artifact_url: request.artifact_url,
        capabilities: request.capabilities.clone(),
        now: request.now,
    });
    let (stored, record) = drive(issue, context).await?;
    recheck(context, auth, &record).await?;
    if stored {
        return Ok(record.grant);
    }
    let key = record_key(request.job_id);
    // A racing request may have stored its grant or a revocation first; that one wins.
    match change_record(context, RecordChange::StoreGrant { key, record }).await? {
        RecordOutcome::Grant(stored) if stored.revoked => Err(GrantError::Revoked),
        RecordOutcome::Grant(stored) => Ok(stored.grant),
        other => Err(GrantError::Storage(format!("unexpected outcome {other:?}"))),
    }
}

/// Admits one artifact read by a grant: its signature, binding and lifetime, the stored record
/// and revoked flag, and the current checks of the consenting user.
pub async fn admit_grant(
    context: &DriverContext,
    local: RealmId,
    job_id: JobId,
    grant: &Signed<ExportGrant>,
    now: u64,
) -> Result<GrantRecord, GrantError> {
    check_issued(grant, &local, job_id.as_ulid(), now)?;
    let record = read_record(context, job_id)
        .await?
        .filter(|record| record.grant == *grant)
        .ok_or(GrantError::Missing)?;
    if record.revoked {
        return Err(GrantError::Revoked);
    }
    recheck(context, &principal(record.principal, local), &record).await?;
    Ok(record)
}

/// Revokes the grant of `job_id` for every later read; false when none was issued.
pub async fn revoke_grant(context: &DriverContext, job_id: JobId) -> Result<bool, GrantError> {
    let key = record_key(job_id);
    match change_record(context, RecordChange::RevokeGrant { key }).await? {
        RecordOutcome::Revoked(existed) => Ok(existed),
        other => Err(GrantError::Storage(format!("unexpected outcome {other:?}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::StorageEvent;
    use aruna_core::transfer::{MAX_TRANSFER_SECS, TransferError, selection_digest};
    use ed25519_dalek::SigningKey;
    use tempfile::{TempDir, tempdir};

    const NOW: u64 = 10_000;

    async fn write_record(
        context: &DriverContext,
        job_id: JobId,
        record: &GrantRecord,
    ) -> Result<(), GrantError> {
        let value = postcard::to_allocvec(record).unwrap();
        put(context, FEDERATION_KEYSPACE, record_key(job_id), value).await;
        Ok(())
    }

    fn context() -> (TempDir, DriverContext) {
        let dir = tempdir().unwrap();
        let context = DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(dir.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        (dir, context)
    }

    fn capabilities() -> NodeCapabilities {
        NodeCapabilities::management_node(SigningKey::from_bytes(&[4; 32])).unwrap()
    }

    fn local() -> RealmId {
        RealmId::from_bytes(SigningKey::from_bytes(&[4; 32]).verifying_key().to_bytes())
    }

    fn job() -> JobId {
        JobId::from_bytes([5; 16])
    }

    fn record() -> GrantRecord {
        let grant = ExportGrant {
            source: local(),
            audience: RealmId::from_bytes([9; 32]),
            intent_digest: "aa".repeat(32),
            export_job_id: job().as_ulid(),
            document_id: Ulid::from_bytes([6; 16]),
            source_revision: Ulid::from_bytes([7; 16]),
            dataset_digest: "bb".repeat(32),
            selection_digest: "cc".repeat(32),
            artifact_url: Url::parse("https://a.example.org/artifact").unwrap(),
            artifact_blake3: "dd".repeat(32),
            artifact_size: 10,
            issued_at: NOW,
            expires_at: NOW + MAX_TRANSFER_SECS,
        };
        GrantRecord {
            grant: Signed::sign(grant, &capabilities()).unwrap(),
            principal: UserId::new(Ulid::from_bytes([1; 16]), local()),
            document_path: "/doc".to_string(),
            with_files: false,
            sources: Vec::new(),
            revoked: false,
        }
    }

    #[tokio::test]
    async fn revoked_grant_refused() {
        // After revocation every later read is refused before any other check.
        let (_dir, context) = context();
        let record = record();
        write_record(&context, job(), &record).await.unwrap();
        assert!(revoke_grant(&context, job()).await.unwrap());
        let admitted = admit_grant(&context, local(), job(), &record.grant, NOW).await;
        assert_eq!(admitted, Err(GrantError::Revoked));
    }

    #[tokio::test]
    async fn denial_stops_reads() {
        // A consenting user who lost READ on the document can no longer be read for.
        let (_dir, context) = context();
        let record = record();
        write_record(&context, job(), &record).await.unwrap();
        let admitted = admit_grant(&context, local(), job(), &record.grant, NOW).await;
        assert_eq!(admitted, Err(GrantError::Denied));
    }

    async fn put(context: &DriverContext, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
        let effect = StorageEffect::Write {
            key_space: key_space.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        };
        context.storage_handle.send_storage_effect(effect).await;
    }

    #[tokio::test]
    async fn encrypted_needs_holder() {
        // A reader who holds no key of an encrypting bucket is refused for its files only.
        use aruna_core::document::DocumentTarget;
        use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE, S3_BUCKET_KEYSPACE};
        use aruna_core::structs::identity::auth::Actor;
        use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
        use aruna_core::structs::identity::realm::{
            RealmAuthorizationDocument, RealmConfigDocument,
        };
        use aruna_core::structs::storage::blob::BucketInfo;
        let (_dir, context) = context();
        let (realm, group) = (local(), Ulid::from_bytes([2; 16]));
        let owner = UserId::new(Ulid::from_bytes([9; 16]), realm);
        let mut record = record();
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[3; 32]).public(),
            user_id: owner,
            realm_id: realm,
        };
        let mut group_auth = GroupAuthorizationDocument::default_group_doc(owner, realm, group);
        let viewer = group_auth
            .roles
            .values_mut()
            .find(|role| role.name == "viewer");
        viewer.unwrap().assigned_users.insert(record.principal);
        let group_doc = Group {
            display_name: "group".to_string(),
            group_id: group,
            realm_id: realm,
            roles: group_auth.roles.keys().copied().collect(),
            owner,
        };
        let realm_auth = RealmAuthorizationDocument::default_realm_doc(realm);
        let config = RealmConfigDocument::default_for_realm(realm, Vec::new());
        let config_target = DocumentTarget::RealmConfig { realm_id: realm };
        let info = BucketInfo {
            group_id: group,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: owner,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Default::default(),
            distribution: None,
        };
        let realm_key = realm.as_bytes().to_vec();
        put(
            &context,
            AUTH_KEYSPACE,
            realm_key,
            realm_auth.to_bytes(&actor).unwrap(),
        )
        .await;
        let group_key = group.to_bytes().to_vec();
        let auth_doc = group_auth.to_bytes(&actor).unwrap();
        put(&context, AUTH_KEYSPACE, group_key.clone(), auth_doc).await;
        put(
            &context,
            GROUP_KEYSPACE,
            group_key,
            group_doc.to_bytes(&actor).unwrap(),
        )
        .await;
        let config_key = config_target.storage_key().to_vec();
        let config_space = config_target.storage_keyspace();
        put(
            &context,
            config_space,
            config_key.clone(),
            config.to_bytes(&actor).unwrap(),
        )
        .await;
        put(
            &context,
            S3_BUCKET_KEYSPACE,
            b"sealed".to_vec(),
            info.to_bytes().unwrap(),
        )
        .await;
        let path = format!("/{realm}/g/{group}/meta/doc");
        record.document_path = path.clone();
        record.with_files = true;
        let version_id = Ulid::from_bytes([3; 16]);
        record.sources = vec![PinnedSource {
            bucket: "sealed".to_string(),
            key: "data.csv".to_string(),
            path,
            version_id,
            blake3: [7; 32],
            size: 5,
            key_ref: None,
        }];
        write_record(&context, job(), &record).await.unwrap();
        // The pinned version must still exist with its hash.
        let admitted = admit_grant(&context, local(), job(), &record.grant, NOW).await;
        assert_eq!(admitted, Err(GrantError::Denied));
        let version = BlobVersion::materialized(
            [7; 32],
            aruna_core::structs::storage::blob::BackendRef::node_default(),
            aruna_core::structs::storage::format::EncodingClass::Raw,
            std::time::SystemTime::UNIX_EPOCH,
            owner,
            None,
        );
        let key = VersionKey::new("sealed".to_string(), "data.csv".to_string(), version_id);
        let row = version.to_bytes().unwrap();
        put(
            &context,
            BLOB_VERSIONS_KEYSPACE,
            key.to_bytes().unwrap(),
            row,
        )
        .await;
        let admitted = admit_grant(&context, local(), job(), &record.grant, NOW).await;
        assert!(admitted.is_ok());
        // A plain copy of an encrypting bucket needs the bucket's active key like a sealed one.
        use aruna_core::structs::storage::encryption::EncryptionMode;
        let mut settings = BucketEncryption {
            mode: EncryptionMode::NodeManaged,
            bucket_id: Some(Ulid::from_bytes([8; 16])),
            key_generation: 1,
            ..Default::default()
        };
        let sealed = b"sealed".to_vec();
        let row = settings.to_bytes().unwrap();
        put(&context, BUCKET_ENCRYPTION_KEYSPACE, sealed.clone(), row).await;
        let admitted = admit_grant(&context, local(), job(), &record.grant, NOW).await;
        assert_eq!(admitted, Err(GrantError::Denied));
        settings.mode = EncryptionMode::Off;
        let row = settings.to_bytes().unwrap();
        put(&context, BUCKET_ENCRYPTION_KEYSPACE, sealed, row).await;
        record.sources[0].key_ref = Some(BucketKeyRef::new(Ulid::from_bytes([8; 16]), 1));
        write_record(&context, job(), &record).await.unwrap();
        let admitted = admit_grant(&context, local(), job(), &record.grant, NOW).await;
        assert_eq!(admitted, Err(GrantError::Denied));
        // A credential cutoff of the consenting user after issuance ends the grant.
        use aruna_core::time::unix_timestamp_secs;
        record.sources[0].key_ref = None;
        write_record(&context, job(), &record).await.unwrap();
        let mut config = config;
        config
            .revoked_tokens
            .push(aruna_core::structs::identity::realm::TokenRevocation {
                token_hash: aruna_core::auth::user_cutoff_hash(&record.principal),
                expires_at: aruna_core::auth::user_cutoff_expiry(unix_timestamp_secs()),
            });
        let config_bytes = config.to_bytes(&actor).unwrap();
        put(&context, config_space, config_key, config_bytes).await;
        let admitted = admit_grant(&context, local(), job(), &record.grant, NOW).await;
        assert_eq!(admitted, Err(GrantError::Denied));
    }

    #[tokio::test]
    async fn grant_needs_record() {
        let (_dir, context) = context();
        let record = record();
        // A valid signature alone is not enough: the node-local record must exist.
        let admitted = admit_grant(&context, local(), job(), &record.grant, NOW).await;
        assert_eq!(admitted, Err(GrantError::Missing));
        assert!(!revoke_grant(&context, job()).await.unwrap());
        write_record(&context, job(), &record).await.unwrap();
        // The grant works only at the artifact of its own export job.
        let other = JobId::from_bytes([8; 16]);
        let admitted = admit_grant(&context, local(), other, &record.grant, NOW).await;
        assert_eq!(admitted, Err(GrantError::Transfer(TransferError::Unbound)));
        let late = NOW + MAX_TRANSFER_SECS;
        let admitted = admit_grant(&context, local(), job(), &record.grant, late).await;
        assert_eq!(
            admitted,
            Err(GrantError::Transfer(TransferError::BadLifetime))
        );
    }

    #[test]
    fn issue_never_writes() {
        // The issue decision never writes: a revoked record refuses, no checkpoint is unfinished.
        let record = record();
        let config = IssueGrantConfig {
            realm_id: local(),
            principal: record.principal,
            job_id: job(),
            document_id: record.grant.payload.document_id,
            document_path: record.document_path.clone(),
            selection: ExportSelection {
                files: Vec::new(),
                audience: record.grant.payload.audience,
                intent_digest: String::new(),
            },
            artifact_url: record.grant.payload.artifact_url.clone(),
            capabilities: capabilities(),
            now: NOW,
        };
        let read = |record: Option<Vec<u8>>| {
            Event::Storage(StorageEvent::BatchReadResult {
                values: vec![
                    (Vec::new().into(), record.map(Into::into)),
                    (Vec::new().into(), None),
                ],
            })
        };
        let mut operation = IssueGrantOperation::new(config.clone());
        assert_eq!(operation.start().len(), 1);
        assert!(operation.step(read(None)).is_empty());
        assert_eq!(operation.finalize(), Err(GrantError::Unfinished));
        let revoked = GrantRecord {
            revoked: true,
            ..record
        };
        let mut operation = IssueGrantOperation::new(config.clone());
        operation.start();
        operation.step(read(Some(postcard::to_allocvec(&revoked).unwrap())));
        assert_eq!(operation.finalize(), Err(GrantError::Revoked));
        // A decided grant takes no further event.
        let mut late = IssueGrantOperation::new(config);
        late.start();
        late.step(read(None));
        late.step(read(None));
        assert!(matches!(late.finalize(), Err(GrantError::Storage(_))));
    }

    #[tokio::test]
    async fn expired_grant_kept() {
        // A stored grant past its lifetime is never signed again for the same export.
        let (_dir, context) = context();
        let record = record();
        write_record(&context, job(), &record).await.unwrap();
        let auth = principal(record.principal, local());
        let selection = ExportSelection {
            files: Vec::new(),
            audience: record.grant.payload.audience,
            intent_digest: record.grant.payload.intent_digest.clone(),
        };
        let request = GrantRequest {
            auth: &auth,
            job_id: job(),
            document_id: record.grant.payload.document_id,
            document_path: record.document_path.clone(),
            selection: &selection,
            artifact_url: record.grant.payload.artifact_url.clone(),
            capabilities: &capabilities(),
            now: NOW + MAX_TRANSFER_SECS,
        };
        let issued = issue_grant(&context, request).await;
        let expired = GrantError::Transfer(TransferError::BadLifetime);
        assert_eq!(issued, Err(expired));
        let stored = read_record(&context, job()).await.unwrap();
        assert_eq!(stored, Some(record));
    }
}
