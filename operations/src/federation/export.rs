//! Source side of an export into another realm: the grant for a finished export artifact, its
//! node-local record with a revoked flag, and the checks every artifact read repeats.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::federation::Signed;
use aruna_core::id::NodeId;
use aruna_core::structs::execution::job::{ExportRoCrateSpec, ExportSelection, JobId};
use aruna_core::structs::identity::auth::{AuthContext, NodeCapabilities};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::transfer::{ExportGrant, GrantError, GrantRecord};
use ulid::Ulid;
use url::Url;

use crate::driver::{DriverContext, drive, drive_retry};
use crate::federation::grant::{
    AdmitGrantOperation, IssueExportOperation, IssueGrantConfig, ReadGrantOperation, export_access,
    record_key,
};
use crate::federation::records::{RecordChange, RecordOperation, RecordOutcome};
use crate::jobs::export::{crate_jsonld, unheld_bucket};

/// READ on the document and the deny-only `federation.export` policies for `audience`.
pub async fn authorize_export(
    context: &DriverContext,
    auth: &AuthContext,
    document_path: &str,
    audience: RealmId,
    with_files: bool,
) -> Result<(), GrantError> {
    let access = export_access(auth, document_path, audience, with_files);
    match drive(access, context).await {
        Ok(true) => Ok(()),
        Ok(false) | Err(_) => Err(GrantError::Denied),
    }
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

/// The grant record of `job_id`, if one was issued.
pub async fn read_record(
    context: &DriverContext,
    job_id: JobId,
) -> Result<Option<GrantRecord>, GrantError> {
    drive(ReadGrantOperation::new(job_id), context).await
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

/// See [`IssueExportOperation`].
pub async fn issue_grant(
    context: &DriverContext,
    request: GrantRequest<'_>,
) -> Result<Signed<ExportGrant>, GrantError> {
    let auth = request.auth;
    let config = IssueGrantConfig {
        realm_id: auth.realm_id,
        principal: auth.user_id,
        job_id: request.job_id,
        document_id: request.document_id,
        document_path: request.document_path,
        selection: request.selection.clone(),
        artifact_url: request.artifact_url,
        capabilities: request.capabilities.clone(),
        now: request.now,
    };
    drive(IssueExportOperation::new(auth.clone(), config), context).await
}

/// See [`AdmitGrantOperation`].
pub async fn admit_grant(
    context: &DriverContext,
    local: RealmId,
    job_id: JobId,
    grant: &Signed<ExportGrant>,
    now: u64,
) -> Result<GrantRecord, GrantError> {
    let admit = AdmitGrantOperation::new(local, job_id, grant.clone(), now);
    drive(admit, context).await
}

/// Revokes the grant of `job_id` for every later read; false when none was issued.
pub async fn revoke_grant(context: &DriverContext, job_id: JobId) -> Result<bool, GrantError> {
    let change = RecordChange::RevokeGrant {
        key: record_key(job_id),
    };
    let run = || RecordOperation::new(change.clone());
    let outcome = drive_retry(run, context, change.key()).await;
    match outcome.map_err(|error| GrantError::Storage(error.to_string()))? {
        RecordOutcome::Revoked(existed) => Ok(existed),
        other => Err(GrantError::Storage(format!("unexpected outcome {other:?}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::federation::grant::{IssueGrantOperation, grant_principal};
    use aruna_core::UserId;
    use aruna_core::calls::{IoCall, IoResult, Next};
    use aruna_core::effects::StorageEffect;
    use aruna_core::keyspaces::{
        BLOB_VERSIONS_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE, FEDERATION_KEYSPACE,
    };
    use aruna_core::operation::Operation;
    use aruna_core::structs::storage::blob::{BlobVersion, VersionKey};
    use aruna_core::structs::storage::encryption::{BucketEncryption, BucketKeyRef};
    use aruna_core::transfer::PinnedSource;
    use aruna_core::transfer::{MAX_TRANSFER_SECS, TransferError};
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
        let read = |record: Option<Vec<u8>>| IoResult::Values(vec![record.map(Into::into), None]);
        let mut operation = IssueGrantOperation::new(config.clone());
        assert!(matches!(operation.first(), Ok(Next::Call(IoCall::Read(_)))));
        assert_eq!(operation.next(read(None)), Err(GrantError::Unfinished));
        let revoked = GrantRecord {
            revoked: true,
            ..record
        };
        let mut operation = IssueGrantOperation::new(config);
        let revoked = postcard::to_allocvec(&revoked).unwrap();
        assert_eq!(
            operation.next(read(Some(revoked))),
            Err(GrantError::Revoked)
        );
    }

    #[tokio::test]
    async fn expired_grant_kept() {
        // A stored grant past its lifetime is never signed again for the same export.
        let (_dir, context) = context();
        let record = record();
        write_record(&context, job(), &record).await.unwrap();
        let auth = grant_principal(record.principal, local());
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
