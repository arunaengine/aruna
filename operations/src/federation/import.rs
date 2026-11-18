//! Destination side of an export from another realm: the checks an import intent stands for,
//! and the node-local record that binds an upload to its intent and grant.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::federation::records::ImportRecord;

use aruna_core::federation::Signed;
use aruna_core::structs::execution::job::ImportRoCrateSpec;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::transfer::{ExportGrant, ImportDestination, ImportIntent};
use aruna_core::{NodeId, UserId};
use serde::Serialize;
use thiserror::Error;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::federation::import_checks::{
    AuthorizeImportOperation, BoundUploadOperation, ConfirmSourceOperation, ReadImportOperation,
    RecheckImportOperation, ReusableUploadOperation, WriteImportOperation, answer_error,
};

#[derive(Debug, Error, PartialEq)]
pub enum ImportError {
    #[error("the import into this destination is not allowed")]
    Denied,
    #[error("the import does not match its intent")]
    Unbound,
    #[error("the destination bucket does not exist")]
    NoBucket,
    #[error("the import key is bound to another transfer")]
    Conflict,
    #[error("the import waits for a fresh intent and grant")]
    Expired,
    #[error("import storage failed: {0}")]
    Storage(String),
    #[error("the source realm could not be reached: {0}")]
    Unreachable(String),
    #[error("the source realm answered {0}")]
    Refused(reqwest::StatusCode),
}

/// Header carrying a signed export grant as unpadded base64url JSON.
pub const GRANT_HEADER: &str = "x-aruna-export-grant";

/// The unpadded base64url JSON of a signed transfer value, as a header carries it.
pub fn header_value<T: Serialize>(value: &T) -> Result<String, serde_json::Error> {
    use base64::Engine;
    let json = serde_json::to_vec(value)?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json))
}

/// Sends `method` for the grant's artifact to the source realm through the egress guard and
/// hands back the response, whose body the caller streams.
pub async fn source_request(
    context: &DriverContext,
    method: reqwest::Method,
    grant: &Signed<ExportGrant>,
    timeout: std::time::Duration,
) -> Result<reqwest::Response, ImportError> {
    let unreachable = |error: String| ImportError::Unreachable(error);
    let blob = context
        .blob_handle
        .as_ref()
        .ok_or_else(|| ImportError::Storage("blob handle unavailable".to_string()))?;
    let header = header_value(grant).map_err(|error| ImportError::Storage(error.to_string()))?;
    let response = blob
        .repository_request(method, grant.payload.artifact_url.clone())
        .map_err(|error| unreachable(error.to_string()))?
        .header(GRANT_HEADER, header)
        .timeout(timeout)
        .send()
        .await
        .map_err(|error| unreachable(error.to_string()))?;
    if !response.status().is_success() {
        return Err(answer_error(response.status()));
    }
    Ok(response)
}

/// See [`ConfirmSourceOperation`].
pub(crate) async fn confirm_source(
    context: &DriverContext,
    spec: &ImportRoCrateSpec,
    timeout: std::time::Duration,
) -> Result<(), ImportError> {
    drive(ConfirmSourceOperation::new(spec, timeout), context).await
}

/// See [`AuthorizeImportOperation`].
pub async fn authorize_import(
    context: &DriverContext,
    auth: &AuthContext,
    destination: &ImportDestination,
    source: Option<RealmId>,
    node_id: NodeId,
) -> Result<(), ImportError> {
    let authorize =
        AuthorizeImportOperation::new(auth.clone(), destination.clone(), source, node_id);
    drive(authorize, context).await
}

/// See [`WriteImportOperation`].
pub async fn write_import(
    context: &DriverContext,
    import_key: &str,
    upload_id: Ulid,
    record: &ImportRecord,
) -> Result<Ulid, ImportError> {
    let write = WriteImportOperation::new(import_key, upload_id, record.clone());
    drive(write, context).await
}

/// The upload `principal`'s import key was bound to by an earlier transfer.
pub async fn bound_upload(
    context: &DriverContext,
    principal: UserId,
    import_key: &str,
) -> Result<Option<Ulid>, ImportError> {
    drive(BoundUploadOperation::new(principal, import_key), context).await
}

/// See [`ReusableUploadOperation`].
pub async fn reusable_upload(
    context: &DriverContext,
    intent: &ImportIntent,
    grant: &ExportGrant,
    import_key: &str,
) -> Result<Option<Ulid>, ImportError> {
    let reuse = ReusableUploadOperation::new(intent.clone(), grant.clone(), import_key);
    drive(reuse, context).await
}

pub async fn read_import(
    context: &DriverContext,
    upload_id: Ulid,
) -> Result<Option<ImportRecord>, ImportError> {
    drive(ReadImportOperation::new(upload_id), context).await
}

/// See [`RecheckImportOperation`].
pub(crate) async fn recheck_import(
    context: &DriverContext,
    spec: &ImportRoCrateSpec,
    node_id: NodeId,
    now: u64,
) -> Result<(), ImportError> {
    drive(
        RecheckImportOperation::new(spec.clone(), node_id, now),
        context,
    )
    .await
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use aruna_core::effects::StorageEffect;
    use aruna_core::federation::RealmDescriptor;
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

    pub(crate) fn context() -> (TempDir, DriverContext) {
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

    pub(crate) fn realm(seed: u8) -> RealmId {
        RealmId::from_bytes(
            SigningKey::from_bytes(&[seed; 32])
                .verifying_key()
                .to_bytes(),
        )
    }

    pub(crate) fn signer(seed: u8) -> NodeCapabilities {
        NodeCapabilities::management_node(SigningKey::from_bytes(&[seed; 32])).unwrap()
    }

    pub(crate) fn principal() -> UserId {
        UserId::new(Ulid::from_bytes([3; 16]), realm(1))
    }

    pub(crate) fn descriptor() -> Signed<RealmDescriptor> {
        let descriptor = RealmDescriptor {
            realm_id: realm(2),
            name: "B".to_string(),
            description: String::new(),
            api_url: Url::parse("https://b.example.org/api/v1").unwrap(),
            portal_url: Url::parse("https://b.example.org/").unwrap(),
            contact: None,
            login_providers: Vec::new(),
            issued_at: 1,
        };
        Signed::sign(descriptor, &signer(2)).unwrap()
    }

    /// Stores realm 2's federation settings with `descriptor`.
    async fn seed_config(context: &DriverContext, descriptor: Signed<RealmDescriptor>) {
        seed_cutoff(context, descriptor, None).await;
    }

    pub(crate) async fn seed_cutoff(
        context: &DriverContext,
        descriptor: Signed<RealmDescriptor>,
        cutoff: Option<aruna_core::structs::identity::realm::TokenRevocation>,
    ) {
        use aruna_core::document::DocumentTarget;
        use aruna_core::federation::{AcceptedRealms, FederationSettings, RegistrationMode};
        use aruna_core::structs::identity::auth::Actor;
        use aruna_core::structs::identity::realm::RealmConfigDocument;
        let mut config = RealmConfigDocument::default_for_realm(realm(2), Vec::new());
        config.revoked_tokens.extend(cutoff);
        config.federation = Some(FederationSettings {
            name: "B".to_string(),
            description: String::new(),
            contact: None,
            login_providers: Vec::new(),
            api_url: descriptor.payload.api_url.clone(),
            portal_url: descriptor.payload.portal_url.clone(),
            registry_url: None,
            registration: RegistrationMode::Enabled,
            accepted_realms: AcceptedRealms::None,
            descriptor,
        });
        let actor = Actor {
            node_id: node(),
            user_id: UserId::new(Ulid::from_bytes([2; 16]), realm(2)),
            realm_id: realm(2),
        };
        let target = DocumentTarget::RealmConfig { realm_id: realm(2) };
        let effect = StorageEffect::Write {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key().to_vec().into(),
            value: config.to_bytes(&actor).unwrap().into(),
            txn_id: None,
        };
        context.storage_handle.send_storage_effect(effect).await;
    }

    pub(crate) fn record() -> ImportRecord {
        let intent = ImportIntent {
            realm_id: realm(2),
            descriptor_digest: aruna_core::handoff::descriptor_digest(&descriptor()).unwrap(),
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
        let intent = Signed::sign(intent, &signer(2)).unwrap();
        let grant = ExportGrant {
            source: realm(1),
            audience: realm(2),
            intent_digest: aruna_core::transfer::intent_digest(&intent).unwrap(),
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
            intent,
            grant: Signed::sign(grant, &signer(1)).unwrap(),
            confirmed: false,
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

    const NOW: u64 = 100;

    fn node() -> NodeId {
        iroh::SecretKey::from_bytes(&[8; 32]).public()
    }

    #[tokio::test]
    async fn destination_binding_kept() {
        let (_dir, context) = context();
        let upload_id = Ulid::from_bytes([9; 16]);
        // An ordinary upload import has no record and is not rechecked here.
        assert_eq!(
            recheck_import(&context, &spec(upload_id), node(), NOW).await,
            Ok(())
        );
        write_import(&context, "key", upload_id, &record())
            .await
            .unwrap();
        let bound = bound_upload(&context, principal(), "key").await;
        assert_eq!(bound, Ok(Some(upload_id)));
        // Another principal's binding of the same import key is separate.
        let other = UserId::new(Ulid::from_bytes([4; 16]), realm(2));
        assert_eq!(bound_upload(&context, other, "key").await, Ok(None));
        let mut moved = spec(upload_id);
        moved.target.prefix = "elsewhere".to_string();
        let checked = recheck_import(&context, &moved, node(), NOW).await;
        assert_eq!(checked, Err(ImportError::Unbound));
        let mut other = spec(upload_id);
        other.auth_context.user_id = UserId::new(Ulid::from_bytes([4; 16]), realm(2));
        let checked = recheck_import(&context, &other, node(), NOW).await;
        assert_eq!(checked, Err(ImportError::Unbound));
    }

    #[test]
    fn conflicting_reuse_refused() {
        // A binding is reused only for the same transfer, owner and artifact.
        let stored = record();
        let (intent, grant) = (&stored.intent.payload, &stored.grant.payload);
        assert!(same_transfer(&stored, None, intent, grant));
        let mut other = grant.clone();
        other.artifact_blake3 = "ee".repeat(32);
        assert!(!same_transfer(&stored, None, intent, &other));
        let mut moved = intent.clone();
        moved.destination.prefix = "elsewhere".to_string();
        assert!(!same_transfer(&stored, None, &moved, grant));
        let mut upload = upload(0);
        let mut matching = grant.clone();
        matching.artifact_blake3 = hex::encode([0; 32]);
        let mut bound = stored.clone();
        bound.grant.payload = matching.clone();
        assert!(same_transfer(&bound, Some(&upload), intent, &matching));
        upload.owner = UserId::new(Ulid::from_bytes([4; 16]), realm(2));
        assert!(!same_transfer(&bound, Some(&upload), intent, &matching));
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
            distribution: None,
        };
        let effect = StorageEffect::Write {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: b"lab".to_vec().into(),
            value: info.to_bytes().unwrap().into(),
            txn_id: None,
        };
        context.storage_handle.send_storage_effect(effect).await;
        seed_config(&context, descriptor()).await;
        let checked = recheck_import(&context, &spec(upload_id), node(), NOW).await;
        assert_eq!(checked, Err(ImportError::Denied));
    }

    #[tokio::test]
    async fn expired_consent_pauses() {
        // An expired consent or a superseded descriptor waits for fresh consent.
        let (_dir, context) = context();
        let upload_id = Ulid::from_bytes([9; 16]);
        write_import(&context, "key", upload_id, &record())
            .await
            .unwrap();
        seed_config(&context, descriptor()).await;
        let late = 1 + MAX_TRANSFER_SECS;
        let checked = recheck_import(&context, &spec(upload_id), node(), late).await;
        assert_eq!(checked, Err(ImportError::Expired));
        let mut newer = descriptor().payload;
        newer.issued_at = 2;
        seed_config(&context, Signed::sign(newer, &signer(2)).unwrap()).await;
        let checked = recheck_import(&context, &spec(upload_id), node(), NOW).await;
        assert_eq!(checked, Err(ImportError::Expired));
    }

    #[tokio::test]
    async fn cutoff_ends_intent() {
        // A credential cutoff of the principal after issuance pauses the import.
        let (_dir, context) = context();
        let upload_id = Ulid::from_bytes([9; 16]);
        let record = record();
        write_import(&context, "key", upload_id, &record)
            .await
            .unwrap();
        let cutoff = aruna_core::structs::identity::realm::TokenRevocation {
            token_hash: aruna_core::auth::user_cutoff_hash(&principal()),
            expires_at: aruna_core::auth::user_cutoff_expiry(
                aruna_core::time::unix_timestamp_secs(),
            ),
        };
        seed_cutoff(&context, descriptor(), Some(cutoff)).await;
        let checked = recheck_import(&context, &spec(upload_id), node(), NOW).await;
        assert_eq!(checked, Err(ImportError::Expired));
    }

    #[tokio::test]
    async fn source_confirms_once() {
        // A pull or push confirmation serves one start; a resume asks the source again.
        let (_dir, context) = context();
        let upload_id = Ulid::from_bytes([9; 16]);
        let confirmed = ImportRecord {
            confirmed: true,
            ..record()
        };
        write_import(&context, "key", upload_id, &confirmed)
            .await
            .unwrap();
        let timeout = std::time::Duration::from_secs(1);
        let started = confirm_source(&context, &spec(upload_id), timeout).await;
        assert_eq!(started, Ok(()));
        let stored = read_import(&context, upload_id).await.unwrap().unwrap();
        assert!(!stored.confirmed);
        // The resume asks the source again; this context has no egress, so it cannot.
        let resumed = confirm_source(&context, &spec(upload_id), timeout).await;
        assert!(matches!(resumed, Err(ImportError::Unreachable(_))));
    }

    #[test]
    fn busy_source_waits() {
        // Source infrastructure and throttling defer the import; refusals of the grant end it.
        for code in [500, 502, 503, 504, 429, 408] {
            let status = reqwest::StatusCode::from_u16(code).unwrap();
            assert!(matches!(answer_error(status), ImportError::Unreachable(_)));
        }
        for code in [400, 401, 403, 404, 410] {
            let status = reqwest::StatusCode::from_u16(code).unwrap();
            assert_eq!(answer_error(status), ImportError::Refused(status));
        }
    }

    /// Upload `[9; 16]` of the principal with the size of `record`'s grant.
    fn upload(expires_at_ms: u64) -> RoCrateUploadRecord {
        RoCrateUploadRecord {
            upload_id: Ulid::from_bytes([9; 16]),
            owner: principal(),
            location: aruna_core::structs::storage::blob::BackendLocation {
                backend: aruna_core::structs::storage::blob::BackendRef::node_default(),
                storage_class: None,
                root: "/data".to_string(),
                storage_bucket: "storage".to_string(),
                backend_path: "input".to_string(),
                ulid: Ulid::nil(),
                format: Default::default(),
                created_by: principal(),
                created_at: SystemTime::UNIX_EPOCH,
                staging: false,
                partial: false,
                blob_size: 1,
                hashes: Default::default(),
            },
            blake3: [0; 32],
            size: record().grant.payload.artifact_size,
            media_type: aruna_core::structs::execution::job::RoCrateMediaType::Zip,
            expires_at_ms,
            claimed_by: None,
        }
    }

    #[tokio::test]
    async fn racing_binding_checked() {
        // The live winner of a racing binding serves only the same transfer; another one is
        // refused. Once that upload is gone without an import job, a new upload replaces it.
        let (_dir, context) = context();
        let (first, second) = (Ulid::from_bytes([9; 16]), Ulid::from_bytes([10; 16]));
        let live = upload(u64::MAX);
        let storage = &context.storage_handle;
        crate::jobs::import::upload_store::write_rocrate_upload(&context, &live)
            .await
            .unwrap();
        let mut record = record();
        record.grant.payload.artifact_blake3 = hex::encode(live.blake3);
        write_import(&context, "key", first, &record).await.unwrap();
        let same = write_import(&context, "key", second, &record).await;
        assert_eq!(same, Ok(first));
        let mut other = record.clone();
        other.grant.payload.artifact_size += 1;
        let conflict = write_import(&context, "key", second, &other).await;
        assert_eq!(conflict, Err(ImportError::Conflict));
        let swept = StorageEffect::Delete {
            key_space: aruna_core::keyspaces::ROCRATE_UPLOAD_KEYSPACE.to_string(),
            key: first.to_bytes().to_vec().into(),
            txn_id: None,
        };
        storage.send_storage_effect(swept).await;
        let replaced = write_import(&context, "key", second, &record).await;
        assert_eq!(replaced, Ok(second));
    }
}
