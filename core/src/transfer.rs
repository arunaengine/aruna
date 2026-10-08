//! Signed contracts of an export from a source realm into a destination realm: the destination's
//! import intent and the source's grant for one export artifact.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use ulid::Ulid;
use url::Url;

use crate::UserId;
use crate::federation::{FederationError, RealmDescriptor, Signable, Signed, valid_federation_url};
use crate::handoff::{MAX_HANDOFF_SKEW_SECS, descriptor_digest, secret_nonce};
use crate::structs::identity::realm::RealmId;
use crate::types::GroupId;

pub const INTENT_DOMAIN: &str = "aruna-import-intent-v1";
pub const GRANT_DOMAIN: &str = "aruna-export-grant-v1";
/// Longest lifetime of an import intent and an export grant; neither is renewable.
pub const MAX_TRANSFER_SECS: u64 = 24 * 3600;

/// Where the destination realm stores the import: payload bucket and prefix, metadata path.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportDestination {
    pub group_id: GroupId,
    pub bucket: String,
    /// Normalized: no leading or trailing slash.
    pub prefix: String,
    pub metadata_path: String,
}

/// Signed by the destination realm for `principal`, who may import into `destination`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportIntent {
    pub realm_id: RealmId,
    /// Hex SHA-256 of the destination's signed descriptor when the intent was issued.
    pub descriptor_digest: String,
    pub principal: UserId,
    pub destination: ImportDestination,
    pub max_bytes: u64,
    /// Hex SHA-256 of the secret the destination portal keeps in the browser.
    pub nonce: String,
    pub issued_at: u64,
    pub expires_at: u64,
    pub intent_id: Ulid,
}

impl Signable for ImportIntent {
    const DOMAIN: &'static str = INTENT_DOMAIN;
    fn realm_id(&self) -> RealmId {
        self.realm_id
    }
}

/// Signed by the source realm for one finished export artifact and one import intent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportGrant {
    pub source: RealmId,
    pub audience: RealmId,
    pub intent_digest: String,
    pub export_job_id: Ulid,
    pub document_id: Ulid,
    pub source_revision: Ulid,
    pub dataset_digest: String,
    pub selection_digest: String,
    /// The download route of the artifact on the node that owns the export job.
    pub artifact_url: Url,
    pub artifact_blake3: String,
    pub artifact_size: u64,
    pub issued_at: u64,
    pub expires_at: u64,
}

impl Signable for ExportGrant {
    const DOMAIN: &'static str = GRANT_DOMAIN;
    fn realm_id(&self) -> RealmId {
        self.source
    }
}

/// One selected object version with its plaintext hash and size.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectedVersion {
    pub version_id: Ulid,
    pub blake3: [u8; 32],
    pub size: u64,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TransferError {
    #[error(transparent)]
    Signature(#[from] FederationError),
    #[error("the intent or grant names another realm")]
    WrongRealm,
    #[error("the intent names a superseded or unknown descriptor")]
    StaleDescriptor,
    #[error("the browser secret does not match")]
    WrongSecret,
    #[error("the intent or grant lifetime is invalid or over")]
    BadLifetime,
    #[error("the grant is not bound to this intent")]
    Unbound,
    #[error("the artifact exceeds the intent's size limit")]
    TooLarge,
}

fn digest_of<T: Serialize>(value: &T) -> Result<String, FederationError> {
    let bytes = postcard::to_allocvec(value)
        .map_err(|error| FederationError::Encoding(error.to_string()))?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

/// Hex SHA-256 over the postcard encoding of the signed intent.
pub fn intent_digest(intent: &Signed<ImportIntent>) -> Result<String, FederationError> {
    digest_of(intent)
}

/// Hex SHA-256 over the source revision and the selected versions ordered by version id.
pub fn selection_digest(
    source_revision: Ulid,
    versions: &[SelectedVersion],
) -> Result<String, FederationError> {
    let mut versions = versions.to_vec();
    versions.sort_by_key(|version| version.version_id);
    digest_of(&(source_revision, versions))
}

/// The destination's idempotency key: source realm, document, dataset and selection digests,
/// destination bucket, prefix and metadata path.
pub fn import_key(
    grant: &ExportGrant,
    destination: &ImportDestination,
) -> Result<String, FederationError> {
    let digest = digest_of(&(
        grant.source,
        grant.document_id,
        &grant.dataset_digest,
        &grant.selection_digest,
        &destination.bucket,
        &destination.prefix,
        &destination.metadata_path,
    ))?;
    Ok(format!("federation-{digest}"))
}

fn check_lifetime(issued_at: u64, expires_at: u64, now: u64) -> Result<(), TransferError> {
    let lifetime = expires_at.checked_sub(issued_at);
    if !lifetime.is_some_and(|secs| secs > 0 && secs <= MAX_TRANSFER_SECS)
        || now >= expires_at
        || issued_at > now.saturating_add(MAX_HANDOFF_SKEW_SECS)
    {
        return Err(TransferError::BadLifetime);
    }
    Ok(())
}

/// Admits an intent at the destination `local` against its current descriptor; `secret` is
/// checked when the browser presents it.
pub fn check_intent(
    intent: &Signed<ImportIntent>,
    local: &RealmId,
    descriptor: &Signed<RealmDescriptor>,
    secret: Option<&[u8]>,
    now: u64,
) -> Result<(), TransferError> {
    let payload = &intent.payload;
    intent.verify(local)?;
    if descriptor.payload.realm_id != *local {
        return Err(TransferError::WrongRealm);
    }
    if payload.descriptor_digest != descriptor_digest(descriptor)? {
        return Err(TransferError::StaleDescriptor);
    }
    if secret.is_some_and(|secret| secret_nonce(secret) != payload.nonce) {
        return Err(TransferError::WrongSecret);
    }
    check_lifetime(payload.issued_at, payload.expires_at, now)
}

/// Admits a destination's intent at the source `local` with the destination's descriptor.
pub fn check_remote(
    intent: &Signed<ImportIntent>,
    descriptor: &Signed<RealmDescriptor>,
    local: &RealmId,
    now: u64,
) -> Result<(), TransferError> {
    let target = &descriptor.payload;
    descriptor.verify(&target.realm_id)?;
    if !valid_federation_url(&target.api_url) || target.realm_id == *local {
        return Err(TransferError::WrongRealm);
    }
    intent.verify(&target.realm_id)?;
    if intent.payload.descriptor_digest != descriptor_digest(descriptor)? {
        return Err(TransferError::StaleDescriptor);
    }
    check_lifetime(intent.payload.issued_at, intent.payload.expires_at, now)
}

/// Admits a grant signed by its source realm for `intent`.
pub fn check_grant(
    grant: &Signed<ExportGrant>,
    intent: &Signed<ImportIntent>,
    now: u64,
) -> Result<(), TransferError> {
    let payload = &grant.payload;
    grant.verify(&payload.source)?;
    if payload.audience != intent.payload.realm_id || payload.source == payload.audience {
        return Err(TransferError::WrongRealm);
    }
    if payload.intent_digest != intent_digest(intent)? {
        return Err(TransferError::Unbound);
    }
    if payload.artifact_size > intent.payload.max_bytes {
        return Err(TransferError::TooLarge);
    }
    check_lifetime(payload.issued_at, payload.expires_at, now)
}

/// Admits a grant at its own source realm for the artifact of `job_id`.
pub fn check_issued(
    grant: &Signed<ExportGrant>,
    local: &RealmId,
    job_id: Ulid,
    now: u64,
) -> Result<(), TransferError> {
    grant.verify(local)?;
    if grant.payload.export_job_id != job_id {
        return Err(TransferError::Unbound);
    }
    check_lifetime(grant.payload.issued_at, grant.payload.expires_at, now)
}

#[cfg(test)]
#[path = "transfer_tests.rs"]
mod tests;
