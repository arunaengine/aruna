//! Public ABE anchors and per-write envelopes, separate from existing storage records.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::blob::ArchiveKey;
use super::encryption::{BucketKeyRef, generate_key, public_key_of};
use crate::NodeId;
use crate::compute::{SecretBytes, SharedSecret};
use crate::key_seal::{SealedSecret, open_sealed, seal_to};
use crate::structs::identity::realm::RealmId;
use aruna_kpabe::{Attribute, MasterSecret, PublicParameters, frame_context, setup_from_seed};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;
use zeroize::Zeroizing;

pub const SETUP_PURPOSE: &[u8] = b"aruna bucket ABE v1";
pub const RECOVERY_PURPOSE: &[u8] = b"aruna object recovery v1";
pub const GRANT_PURPOSE: &[u8] = b"aruna ABE grant v1";
pub const MAX_ENVELOPE_BYTES: usize = aruna_kpabe::MAX_BYTES;

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum AbeError {
    #[error("the admitted ABE parameters do not match")]
    Parameters,
    #[error("the admitted encryption epoch changed")]
    Epoch,
    #[error("the envelope context does not match the stored version")]
    Context,
    #[error("the object key does not match this version")]
    WrongKey,
    #[error("this version has no envelope; bucket unlock is required")]
    Pending,
    #[error("the bucket is locked; an object key is required")]
    Required,
    #[error("the scope cannot be represented by a continuing encryption key")]
    Scope,
    #[error("the key request is stale")]
    Stale,
    #[error("the encryption record exceeds its limit")]
    Limit,
    #[error("encryption cryptography failed")]
    Crypto,
    #[error("the object version was not found")]
    Missing,
    #[error("encryption storage is unavailable")]
    Unavailable,
}

impl From<aruna_kpabe::Error> for AbeError {
    fn from(_: aruna_kpabe::Error) -> Self {
        Self::Crypto
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbeParameters {
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub key: BucketKeyRef,
    pub fingerprint: [u8; 32],
    pub parameters: Vec<u8>,
}

impl AbeParameters {
    pub fn context(&self) -> Result<Vec<u8>, AbeError> {
        setup_context(self.realm_id, self.node_id, self.key)
    }

    pub fn public(&self) -> Result<PublicParameters, AbeError> {
        PublicParameters::from_bytes(&self.parameters, &self.context()?, &self.fingerprint)
            .map_err(|_| AbeError::Parameters)
    }

    pub fn domain(&self) -> Result<Vec<u8>, AbeError> {
        Ok(blake3::hash(&self.context()?).as_bytes().to_vec())
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, AbeError> {
        self.public()?;
        postcard::to_allocvec(self).map_err(|_| AbeError::Parameters)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, AbeError> {
        if bytes.len() > aruna_kpabe::MAX_BYTES {
            return Err(AbeError::Limit);
        }
        let value: Self = postcard::from_bytes(bytes).map_err(|_| AbeError::Parameters)?;
        value.public()?;
        Ok(value)
    }

    pub fn recompute(&self, private: &SecretBytes) -> Result<MasterSecret, AbeError> {
        let (parameters, master) = derive_master(private, &self.context()?)?;
        if parameters.fingerprint() != &self.fingerprint || parameters.to_bytes() != self.parameters
        {
            return Err(AbeError::Parameters);
        }
        Ok(master)
    }
}

pub fn setup_context(realm: RealmId, node: NodeId, key: BucketKeyRef) -> Result<Vec<u8>, AbeError> {
    if key.generation == 0 {
        return Err(AbeError::Context);
    }
    Ok(frame_context(&[
        SETUP_PURPOSE,
        realm.as_bytes(),
        node.as_bytes(),
        &key.bucket_id.to_bytes(),
        &key.generation.to_be_bytes(),
    ])?)
}

pub fn derive_master(
    private: &SecretBytes,
    context: &[u8],
) -> Result<(PublicParameters, MasterSecret), AbeError> {
    let mut scalar = Zeroizing::new([0u8; 32]);
    if private.expose().len() != 32 {
        return Err(AbeError::Crypto);
    }
    scalar.copy_from_slice(private.expose());
    scalar[0] &= 248;
    scalar[31] &= 127;
    scalar[31] |= 64;
    let seed = aruna_kpabe::derive_seed(&scalar[..], b"aruna bucket ABE seed v1", context)?;
    Ok(setup_from_seed(seed.as_bytes(), context)?)
}

pub fn create_parameters(
    private: &SecretBytes,
    realm: RealmId,
    node: NodeId,
    key: BucketKeyRef,
) -> Result<AbeParameters, AbeError> {
    let (parameters, _) = derive_master(private, &setup_context(realm, node, key)?)?;
    Ok(AbeParameters {
        realm_id: realm,
        node_id: node,
        key,
        fingerprint: *parameters.fingerprint(),
        parameters: parameters.to_bytes(),
    })
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvelopeContext {
    pub parameters: AbeParameters,
    pub epoch: u64,
    pub object_key: String,
    pub write_id: Ulid,
    pub public_key: [u8; 32],
}

impl EnvelopeContext {
    pub fn bytes(&self) -> Result<Vec<u8>, AbeError> {
        if self.epoch == 0 || self.write_id == Ulid::nil() {
            return Err(AbeError::Context);
        }
        let p = &self.parameters;
        Ok(frame_context(&[
            b"aruna object envelope v1",
            p.realm_id.as_bytes(),
            p.node_id.as_bytes(),
            &p.key.bucket_id.to_bytes(),
            &p.key.generation.to_be_bytes(),
            &p.fingerprint,
            &self.epoch.to_be_bytes(),
            self.object_key.as_bytes(),
            &self.write_id.to_bytes(),
            &self.public_key,
        ])?)
    }

    pub fn attributes(&self) -> Result<Vec<Attribute>, AbeError> {
        let mut attributes = vec![
            Attribute::Domain(self.parameters.domain()?),
            Attribute::Epoch(self.epoch),
            Attribute::Key(self.object_key.as_bytes().to_vec()),
            Attribute::Write(self.write_id.to_bytes()),
            Attribute::Prefix(Vec::new()),
        ];
        for (index, _) in self.object_key.match_indices('/') {
            attributes.push(Attribute::Prefix(
                self.object_key.as_bytes()[..=index].to_vec(),
            ));
        }
        if attributes.len() > aruna_kpabe::MAX_ATTRIBUTES {
            return Err(AbeError::Limit);
        }
        Ok(attributes)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectEnvelope {
    pub context: EnvelopeContext,
    pub abe: Vec<u8>,
    pub recovery_enc: [u8; 32],
    pub recovery_ciphertext: Vec<u8>,
}

impl ObjectEnvelope {
    pub fn to_bytes(&self) -> Result<Vec<u8>, AbeError> {
        let bytes = postcard::to_allocvec(self).map_err(|_| AbeError::Context)?;
        if bytes.len() > MAX_ENVELOPE_BYTES {
            return Err(AbeError::Limit);
        }
        Ok(bytes)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, AbeError> {
        if bytes.len() > MAX_ENVELOPE_BYTES {
            return Err(AbeError::Limit);
        }
        let value: Self = postcard::from_bytes(bytes).map_err(|_| AbeError::Context)?;
        value.context.bytes()?;
        aruna_kpabe::Envelope::from_bytes(&value.context.parameters.public()?, &value.abe)?;
        if value.recovery_ciphertext.len() != 48 {
            return Err(AbeError::Context);
        }
        Ok(value)
    }

    pub fn anchored(&self, parameters: &AbeParameters, epoch: u64) -> Result<(), AbeError> {
        if &self.context.parameters != parameters {
            return Err(AbeError::Parameters);
        }
        if self.context.epoch != epoch {
            return Err(AbeError::Epoch);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnvelopePlan {
    pub parameters: AbeParameters,
    pub epoch: u64,
    pub write_id: Ulid,
    pub object_key: String,
    pub bucket_public: [u8; 32],
}

pub fn create_envelope(plan: EnvelopePlan) -> Result<(ObjectEnvelope, SharedSecret), AbeError> {
    let (public_key, private) = generate_key().map_err(|_| AbeError::Crypto)?;
    Ok((seal_envelope(plan, public_key, &private)?, private))
}

/// A new envelope for `plan` around the object key that `source`'s recovery wrap opens.
pub fn copy_envelope(
    source: &ObjectEnvelope,
    bucket: &SecretBytes,
    plan: EnvelopePlan,
) -> Result<ObjectEnvelope, AbeError> {
    let bucket: &[u8; 32] = bucket.expose().try_into().map_err(|_| AbeError::Crypto)?;
    let sealed = SealedSecret {
        enc: source.recovery_enc,
        ciphertext: source.recovery_ciphertext.clone(),
    };
    let opened = open_sealed(bucket, &sealed, RECOVERY_PURPOSE, &source.context.bytes()?)
        .map_err(|_| AbeError::WrongKey)?;
    let private = SharedSecret::new(SecretBytes::new(opened.to_vec()));
    check_object(private.bytes(), source)?;
    seal_envelope(plan, source.context.public_key, &private)
}

fn seal_envelope(
    plan: EnvelopePlan,
    public_key: [u8; 32],
    private: &SharedSecret,
) -> Result<ObjectEnvelope, AbeError> {
    let context = EnvelopeContext {
        parameters: plan.parameters,
        epoch: plan.epoch,
        object_key: plan.object_key,
        write_id: plan.write_id,
        public_key,
    };
    let bytes = context.bytes()?;
    let scalar: &[u8; 32] = private
        .bytes()
        .expose()
        .try_into()
        .map_err(|_| AbeError::Crypto)?;
    let abe = aruna_kpabe::seal(
        &context.parameters.public()?,
        &context.attributes()?,
        scalar,
        &bytes,
        &mut SysRng,
    )?
    .to_bytes()?;
    let recovery = seal_to(&plan.bucket_public, RECOVERY_PURPOSE, &bytes, scalar)
        .map_err(|_| AbeError::Crypto)?;
    let envelope = ObjectEnvelope {
        context,
        abe,
        recovery_enc: recovery.enc,
        recovery_ciphertext: recovery.ciphertext,
    };
    envelope.to_bytes()?;
    Ok(envelope)
}

pub fn check_object(private: &SecretBytes, envelope: &ObjectEnvelope) -> Result<(), AbeError> {
    if public_key_of(private) != Some(envelope.context.public_key) {
        return Err(AbeError::WrongKey);
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvelopeArchive {
    pub archive: ArchiveKey,
    pub location_key: Vec<u8>,
}

/// A same-bucket copy published while locked; its own envelope is written at the next unlock.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingCopy {
    /// A complete envelope of the shared archive; its recovery wrap opens the object key.
    pub source: ObjectEnvelope,
    pub archive: ArchiveKey,
}

impl PendingCopy {
    pub fn to_bytes(&self) -> Result<Vec<u8>, AbeError> {
        postcard::to_allocvec(self).map_err(|_| AbeError::Context)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, AbeError> {
        if bytes.len() > 2 * MAX_ENVELOPE_BYTES {
            return Err(AbeError::Limit);
        }
        let value: Self = postcard::from_bytes(bytes).map_err(|_| AbeError::Context)?;
        ObjectEnvelope::from_bytes(&value.source.to_bytes()?)?;
        Ok(value)
    }
}

/// Usage bytes charged for one version's stored envelope and archive mapping rows.
pub fn envelope_charge(envelope: &[u8], archive: &[u8]) -> u64 {
    // A pending mapping is charged its final size, so promotion adds no usage.
    let pending = postcard::from_bytes::<EnvelopeArchive>(archive)
        .ok()
        .filter(|mapping| mapping.location_key.is_empty());
    let archive = pending.map_or(archive.len(), |mapping| mapping.final_len());
    (envelope.len() + archive) as u64
}

impl EnvelopeArchive {
    /// Encoded size once a pending Pithos archive's location key is known.
    fn final_len(&self) -> usize {
        let digest = [0; 32];
        let class = super::format::EncodingClass::Pithos { digest };
        let key = super::blob::BlobLocationKey::new(digest, class, self.archive.backend.clone());
        let filled = Self {
            archive: self.archive.clone(),
            location_key: key.to_bytes(),
        };
        postcard::to_allocvec(&filled).map_or(0, |bytes| bytes.len())
    }
}

pub use getrandom::SysRng;

#[derive(Debug, PartialEq)]
pub enum AbeEffect {
    Issue(super::abe_access::GrantContext),
    Parameters {
        realm: RealmId,
        node: NodeId,
        key: BucketKeyRef,
        private: SharedSecret,
    },
    Write {
        plan: EnvelopePlan,
        resolved: super::blob::ResolvedBackend,
        bucket: String,
        key: String,
        created_by: crate::UserId,
        blob: crate::stream::BackendStream<Result<bytes::Bytes, crate::stream::StreamError>>,
        size: Option<u64>,
    },
    Admit {
        envelope: ObjectEnvelope,
        archive: ArchiveKey,
        private: SharedSecret,
    },
    /// Creates an object key and its envelope without writing bytes.
    Envelope(EnvelopePlan),
    /// Envelopes `source`'s object key under its parameters for another write with the unlocked
    /// bucket key; a locked key fails with `AbeError::Required`.
    Copy {
        source: ObjectEnvelope,
        epoch: u64,
        write_id: Ulid,
        object_key: String,
    },
}

#[derive(Debug, PartialEq)]
pub enum AbeEvent {
    Grant(super::abe_access::KeyGrant),
    Parameters(AbeParameters),
    Written {
        location: super::blob::BackendLocation,
        envelope: ObjectEnvelope,
    },
    Envelope(ObjectEnvelope),
}
