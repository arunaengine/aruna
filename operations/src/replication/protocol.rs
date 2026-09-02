use crate::s3::get_object::MAX_AUTO_ADVANCES;
use aruna_blob::hash::Hasher;
use aruna_core::errors::ConversionError;
use aruna_core::id::NodeId;
use aruna_core::structs::checksum::ChecksumAlgorithm;
use aruna_core::structs::{
    ArunaArn, AuthContext, BackendLocation, CopyOrigin, MAX_POLICY_REF_INPUT,
    MultipartChecksumType, MultipartObjectPart, MultipartObjectSummary, PlacementPolicyRef,
    PlacementSubject, RealmId, ReplicationItemKind, ReplicationNegotiationResult, SourceMetadata,
    VersionSourceBinding, VersionedObjectArn,
};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use ulid::Ulid;

const VERSION_REPLICATION_MAGIC: &[u8; 4] = b"vrp1";

pub const MAX_REPLICATION_PARTS: usize = 10_000;
pub const MAX_REPLICATION_SOURCES: usize = 4;
pub const MAX_REPLICATION_METADATA: usize = 128;
pub const MAX_REPLICATION_HASHES: usize = 7;
pub const MAX_REPLICATION_KEY_BYTES: usize = 128;
pub const MAX_REPLICATION_VALUE_BYTES: usize = 4 * 1024;
pub const MAX_REPLICATION_HASH_BYTES: usize = 64;
pub const MAX_REPLICATION_MANIFEST_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_REPLICATION_MANIFEST_WORK: usize = 4 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReferenceAdvance {
    pub generation: u64,
    pub predecessor: Ulid,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VersionReplicationManifest {
    pub bucket: String,
    pub key: String,
    pub version_id: Ulid,
    pub group_id: aruna_core::types::GroupId,
    pub kind: ReplicationItemKind,
    pub created_at: std::time::SystemTime,
    pub created_by: aruna_core::types::UserId,
    pub current_version: bool,
    pub current_version_generation: Option<u64>,
    pub auth_context: AuthContext,
    pub blob: Option<MaterializedBlobInfo>,
    pub source: Option<VersionSourceBinding>,
    pub multipart: Option<MultipartObjectReplicationMetadata>,
    pub reference_intent: bool,
    pub origin: Option<SyncOrigin>,
    pub upstream_sources: Vec<ArunaArn>,
    pub writer_auth_context: Option<AuthContext>,
    pub reference_metadata: Option<SourceMetadata>,
    pub metadata: HashMap<String, String>,
    #[serde(skip)]
    pub reference_advance: Option<ReferenceAdvance>,
    /// Automatic advance count of the reference this manifest carries, so repair
    /// and snapshot replication preserve the cap instead of resetting it.
    pub reference_advance_count: Option<u16>,
    /// Refs sealed on the replicated version. The target reconstructs the same
    /// governed version, so replication never relaxes an attachment.
    pub placement_policies: Vec<PlacementPolicyRef>,
}

impl VersionReplicationManifest {
    /// A manifest the target reconstructs as a reference version.
    pub fn is_reference(&self) -> bool {
        self.reference_intent && self.kind == ReplicationItemKind::Materialized
    }

    pub(crate) fn validate(&self) -> Result<(), ConversionError> {
        let mut budget = ManifestBudget::default();
        if PlacementPolicyRef::canonical_set(&self.placement_policies)
            .is_ok_and(|canonical| canonical == self.placement_policies)
        {
            budget.add(0, self.placement_policies.len())?;
        } else {
            return Err(ConversionError::NonCanonicalPolicyRefs);
        }
        check_text(&mut budget, &self.bucket, MAX_REPLICATION_VALUE_BYTES)?;
        check_text(&mut budget, &self.key, MAX_REPLICATION_VALUE_BYTES)?;
        check_map(&mut budget, &self.metadata)?;

        if self.upstream_sources.len() > MAX_REPLICATION_SOURCES {
            return Err(ConversionError::FromStrError(
                "replication manifest source count exceeded".to_string(),
            ));
        }
        for source in &self.upstream_sources {
            check_text(&mut budget, &source.path, MAX_REPLICATION_VALUE_BYTES)?;
        }

        if let Some(source) = &self.source {
            let descriptor = &source.descriptor;
            if descriptor.public_config.len() > MAX_REPLICATION_METADATA
                || descriptor.capabilities.len() > MAX_REPLICATION_METADATA
            {
                return Err(ConversionError::FromStrError(
                    "replication manifest source count exceeded".to_string(),
                ));
            }
            check_map(&mut budget, &descriptor.public_config)?;
            check_text(
                &mut budget,
                &descriptor.source_path,
                MAX_REPLICATION_VALUE_BYTES,
            )?;
            if let Some(selector) = &descriptor.version_selector {
                check_text(&mut budget, selector, MAX_REPLICATION_VALUE_BYTES)?;
            }
            for capability in &descriptor.capabilities {
                check_text(&mut budget, capability, MAX_REPLICATION_VALUE_BYTES)?;
            }
        }

        if let Some(reference) = &self.reference_metadata {
            if let Some(content_type) = &reference.content_type {
                check_text(&mut budget, content_type, MAX_REPLICATION_VALUE_BYTES)?;
            }
            if let Some(etag) = &reference.etag {
                check_text(&mut budget, etag, MAX_REPLICATION_VALUE_BYTES)?;
            }
        }

        if let Some(blob) = &self.blob {
            check_location(&mut budget, &blob.location)?;
        }

        if let Some(multipart) = &self.multipart {
            if multipart.parts.len() > MAX_REPLICATION_PARTS
                || multipart.summary.part_count != multipart.parts.len()
            {
                return Err(ConversionError::FromStrError(
                    "replication manifest part count is invalid".to_string(),
                ));
            }
            if multipart.summary.composite_hashes.len() > MAX_REPLICATION_HASHES - 1 {
                return Err(ConversionError::FromStrError(
                    "replication manifest hash count exceeded".to_string(),
                ));
            }
            for (name, digest) in &multipart.summary.composite_hashes {
                check_hash(&mut budget, name, digest)?;
            }

            let mut part_numbers = HashSet::with_capacity(multipart.parts.len());
            for part in &multipart.parts {
                if !part_numbers.insert(part.part_number) {
                    return Err(ConversionError::FromStrError(
                        "replication manifest part number is duplicated".to_string(),
                    ));
                }
                budget.add(0, 1)?;
                if part.hashes.len() > MAX_REPLICATION_HASHES {
                    return Err(ConversionError::FromStrError(
                        "replication manifest hash count exceeded".to_string(),
                    ));
                }
                for (name, digest) in &part.hashes {
                    check_hash(&mut budget, name, digest)?;
                }
            }
        }

        if let Some(advance) = &self.reference_advance
            && (!self.current_version
                || self.current_version_generation != Some(advance.generation)
                || advance.generation == 0
                || advance.predecessor == self.version_id
                || !self.reference_intent
                || self.kind != ReplicationItemKind::Materialized
                || self.blob.is_some()
                || self.multipart.is_some()
                || self.source.is_none()
                || self.reference_metadata.is_none()
                || self.origin.is_none()
                || self.writer_auth_context.is_some())
        {
            return Err(ConversionError::FromStrError(
                "replication manifest reference advance is invalid".to_string(),
            ));
        }

        let count_valid = match self.reference_advance_count {
            Some(count) => self.is_reference() && count <= MAX_AUTO_ADVANCES,
            None => !self.is_reference(),
        };
        if !count_valid {
            return Err(ConversionError::FromStrError(
                "replication manifest reference advance count is invalid".to_string(),
            ));
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncOrigin {
    pub relationship_id: Ulid,
    pub hop_count: u8,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MaterializedBlobInfo {
    pub hash: [u8; 32],
    pub size: u64,
    pub compressed: bool,
    pub encrypted: bool,
    pub location: BackendLocation,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BaoReadTarget {
    ExactVersion(VersionedObjectArn),
    Blake3([u8; 32]),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BaoReadRequest {
    pub auth_context: AuthContext,
    pub realm_id: aruna_core::structs::RealmId,
    pub target: BaoReadTarget,
    pub expected_blake3: Option<[u8; 32]>,
    pub metadata_only: bool,
    /// Where the bytes would land. Absent means the caller is not asking for a
    /// managed copy, so the source refuses anything governed.
    pub destination: Option<PlacementSubject>,
    /// Refs the requester has already resolved. Echoing one is never authority:
    /// the source evaluates the destination independently.
    pub known_refs: Vec<PlacementPolicyRef>,
}

impl BaoReadRequest {
    /// Bounds the destination facts before they are evaluated or stored.
    pub fn validate(&self) -> Result<(), ConversionError> {
        if self.known_refs.len() > MAX_POLICY_REF_INPUT {
            return Err(ConversionError::PlacementPolicyError(
                aruna_core::structs::PlacementPolicyError::RefCount,
            ));
        }
        if PlacementPolicyRef::canonical_set(&self.known_refs)? != self.known_refs {
            return Err(ConversionError::NonCanonicalPolicyRefs);
        }
        if let Some(destination) = self.destination.as_ref() {
            destination.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BaoReadRefusal {
    RealmPeerDenied,
    InvalidTarget,
    NotFound,
    ReadDenied,
    HashMismatch,
    BackendFailure,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MultipartObjectReplicationMetadata {
    pub summary: MultipartObjectSummary,
    pub parts: Vec<MultipartObjectPart>,
    pub checksum_type: MultipartChecksumType,
}

impl<'de> Deserialize<'de> for MultipartObjectReplicationMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct WireMetadata {
            summary: MultipartObjectSummary,
            parts: Vec<MultipartObjectPart>,
            checksum_type: MultipartChecksumType,
        }

        let metadata = WireMetadata::deserialize(deserializer)?;
        Ok(Self {
            summary: metadata.summary,
            parts: metadata.parts,
            checksum_type: metadata.checksum_type,
        })
    }
}

#[derive(Default)]
struct ManifestBudget {
    bytes: usize,
    work: usize,
}

impl ManifestBudget {
    fn add(&mut self, bytes: usize, work: usize) -> Result<(), ConversionError> {
        self.bytes = self.bytes.checked_add(bytes).ok_or_else(|| {
            ConversionError::FromStrError("replication manifest byte budget overflow".to_string())
        })?;
        self.work = self.work.checked_add(work).ok_or_else(|| {
            ConversionError::FromStrError("replication manifest work budget overflow".to_string())
        })?;
        if self.bytes > MAX_REPLICATION_MANIFEST_BYTES || self.work > MAX_REPLICATION_MANIFEST_WORK
        {
            return Err(ConversionError::FromStrError(
                "replication manifest budget exceeded".to_string(),
            ));
        }
        Ok(())
    }
}

fn check_text(
    budget: &mut ManifestBudget,
    value: &str,
    limit: usize,
) -> Result<(), ConversionError> {
    if value.len() > limit {
        return Err(ConversionError::FromStrError(
            "replication manifest entry is too large".to_string(),
        ));
    }
    let work = value.len().checked_add(1).ok_or_else(|| {
        ConversionError::FromStrError("replication manifest work budget overflow".to_string())
    })?;
    budget.add(value.len(), work)
}

fn check_map(
    budget: &mut ManifestBudget,
    map: &HashMap<String, String>,
) -> Result<(), ConversionError> {
    if map.len() > MAX_REPLICATION_METADATA {
        return Err(ConversionError::FromStrError(
            "replication manifest metadata count exceeded".to_string(),
        ));
    }
    for (key, value) in map {
        check_text(budget, key, MAX_REPLICATION_KEY_BYTES)?;
        check_text(budget, value, MAX_REPLICATION_VALUE_BYTES)?;
    }
    Ok(())
}

fn check_hash(
    budget: &mut ManifestBudget,
    name: &str,
    digest: &[u8],
) -> Result<(), ConversionError> {
    let expected = match name {
        aruna_core::structs::checksum::HASH_BLAKE3 => 32,
        aruna_core::structs::checksum::HASH_MD5 => 16,
        aruna_core::structs::checksum::HASH_SHA1 => 20,
        aruna_core::structs::checksum::HASH_SHA256 => 32,
        aruna_core::structs::checksum::HASH_CRC32 | aruna_core::structs::checksum::HASH_CRC32C => 4,
        aruna_core::structs::checksum::HASH_CRC64NVME => 8,
        _ => {
            return Err(ConversionError::FromStrError(
                "replication manifest hash algorithm is unsupported".to_string(),
            ));
        }
    };
    if digest.len() != expected || digest.len() > MAX_REPLICATION_HASH_BYTES {
        return Err(ConversionError::FromStrError(
            "replication manifest hash length is invalid".to_string(),
        ));
    }
    check_text(budget, name, MAX_REPLICATION_KEY_BYTES)?;
    budget.add(digest.len(), digest.len())
}

fn check_location(
    budget: &mut ManifestBudget,
    location: &BackendLocation,
) -> Result<(), ConversionError> {
    match &location.backend {
        aruna_core::structs::BackendRef::Node(name) => {
            check_text(budget, name, MAX_REPLICATION_VALUE_BYTES)?;
        }
        aruna_core::structs::BackendRef::Group(_) => {}
    }
    if let Some(storage_class) = &location.storage_class {
        check_text(budget, storage_class, MAX_REPLICATION_VALUE_BYTES)?;
    }
    check_text(budget, &location.root, MAX_REPLICATION_VALUE_BYTES)?;
    check_text(
        budget,
        &location.storage_bucket,
        MAX_REPLICATION_VALUE_BYTES,
    )?;
    check_text(budget, &location.backend_path, MAX_REPLICATION_VALUE_BYTES)?;
    if location.hashes.len() > MAX_REPLICATION_HASHES {
        return Err(ConversionError::FromStrError(
            "replication manifest hash count exceeded".to_string(),
        ));
    }
    for (name, digest) in &location.hashes {
        check_hash(budget, name, digest)?;
    }
    Ok(())
}

fn hashes_from_parts(parts: &[MultipartObjectPart]) -> HashMap<String, Vec<u8>> {
    let mut composite_hashes = HashMap::new();
    for algorithm in [
        ChecksumAlgorithm::Md5,
        ChecksumAlgorithm::Sha1,
        ChecksumAlgorithm::Sha256,
        ChecksumAlgorithm::Crc32,
        ChecksumAlgorithm::Crc32c,
        ChecksumAlgorithm::Crc64Nvme,
    ] {
        let mut combined = Vec::new();
        for part in parts {
            let Some(digest) = part.hashes.get(algorithm.hash_key()) else {
                combined.clear();
                break;
            };
            combined.extend_from_slice(digest);
        }
        if combined.is_empty() {
            continue;
        }

        let hashes = Hasher::new_with_bytes(&combined).finalize();
        let digest = match algorithm {
            ChecksumAlgorithm::Md5 => hashes.md5.to_vec(),
            ChecksumAlgorithm::Sha1 => hashes.sha1.to_vec(),
            ChecksumAlgorithm::Sha256 => hashes.sha256.to_vec(),
            ChecksumAlgorithm::Crc32 => hashes.crc32.to_vec(),
            ChecksumAlgorithm::Crc32c => hashes.crc32c.to_vec(),
            ChecksumAlgorithm::Crc64Nvme => hashes.crc64nvme.to_vec(),
        };
        composite_hashes.insert(algorithm.hash_key().to_string(), digest);
    }
    composite_hashes
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum VersionReplicationMessage {
    VersionManifest(VersionReplicationManifest),
    VersionNegotiationResponse(ReplicationNegotiationResult),
    VersionApplyComplete,
    VersionApplyRejected(String),
    BaoReadRequest(BaoReadRequest),
    BaoReadAccepted {
        size: u64,
        blake3: [u8; 32],
    },
    BaoReadRefused(BaoReadRefusal),
    LocationSummaryRequest(LocationSummaryRequest),
    LocationSummaryResponse(LocationSummary),
    /// The asking user has no read access to the bucket this node holds the
    /// copy under, so the caller learns nothing except that it was refused.
    LocationSummaryDenied,
    ReferenceAdvance {
        manifest: VersionReplicationManifest,
        advance: ReferenceAdvance,
    },
    /// The requester has not resolved every rule this copy carries. Sent only
    /// after authorization, so object existence and policy ids stay private.
    PlacementPolicyRequired {
        refs: Vec<PlacementPolicyRef>,
    },
    /// The source evaluated the authenticated destination and refused it.
    PlacementPolicyDenied {
        policy_ids: Vec<Ulid>,
    },
}

/// Read-only question a node asks a peer: do you hold this version, and on
/// what storage? Never mutates the peer.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LocationSummaryRequest {
    pub realm_id: RealmId,
    pub bucket: String,
    pub key: String,
    pub version_id: Option<Ulid>,
    pub auth_context: AuthContext,
}

/// What a copy sits on. Node-managed copies carry the storage class only: the
/// operator's backend names stay out of object-scoped answers.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum LocationCopyStorage {
    NodeManaged {
        storage_class: Option<String>,
    },
    GroupBackend {
        backend_id: Ulid,
        name: Option<String>,
    },
}

/// Whether the answering node considers the copy it holds admissible under the
/// rules the version carries. It names no policy either way.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum CopyCompliance {
    /// The node evaluated the copy and would serve it.
    Allowed,
    /// The node holds the copy but is not serving it, because it no longer
    /// matches the rules the copy carries.
    Quarantined,
    /// The node reported no verdict, so nothing is claimed either way.
    #[default]
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LocationSummary {
    /// The version the answering node resolved; `None` means it knows none.
    pub version_id: Option<Ulid>,
    pub held: bool,
    pub storage: Option<LocationCopyStorage>,
    /// Why the answering node holds the copy, from its own registration.
    pub origin: CopyOrigin,
    pub compliance: CopyCompliance,
    /// Whether the resolved version carries stored bytes at all. A delete
    /// marker or a reference-only version never will, anywhere.
    pub materialized: bool,
    /// Group owning the answering node's bucket record, so a routed resolve can
    /// report the object without a second authorization round trip.
    pub group_id: Option<Ulid>,
    pub blob_size: Option<u64>,
    pub hashes: BTreeMap<String, Vec<u8>>,
}

impl LocationSummary {
    pub fn absent() -> Self {
        Self {
            version_id: None,
            held: false,
            storage: None,
            origin: CopyOrigin::Unknown,
            compliance: CopyCompliance::Unknown,
            materialized: false,
            group_id: None,
            blob_size: None,
            hashes: BTreeMap::new(),
        }
    }
}

impl VersionReplicationMessage {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        let mut bytes = VERSION_REPLICATION_MAGIC.to_vec();
        bytes.extend(postcard::to_allocvec(self)?);
        Ok(bytes)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        let payload = bytes
            .strip_prefix(VERSION_REPLICATION_MAGIC)
            .ok_or_else(|| {
                ConversionError::FromStrError(
                    "invalid version replication message prefix".to_string(),
                )
            })?;
        let mut message: Self = postcard::from_bytes(payload)?;
        let manifest = match &mut message {
            Self::VersionManifest(manifest) => Some(manifest),
            Self::ReferenceAdvance { manifest, advance } => {
                manifest.reference_advance = Some(*advance);
                Some(manifest)
            }
            // A hostile peer must not decode into an unbounded ref set.
            Self::BaoReadRequest(request) => {
                request.validate()?;
                None
            }
            Self::PlacementPolicyRequired { refs } => {
                if PlacementPolicyRef::canonical_set(refs)? != *refs {
                    return Err(ConversionError::NonCanonicalPolicyRefs);
                }
                None
            }
            Self::PlacementPolicyDenied { policy_ids } => {
                if policy_ids.len() > MAX_POLICY_REF_INPUT {
                    return Err(ConversionError::PlacementPolicyError(
                        aruna_core::structs::PlacementPolicyError::RefCount,
                    ));
                }
                None
            }
            _ => None,
        };
        if let Some(manifest) = manifest {
            manifest.validate()?;
            if let Some(multipart) = manifest.multipart.as_mut() {
                multipart.summary.composite_hashes = hashes_from_parts(&multipart.parts);
            }
        }
        Ok(message)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ReplicationMode {
    Live,
    OnDemand,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VersionReplicationRequest {
    pub bucket: String,
    pub key: String,
    pub version_id: Ulid,
    pub source_group_id: aruna_core::types::GroupId,
    pub target_node_id: NodeId,
    pub auth_context: AuthContext,
    pub mode: ReplicationMode,
}

#[cfg(test)]
mod tests {
    use super::{
        BaoReadRefusal, BaoReadRequest, BaoReadTarget, MAX_REPLICATION_HASH_BYTES,
        MAX_REPLICATION_PARTS, MAX_REPLICATION_SOURCES, MAX_REPLICATION_VALUE_BYTES,
        MaterializedBlobInfo, MultipartObjectReplicationMetadata, ReferenceAdvance, SyncOrigin,
        VersionReplicationManifest, VersionReplicationMessage,
    };
    use aruna_blob::hash::Hasher;
    use aruna_core::UserId;
    use aruna_core::errors::ConversionError;
    use aruna_core::structs::checksum::HASH_SHA256;
    use aruna_core::structs::{
        ArunaArn, AuthContext, BackendLocation, BackendRef, MultipartChecksumType,
        MultipartObjectPart, MultipartObjectSummary, PlacementPolicyRef, PortableSourceDescriptor,
        RealmId, ReplicationItemKind, SourceConnectorKind, SourceMetadata, StagingStrategy,
        VersionSourceBinding,
    };
    use std::collections::HashMap;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn test_realm_id() -> RealmId {
        RealmId::from_bytes([7u8; 32])
    }

    fn test_user_id() -> UserId {
        UserId::nil(test_realm_id())
    }

    fn make_manifest() -> VersionReplicationManifest {
        VersionReplicationManifest {
            bucket: "bucket".to_string(),
            key: "path/file.txt".to_string(),
            version_id: Ulid::generate(),
            group_id: Ulid::generate(),
            kind: ReplicationItemKind::DeleteMarker,
            created_at: SystemTime::now(),
            created_by: test_user_id(),
            current_version: true,
            current_version_generation: Some(1),
            auth_context: AuthContext {
                user_id: test_user_id(),
                realm_id: test_realm_id(),
                path_restrictions: None,
                session: None,
            },
            blob: None,
            source: None,
            multipart: None,
            reference_intent: false,
            origin: None,
            upstream_sources: Vec::new(),
            writer_auth_context: None,
            reference_metadata: None,
            metadata: HashMap::new(),
            reference_advance: None,
            reference_advance_count: None,
            placement_policies: Vec::new(),
        }
    }

    fn make_advance() -> VersionReplicationManifest {
        let mut manifest = make_manifest();
        manifest.kind = ReplicationItemKind::Materialized;
        manifest.current_version_generation = Some(2);
        manifest.source = Some(VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::Http,
                public_config: HashMap::new(),
                source_path: "path/file.txt".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
            },
            connector_id: Some(Ulid::generate()),
        });
        manifest.reference_intent = true;
        manifest.reference_metadata = Some(SourceMetadata {
            content_length: 42,
            content_type: None,
            etag: None,
            last_modified: None,
            source_version: None,
        });
        manifest.origin = Some(SyncOrigin {
            relationship_id: Ulid::generate(),
            hop_count: 0,
        });
        manifest.reference_advance = Some(ReferenceAdvance {
            generation: 2,
            predecessor: Ulid::from(1u128),
        });
        manifest.reference_advance_count = Some(1);
        manifest
    }

    fn make_blob() -> MaterializedBlobInfo {
        let location = BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/data".to_string(),
            storage_bucket: "bucket".to_string(),
            backend_path: "path/file.txt".to_string(),
            ulid: Ulid::generate(),
            compressed: false,
            encrypted: false,
            created_by: test_user_id(),
            created_at: SystemTime::now(),
            staging: false,
            partial: false,
            blob_size: 42,
            hashes: HashMap::new(),
        };
        MaterializedBlobInfo {
            hash: [1u8; 32],
            size: 42,
            compressed: false,
            encrypted: false,
            location,
        }
    }

    #[test]
    fn bao_frames_roundtrip() {
        let request = VersionReplicationMessage::BaoReadRequest(BaoReadRequest {
            auth_context: AuthContext {
                user_id: test_user_id(),
                realm_id: test_realm_id(),
                path_restrictions: None,
                session: None,
            },
            realm_id: test_realm_id(),
            target: BaoReadTarget::Blake3([8u8; 32]),
            expected_blake3: Some([8u8; 32]),
            metadata_only: false,
            destination: None,
            known_refs: Vec::new(),
        });
        let accepted = VersionReplicationMessage::BaoReadAccepted {
            size: 42,
            blake3: [8u8; 32],
        };
        let refused = VersionReplicationMessage::BaoReadRefused(BaoReadRefusal::ReadDenied);

        for message in [request, accepted, refused] {
            assert_eq!(
                VersionReplicationMessage::from_bytes(&message.to_bytes().unwrap()).unwrap(),
                message
            );
        }
    }

    #[test]
    fn version_replication_messages_roundtrip_with_magic_prefix() {
        let mut manifest = make_manifest();
        manifest.origin = Some(SyncOrigin {
            relationship_id: Ulid::from(7u128),
            hop_count: 3,
        });
        manifest.upstream_sources.push(
            ArunaArn::s3_bucket(
                test_realm_id(),
                iroh::SecretKey::from_bytes(&[3u8; 32]).public(),
                "source",
            )
            .unwrap(),
        );
        manifest.reference_metadata = Some(aruna_core::structs::SourceMetadata {
            content_length: 42,
            content_type: Some("text/plain".to_string()),
            etag: Some("etag-1".to_string()),
            last_modified: Some(SystemTime::UNIX_EPOCH),
            source_version: None,
        });
        manifest
            .metadata
            .insert("mtime".to_string(), "1753272000.123456789".to_string());
        let message = VersionReplicationMessage::VersionManifest(manifest);
        let bytes = message.to_bytes().unwrap();

        assert_eq!(
            VersionReplicationMessage::from_bytes(&bytes).unwrap(),
            message
        );
    }

    #[test]
    fn version_replication_messages_reject_invalid_prefix() {
        let message = VersionReplicationMessage::VersionManifest(make_manifest());
        let mut bytes = message.to_bytes().unwrap();
        bytes[0] = b'x';

        assert_eq!(
            VersionReplicationMessage::from_bytes(&bytes).unwrap_err(),
            ConversionError::FromStrError("invalid version replication message prefix".to_string())
        );
    }

    #[test]
    fn rejects_noncanonical_refs() {
        // A governed manifest must carry the one canonical ref set or nothing.
        let mut manifest = make_manifest();
        manifest.placement_policies = vec![
            PlacementPolicyRef {
                policy_id: Ulid::from_bytes([6u8; 16]),
                digest: [6u8; 32],
            },
            PlacementPolicyRef {
                policy_id: Ulid::from_bytes([1u8; 16]),
                digest: [1u8; 32],
            },
        ];

        assert!(manifest.validate().is_err());
        manifest.placement_policies.reverse();
        assert!(manifest.validate().is_ok());
    }

    #[test]
    fn rejects_truncated_manifest() {
        let mut bytes = VersionReplicationMessage::VersionManifest(make_manifest())
            .to_bytes()
            .unwrap();
        bytes.pop();

        assert!(VersionReplicationMessage::from_bytes(&bytes).is_err());
    }

    #[test]
    fn advance_roundtrip() {
        let manifest = make_advance();
        let message = VersionReplicationMessage::ReferenceAdvance {
            advance: manifest.reference_advance.unwrap(),
            manifest,
        };

        assert_eq!(
            VersionReplicationMessage::from_bytes(&message.to_bytes().unwrap()).unwrap(),
            message
        );
    }

    #[test]
    fn rejects_invalid_advances() {
        let invalidators: [fn(&mut VersionReplicationManifest); 12] = [
            |manifest| manifest.current_version = false,
            |manifest| manifest.current_version_generation = None,
            |manifest| {
                manifest.current_version_generation = Some(0);
                manifest.reference_advance.as_mut().unwrap().generation = 0;
            },
            |manifest| {
                let version_id = manifest.version_id;
                manifest.reference_advance.as_mut().unwrap().predecessor = version_id;
            },
            |manifest| manifest.reference_intent = false,
            |manifest| manifest.kind = ReplicationItemKind::DeleteMarker,
            |manifest| manifest.blob = Some(make_blob()),
            |manifest| {
                manifest.multipart = Some(MultipartObjectReplicationMetadata {
                    summary: MultipartObjectSummary {
                        checksum_type: MultipartChecksumType::Composite,
                        part_count: 0,
                        composite_hashes: HashMap::new(),
                    },
                    parts: Vec::new(),
                    checksum_type: MultipartChecksumType::Composite,
                });
            },
            |manifest| manifest.source = None,
            |manifest| manifest.reference_metadata = None,
            |manifest| manifest.origin = None,
            |manifest| manifest.writer_auth_context = Some(manifest.auth_context.clone()),
        ];

        for invalidate in invalidators {
            let mut manifest = make_advance();
            invalidate(&mut manifest);
            let message = VersionReplicationMessage::ReferenceAdvance {
                advance: manifest.reference_advance.unwrap(),
                manifest,
            };
            assert_eq!(
                VersionReplicationMessage::from_bytes(&message.to_bytes().unwrap()).unwrap_err(),
                ConversionError::FromStrError(
                    "replication manifest reference advance is invalid".to_string()
                )
            );
        }
    }

    // The cap only survives replication if every reference manifest carries a
    // count in range and no other manifest carries one at all.
    #[test]
    fn rejects_invalid_counts() {
        let mut missing = make_advance();
        missing.reference_advance_count = None;
        let mut over_cap = make_advance();
        over_cap.reference_advance_count = Some(super::MAX_AUTO_ADVANCES + 1);
        let mut not_reference = make_manifest();
        not_reference.reference_advance_count = Some(0);

        for manifest in [missing, over_cap, not_reference] {
            let bytes = VersionReplicationMessage::VersionManifest(manifest)
                .to_bytes()
                .unwrap();
            assert_eq!(
                VersionReplicationMessage::from_bytes(&bytes).unwrap_err(),
                ConversionError::FromStrError(
                    "replication manifest reference advance count is invalid".to_string()
                )
            );
        }
    }

    #[test]
    fn rejects_source_count() {
        let source = ArunaArn::s3_bucket(
            test_realm_id(),
            iroh::SecretKey::from_bytes(&[3u8; 32]).public(),
            "source",
        )
        .unwrap();
        let mut manifest = make_manifest();
        manifest.upstream_sources = vec![source; MAX_REPLICATION_SOURCES + 1];
        let bytes = VersionReplicationMessage::VersionManifest(manifest)
            .to_bytes()
            .unwrap();

        assert!(VersionReplicationMessage::from_bytes(&bytes).is_err());
    }

    #[test]
    fn rejects_metadata_size() {
        let mut manifest = make_manifest();
        manifest.metadata.insert(
            "metadata".to_string(),
            "x".repeat(MAX_REPLICATION_VALUE_BYTES + 1),
        );
        let bytes = VersionReplicationMessage::VersionManifest(manifest)
            .to_bytes()
            .unwrap();

        assert!(VersionReplicationMessage::from_bytes(&bytes).is_err());
    }

    #[test]
    fn rejects_part_count() {
        let part = MultipartObjectPart {
            part_number: 1,
            size: 5,
            hashes: HashMap::from([(HASH_SHA256.to_string(), vec![1u8; 32])]),
        };
        let mut manifest = make_manifest();
        manifest.multipart = Some(MultipartObjectReplicationMetadata {
            summary: MultipartObjectSummary {
                checksum_type: MultipartChecksumType::Composite,
                part_count: MAX_REPLICATION_PARTS + 1,
                composite_hashes: HashMap::new(),
            },
            parts: vec![part; MAX_REPLICATION_PARTS + 1],
            checksum_type: MultipartChecksumType::Composite,
        });
        let bytes = VersionReplicationMessage::VersionManifest(manifest)
            .to_bytes()
            .unwrap();

        assert!(VersionReplicationMessage::from_bytes(&bytes).is_err());
    }

    #[test]
    fn rejects_hash_size() {
        let part = MultipartObjectPart {
            part_number: 1,
            size: 5,
            hashes: HashMap::from([(
                HASH_SHA256.to_string(),
                vec![1u8; MAX_REPLICATION_HASH_BYTES + 1],
            )]),
        };
        let mut manifest = make_manifest();
        manifest.multipart = Some(MultipartObjectReplicationMetadata {
            summary: MultipartObjectSummary {
                checksum_type: MultipartChecksumType::Composite,
                part_count: 1,
                composite_hashes: HashMap::new(),
            },
            parts: vec![part],
            checksum_type: MultipartChecksumType::Composite,
        });
        let bytes = VersionReplicationMessage::VersionManifest(manifest)
            .to_bytes()
            .unwrap();

        assert!(VersionReplicationMessage::from_bytes(&bytes).is_err());
    }

    #[test]
    fn preserves_vrp1_hashes() {
        let part_digests = [vec![1u8; 32], vec![2u8; 32]];
        let parts = part_digests
            .iter()
            .enumerate()
            .map(|(index, digest)| MultipartObjectPart {
                part_number: u16::try_from(index + 1).unwrap(),
                size: 5,
                hashes: HashMap::from([(HASH_SHA256.to_string(), digest.clone())]),
            })
            .collect::<Vec<_>>();
        let combined = part_digests.concat();
        let composite_sha256 = Hasher::new_with_bytes(&combined).finalize().sha256.to_vec();
        let mut manifest = make_manifest();
        manifest.multipart = Some(MultipartObjectReplicationMetadata {
            summary: MultipartObjectSummary {
                checksum_type: MultipartChecksumType::Composite,
                part_count: parts.len(),
                composite_hashes: HashMap::from([(
                    HASH_SHA256.to_string(),
                    composite_sha256.clone(),
                )]),
            },
            parts,
            checksum_type: MultipartChecksumType::Composite,
        });

        let bytes = VersionReplicationMessage::VersionManifest(manifest)
            .to_bytes()
            .unwrap();
        let VersionReplicationMessage::VersionManifest(decoded) =
            VersionReplicationMessage::from_bytes(&bytes).unwrap()
        else {
            panic!("expected version manifest")
        };
        assert_eq!(
            decoded
                .multipart
                .unwrap()
                .summary
                .composite_hashes
                .get(HASH_SHA256),
            Some(&composite_sha256)
        );
        assert!(!decoded.reference_intent);
    }
}
