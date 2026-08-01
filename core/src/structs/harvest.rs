use crate::errors::ConversionError;
use crate::types::{GroupId, UserId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::time::SystemTime;
use ulid::Ulid;

/// Metadata repository protocol a connector speaks. Distinct from the data-staging
/// `SourceConnectorKind`: this harvests metadata records, not object bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RepositoryConnectorKind {
    OaiPmh,
}

impl RepositoryConnectorKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OaiPmh => "oai_pmh",
        }
    }
}

impl fmt::Display for RepositoryConnectorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A metadata repository a harvest can pull from.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RepositoryConnector {
    pub connector_id: Ulid,
    pub group_id: GroupId,
    pub name: String,
    pub kind: RepositoryConnectorKind,
    pub endpoint: String,
    pub public_config: HashMap<String, String>,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
    pub created_by: UserId,
}

impl RepositoryConnector {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        connector_id: Ulid,
        group_id: GroupId,
        name: String,
        kind: RepositoryConnectorKind,
        endpoint: String,
        public_config: HashMap<String, String>,
        created_at: SystemTime,
        updated_at: SystemTime,
        created_by: UserId,
    ) -> Self {
        Self {
            connector_id,
            group_id,
            name,
            kind,
            endpoint,
            public_config,
            created_at,
            updated_at,
            created_by,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// Credentials for a repository connector, split from the public record so the
/// public projection never carries a secret.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RepositoryConnectorSecret {
    pub connector_id: Ulid,
    pub secret_config: HashMap<String, String>,
    pub updated_at: SystemTime,
}

impl RepositoryConnectorSecret {
    pub fn new(
        connector_id: Ulid,
        secret_config: HashMap<String, String>,
        updated_at: SystemTime,
    ) -> Option<Self> {
        if secret_config.is_empty() {
            return None;
        }
        Some(Self {
            connector_id,
            secret_config,
            updated_at,
        })
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// Which records a harvest pulls from a connector, e.g. an OAI-PMH set and the
/// requested metadata schema.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct HarvestSelector {
    /// OAI-PMH `set` spec; `None` harvests the whole repository.
    pub set: Option<String>,
    /// OAI-PMH `metadataPrefix`; the record schema to request.
    pub metadata_prefix: Option<String>,
}

/// Incremental harvest position, advanced only after records are applied so a
/// re-run resumes rather than restarts.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HarvestCursor {
    /// Highest upstream datestamp applied so far. Drives the `from` window and
    /// idempotent staleness rejection; never a wall clock.
    pub last_datestamp_ms: u64,
    /// A listing paused mid-page hands back a resumption token to continue with.
    pub resumption_token: Option<String>,
}

/// A recurring pull of metadata records from one repository connector into a
/// metadata-path prefix.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HarvestSource {
    pub source_id: Ulid,
    pub group_id: GroupId,
    pub connector_id: Ulid,
    /// Dedup domain for provenance keys; a source record id is unique within it.
    pub namespace: String,
    /// Metadata path prefix new documents land under.
    pub target_prefix: String,
    pub selector: HarvestSelector,
    /// Re-harvest spacing; `None` harvests once on demand.
    pub schedule_interval_ms: Option<u64>,
    pub cursor: Option<HarvestCursor>,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
    pub created_by: UserId,
}

impl HarvestSource {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source_id: Ulid,
        group_id: GroupId,
        connector_id: Ulid,
        namespace: String,
        target_prefix: String,
        selector: HarvestSelector,
        schedule_interval_ms: Option<u64>,
        created_at: SystemTime,
        created_by: UserId,
    ) -> Self {
        Self {
            source_id,
            group_id,
            connector_id,
            namespace,
            target_prefix,
            selector,
            schedule_interval_ms,
            cursor: None,
            created_at,
            updated_at: created_at,
            created_by,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// Whether a harvested source record currently maps to a live document or a
/// withdrawn one. The mapping row is retained either way so the id is never
/// reused for a different source record.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum HarvestRecordState {
    Live,
    Tombstoned,
}

/// Binds one upstream source record to the metadata document minted for it.
///
/// The mapping `(namespace, source_record_id) -> meta_resource_id` is fixed at
/// mint and NEVER remapped: the same source record resolves to the same id
/// across re-harvests, deletions and revivals.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HarvestProvenance {
    pub namespace: String,
    pub source_record_id: String,
    pub meta_resource_id: Ulid,
    pub version: u64,
    /// Upstream datestamp last applied; an older datestamp is a stale no-op.
    pub source_datestamp_ms: u64,
    pub state: HarvestRecordState,
}

impl HarvestProvenance {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// The provenance keyspace key is `len(namespace) || namespace || record_id`,
/// so a namespace's records scan together and the two string fields cannot be
/// confused across a boundary.
pub fn harvest_provenance_prefix(namespace: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(4 + namespace.len());
    key.extend_from_slice(&(namespace.len() as u32).to_be_bytes());
    key.extend_from_slice(namespace.as_bytes());
    key
}

pub fn harvest_provenance_key(namespace: &str, source_record_id: &str) -> Vec<u8> {
    let mut key = harvest_provenance_prefix(namespace);
    key.extend_from_slice(source_record_id.as_bytes());
    key
}

/// One upstream record as seen during a harvest listing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IncomingRecord {
    pub datestamp_ms: u64,
    pub deleted: bool,
}

/// What a harvest must do for one source record given its stored provenance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProvenanceDecision {
    /// No prior mapping and the record is live: mint a new MetaResourceId.
    Mint,
    /// Live mapping, newer datestamp: write a new version under the same id.
    Update { meta_resource_id: Ulid },
    /// Live mapping, upstream deletion: tombstone and withdraw the document.
    Tombstone { meta_resource_id: Ulid },
    /// Tombstoned mapping, record reappeared: revive under the same id.
    Revive { meta_resource_id: Ulid },
    /// Nothing to do: not newer than what was applied, or a deletion of an
    /// unknown or already-tombstoned record.
    Skip,
}

/// Idempotent, wall-clock-free decision for one harvested record. A deletion at
/// or below the applied datestamp is skipped, so replays and out-of-order old
/// deletions never withdraw a newer document.
pub fn provenance_decision(
    existing: Option<&HarvestProvenance>,
    incoming: &IncomingRecord,
) -> ProvenanceDecision {
    let Some(prov) = existing else {
        return if incoming.deleted {
            ProvenanceDecision::Skip
        } else {
            ProvenanceDecision::Mint
        };
    };

    if incoming.datestamp_ms <= prov.source_datestamp_ms {
        return ProvenanceDecision::Skip;
    }

    let id = prov.meta_resource_id;
    match prov.state {
        HarvestRecordState::Live => {
            if incoming.deleted {
                ProvenanceDecision::Tombstone {
                    meta_resource_id: id,
                }
            } else {
                ProvenanceDecision::Update {
                    meta_resource_id: id,
                }
            }
        }
        HarvestRecordState::Tombstoned => {
            if incoming.deleted {
                ProvenanceDecision::Skip
            } else {
                ProvenanceDecision::Revive {
                    meta_resource_id: id,
                }
            }
        }
    }
}

/// The internal job payload for one run of a harvest source.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarvestJobSpec {
    pub source_id: Ulid,
    pub group_id: GroupId,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::RealmId;

    fn user(seed: u8) -> UserId {
        UserId::local(Ulid::from_bytes([seed; 16]), RealmId([seed; 32]))
    }

    fn live(id: u8, datestamp_ms: u64) -> HarvestProvenance {
        HarvestProvenance {
            namespace: "ns".to_string(),
            source_record_id: "rec-1".to_string(),
            meta_resource_id: Ulid::from_bytes([id; 16]),
            version: 1,
            source_datestamp_ms: datestamp_ms,
            state: HarvestRecordState::Live,
        }
    }

    #[test]
    fn connector_secret_split_stays_redacted() {
        let connector = RepositoryConnector::new(
            Ulid::from_bytes([1u8; 16]),
            Ulid::from_bytes([2u8; 16]),
            "zenodo".to_string(),
            RepositoryConnectorKind::OaiPmh,
            "https://example.org/oai".to_string(),
            HashMap::new(),
            SystemTime::UNIX_EPOCH,
            SystemTime::UNIX_EPOCH,
            user(3),
        );
        let secret = RepositoryConnectorSecret::new(
            connector.connector_id,
            HashMap::from([("token".to_string(), "super-secret".to_string())]),
            SystemTime::UNIX_EPOCH,
        )
        .unwrap();

        let public = connector.to_bytes().unwrap();
        assert!(
            !public
                .windows("super-secret".len())
                .any(|w| w == b"super-secret")
        );
        assert!(
            secret
                .to_bytes()
                .unwrap()
                .windows("super-secret".len())
                .any(|w| w == b"super-secret")
        );
    }

    #[test]
    fn empty_secret_config_is_none() {
        assert!(
            RepositoryConnectorSecret::new(
                Ulid::from_bytes([4u8; 16]),
                HashMap::new(),
                SystemTime::UNIX_EPOCH,
            )
            .is_none()
        );
    }

    #[test]
    fn source_roundtrip_preserves_cursor() {
        let mut source = HarvestSource::new(
            Ulid::from_bytes([5u8; 16]),
            Ulid::from_bytes([6u8; 16]),
            Ulid::from_bytes([7u8; 16]),
            "ns".to_string(),
            "imported/zenodo".to_string(),
            HarvestSelector {
                set: Some("openaire".to_string()),
                metadata_prefix: Some("oai_dc".to_string()),
            },
            Some(3_600_000),
            SystemTime::UNIX_EPOCH,
            user(8),
        );
        source.cursor = Some(HarvestCursor {
            last_datestamp_ms: 42,
            resumption_token: Some("tok".to_string()),
        });

        let restored = HarvestSource::from_bytes(&source.to_bytes().unwrap()).unwrap();
        assert_eq!(restored, source);
    }

    #[test]
    fn provenance_key_disambiguates_namespace_boundary() {
        // "a" + "bc" and "ab" + "c" must not collide on the same key bytes.
        assert_ne!(
            harvest_provenance_key("a", "bc"),
            harvest_provenance_key("ab", "c")
        );
        let key = harvest_provenance_key("ns", "rec-1");
        assert!(key.starts_with(&harvest_provenance_prefix("ns")));
    }

    #[test]
    fn unknown_record_mints_and_deletion_of_unknown_skips() {
        assert_eq!(
            provenance_decision(
                None,
                &IncomingRecord {
                    datestamp_ms: 10,
                    deleted: false
                }
            ),
            ProvenanceDecision::Mint
        );
        assert_eq!(
            provenance_decision(
                None,
                &IncomingRecord {
                    datestamp_ms: 10,
                    deleted: true
                }
            ),
            ProvenanceDecision::Skip
        );
    }

    #[test]
    fn newer_record_updates_same_id_never_remaps() {
        let prov = live(9, 5);
        assert_eq!(
            provenance_decision(
                Some(&prov),
                &IncomingRecord {
                    datestamp_ms: 6,
                    deleted: false
                }
            ),
            ProvenanceDecision::Update {
                meta_resource_id: prov.meta_resource_id
            }
        );
    }

    #[test]
    fn stale_or_equal_datestamp_is_idempotent_skip() {
        let prov = live(9, 5);
        for datestamp_ms in [4, 5] {
            assert_eq!(
                provenance_decision(
                    Some(&prov),
                    &IncomingRecord {
                        datestamp_ms,
                        deleted: false
                    }
                ),
                ProvenanceDecision::Skip
            );
        }
    }

    #[test]
    fn newer_deletion_tombstones_then_reharvest_skips() {
        let live_prov = live(9, 5);
        let decision = provenance_decision(
            Some(&live_prov),
            &IncomingRecord {
                datestamp_ms: 7,
                deleted: true,
            },
        );
        assert_eq!(
            decision,
            ProvenanceDecision::Tombstone {
                meta_resource_id: live_prov.meta_resource_id
            }
        );

        let mut tombstoned = live_prov.clone();
        tombstoned.state = HarvestRecordState::Tombstoned;
        tombstoned.source_datestamp_ms = 7;
        assert_eq!(
            provenance_decision(
                Some(&tombstoned),
                &IncomingRecord {
                    datestamp_ms: 7,
                    deleted: true
                }
            ),
            ProvenanceDecision::Skip
        );
    }

    #[test]
    fn reappeared_record_revives_same_id() {
        let mut prov = live(9, 5);
        prov.state = HarvestRecordState::Tombstoned;
        assert_eq!(
            provenance_decision(
                Some(&prov),
                &IncomingRecord {
                    datestamp_ms: 8,
                    deleted: false
                }
            ),
            ProvenanceDecision::Revive {
                meta_resource_id: prov.meta_resource_id
            }
        );
    }
}
