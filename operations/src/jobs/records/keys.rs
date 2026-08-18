//! Storage keys of the append-only job-record store. Every key is built from
//! the record's own signed identity, so a relay can never place a record under
//! another family, kind, or subject.

use aruna_core::effects::{FetchCursor, FrameBoundsError};
use aruna_core::structs::{
    JOB_RECORD_KEY_BYTES, JobFamilyId, JobId, JobRecordError, JobRecordKey, JobRecordKind,
    SubmissionId,
};
use aruna_core::types::Key;

/// Encoded width of a conflict row key: record key plus the rejected digest.
pub const CONFLICT_KEY_BYTES: usize = JOB_RECORD_KEY_BYTES + 32;

pub fn record_key(key: &JobRecordKey) -> Key {
    Key::from(key.to_bytes().as_slice())
}

/// Prefix of every record of one request family.
pub fn family_prefix(family: &JobFamilyId) -> Key {
    Key::from(family.to_bytes().as_slice())
}

/// Prefix of every family of one submission, including idempotency conflicts.
pub fn submission_prefix(submission_id: SubmissionId) -> Key {
    Key::from(submission_id.0.as_slice())
}

/// Prefix of one kind inside one family, in the key order kinds are stored in.
pub fn kind_prefix(family: &JobFamilyId, kind: JobRecordKind) -> Key {
    let mut bytes = family.to_bytes().to_vec();
    bytes.push(kind.as_byte());
    Key::from(bytes.as_slice())
}

/// Conflict rows keep the rejected digest in the key, so two different bytes
/// under one record key are both retained instead of overwriting each other.
pub fn conflict_key(key: &JobRecordKey, digest: &[u8; 32]) -> Key {
    let mut bytes = Vec::with_capacity(CONFLICT_KEY_BYTES);
    bytes.extend_from_slice(&key.to_bytes());
    bytes.extend_from_slice(digest);
    Key::from(bytes.as_slice())
}

/// Alias rows are append-only: the family is part of the key, so two families
/// claiming one `JobId` both stay visible instead of rebinding the first one.
pub fn alias_key(job_id: JobId, family: &JobFamilyId) -> Key {
    let mut bytes = job_id.to_bytes().to_vec();
    bytes.extend_from_slice(&family.to_bytes());
    Key::from(bytes.as_slice())
}

pub fn alias_prefix(job_id: JobId) -> Key {
    Key::from(job_id.to_bytes().as_slice())
}

/// The family an alias row names, read back from its key.
pub fn alias_family(key: &[u8]) -> Option<JobFamilyId> {
    let submission: [u8; 32] = key.get(16..48)?.try_into().ok()?;
    let request_digest: [u8; 32] = key.get(48..80)?.try_into().ok()?;
    Some(JobFamilyId {
        submission_id: SubmissionId(submission),
        request_digest,
    })
}

/// Audit and page cursors carry the last returned record key, which is bounded
/// and self-describing: a peer cannot hand back a marker of its own choosing.
pub fn cursor_of(key: &JobRecordKey) -> Result<FetchCursor, FrameBoundsError> {
    FetchCursor::new(key.to_bytes().to_vec())
}

pub fn cursor_key(cursor: &FetchCursor) -> Result<JobRecordKey, JobRecordError> {
    JobRecordKey::from_bytes(cursor.as_slice())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ulid::Ulid;

    fn family() -> JobFamilyId {
        JobFamilyId {
            submission_id: SubmissionId([1u8; 32]),
            request_digest: [2u8; 32],
        }
    }

    #[test]
    fn keys_share_prefixes() {
        // Paging by submission, family, and kind must all be prefix scans of the
        // one record key, and a cursor must round-trip that key exactly.
        let key = JobRecordKey {
            family: family(),
            kind: JobRecordKind::Receipt,
            subject: [3u8; 32],
            sequence: 7,
        };
        let encoded = record_key(&key);
        assert!(encoded.starts_with(family_prefix(&family())));
        assert!(encoded.starts_with(submission_prefix(SubmissionId([1u8; 32]))));
        assert!(encoded.starts_with(kind_prefix(&family(), JobRecordKind::Receipt)));
        assert!(!encoded.starts_with(kind_prefix(&family(), JobRecordKind::Launch)));

        let cursor = cursor_of(&key).expect("bounded cursor");
        assert_eq!(cursor_key(&cursor), Ok(key));
        assert_eq!(
            JobRecordKey::from_bytes(&conflict_key(&key, &[4u8; 32])),
            Err(JobRecordError::MalformedKey)
        );
    }

    #[test]
    fn kinds_sort_before_updates() {
        // Evidence kinds must page before updates and outputs, so one bounded
        // scan from the family prefix always yields the evidence first.
        let evidence = kind_prefix(&family(), JobRecordKind::Receipt);
        let update = kind_prefix(&family(), JobRecordKind::Update);
        assert!(evidence < update);
    }

    #[test]
    fn alias_keeps_family() {
        // An alias row must name its family in the key, so a second family
        // claiming the same id is retained instead of rebinding the first.
        let job_id = JobId::from_bytes(Ulid::from_bytes([5u8; 16]).to_bytes());
        let key = alias_key(job_id, &family());
        assert!(key.starts_with(alias_prefix(job_id)));
        assert_eq!(alias_family(&key), Some(family()));
    }
}
