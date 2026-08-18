//! Identity of the one realm wire format this build speaks.
//!
//! [`crate::storage_format`] fences a local database root; this fences a peer.
//! The two evolve independently: a stored-record change need not invalidate a
//! running realm's wire, and a replicated-document change need not condemn an
//! untouched root. A peer that declares another tag is refused before any
//! document is applied; there is no negotiation and no fallback.

use thiserror::Error;

/// Current realm wire epoch. Bump it whenever a replicated document, admin
/// operation, or enrollment payload changes shape.
pub const REALM_FORMAT_EPOCH: u32 = 3;

/// Tag peers compare verbatim during document-sync admission. It doubles as
/// [`crate::document::DocumentSyncEvent`]'s event type id, so one bump refuses
/// every foreign-format topic without a second constant to keep in step.
pub const REALM_FORMAT_TAG: &str = "aruna.document.v3";

/// Longest peer-supplied tag echoed back into an error or log line.
const MAX_REPORTED_TAG: usize = 64;

/// Why a peer may not join or sync with this realm.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum RealmFormatError {
    #[error("peer declares realm format `{declared}`, this build requires `{REALM_FORMAT_TAG}`")]
    Mismatch { declared: String },
    #[error("peer declares no realm format, this build requires `{REALM_FORMAT_TAG}`")]
    Absent,
    #[error(
        "peer declares realm format epoch {declared}, this build requires {REALM_FORMAT_EPOCH}"
    )]
    Epoch { declared: u32 },
}

/// Admits a peer that declares exactly this build's tag. The reported tag is
/// truncated, so a hostile peer cannot inflate an error or a log line.
pub fn verify_realm_format(declared: Option<&str>) -> Result<(), RealmFormatError> {
    match declared {
        None => Err(RealmFormatError::Absent),
        Some(tag) if tag == REALM_FORMAT_TAG => Ok(()),
        Some(tag) => Err(RealmFormatError::Mismatch {
            declared: tag.chars().take(MAX_REPORTED_TAG).collect(),
        }),
    }
}

/// Admits a peer that declares exactly this build's epoch.
pub fn verify_realm_epoch(declared: u32) -> Result<(), RealmFormatError> {
    match declared == REALM_FORMAT_EPOCH {
        true => Ok(()),
        false => Err(RealmFormatError::Epoch { declared }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tag_names_the_epoch() {
        // One bump must move both, or a foreign peer could match the tag while
        // declaring another epoch.
        assert!(REALM_FORMAT_TAG.ends_with(&format!("v{REALM_FORMAT_EPOCH}")));
        assert_eq!(
            REALM_FORMAT_TAG,
            crate::document::DocumentSyncEvent::TYPE_ID
        );
    }

    #[test]
    fn admits_only_this_format() {
        assert_eq!(verify_realm_format(Some(REALM_FORMAT_TAG)), Ok(()));
        assert_eq!(verify_realm_format(None), Err(RealmFormatError::Absent));
        assert_eq!(
            verify_realm_format(Some("aruna.document.v2")),
            Err(RealmFormatError::Mismatch {
                declared: "aruna.document.v2".to_string()
            })
        );
        assert_eq!(verify_realm_epoch(REALM_FORMAT_EPOCH), Ok(()));
        assert_eq!(
            verify_realm_epoch(REALM_FORMAT_EPOCH - 1),
            Err(RealmFormatError::Epoch {
                declared: REALM_FORMAT_EPOCH - 1
            })
        );
    }

    #[test]
    fn bounds_a_reported_tag() {
        let Err(RealmFormatError::Mismatch { declared }) =
            verify_realm_format(Some(&"x".repeat(4096)))
        else {
            panic!("an oversized tag must not be admitted");
        };
        assert_eq!(declared.len(), MAX_REPORTED_TAG);
    }
}
