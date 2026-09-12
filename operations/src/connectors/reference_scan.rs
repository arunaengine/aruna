//! Reference-page scanning shared by connector delete and replace.
//! Each operation owns its transaction lifecycle and final mutation.

use aruna_core::events::Event;
use aruna_core::types::Key;
use ulid::Ulid;

use crate::connectors::repository::{StorageReadError, parse_version_iter, references_connector};

/// What one scanned reference page asks its operation to do next.
pub(crate) enum ScanStep {
    Referenced,
    NextPage(Key),
    Complete,
}

/// Parse one reference-page event and report the next scan step.
pub(crate) fn parse_scan_page(
    event: Event,
    connector_id: Ulid,
) -> Result<ScanStep, StorageReadError> {
    let (versions, next_start_after) = parse_version_iter(event)?;

    if versions
        .iter()
        .any(|version| references_connector(version, connector_id))
    {
        return Ok(ScanStep::Referenced);
    }

    match next_start_after {
        Some(start_after) => Ok(ScanStep::NextPage(start_after)),
        None => Ok(ScanStep::Complete),
    }
}
