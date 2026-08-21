//! Complete reads of one job family.
//!
//! Scheduling, admission, and state publication may only decide on the whole
//! family. A prefix of it, a row that failed to decode, or a scan that stopped
//! at its bound is an availability failure, never evidence that a record does
//! not exist.

use std::future::Future;

use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::JOB_FAMILY_RECORD_KEYSPACE;
use aruna_core::structs::{JobFamilyId, JobRecordEnvelope, JobRecordKey, JobRecordKind};
use aruna_core::types::{Key, Value};
use thiserror::Error;

use super::keys::{family_prefix, kind_prefix};
use super::rows::from_bytes;
use super::{MAX_PROJECTION_RECORDS, RECORD_PAGE_SIZE};
use crate::driver::DriverContext;
use crate::jobs::store::iter_prefix_page;

/// Why one family could not be read completely. Every variant leaves the caller
/// undecided; none of them means the family is empty.
#[derive(Debug, PartialEq, Error)]
pub enum FamilyReadError {
    #[error("job family page read failed: {0}")]
    Storage(String),
    #[error(transparent)]
    Decode(#[from] ConversionError),
    /// A row under the prefix is not addressed by a record key, so the scan
    /// cannot tell what it holds or whether it belongs to this family.
    #[error("job family row key is malformed")]
    MalformedKey,
    #[error("job family exceeds the bounded read after {loaded} records")]
    LimitExceeded { loaded: usize },
}

/// One bounded page of stored rows and the cursor that continues it.
pub type RecordPage = (Vec<(Key, Value)>, Option<Key>);

/// Every record of one family, or the reason it could not be read completely.
pub async fn load_family_complete(
    context: &DriverContext,
    family: JobFamilyId,
) -> Result<Vec<JobRecordEnvelope>, FamilyReadError> {
    read_complete(
        family_prefix(&family),
        MAX_PROJECTION_RECORDS,
        |prefix, cursor| stored_page(context, prefix, cursor),
    )
    .await
}

/// Every record of one kind inside one family. An exact question reads its own
/// kind prefix, so unrelated history can never hide a receipt, a cancellation,
/// or an update behind the read bound.
pub async fn load_kind_complete(
    context: &DriverContext,
    family: JobFamilyId,
    kind: JobRecordKind,
) -> Result<Vec<JobRecordEnvelope>, FamilyReadError> {
    read_complete(
        kind_prefix(&family, kind),
        MAX_PROJECTION_RECORDS,
        |prefix, cursor| stored_page(context, prefix, cursor),
    )
    .await
}

async fn stored_page(
    context: &DriverContext,
    prefix: Key,
    cursor: Option<Key>,
) -> Result<RecordPage, String> {
    iter_prefix_page(
        &context.storage_handle,
        JOB_FAMILY_RECORD_KEYSPACE,
        Some(prefix),
        cursor,
        RECORD_PAGE_SIZE,
        None,
    )
    .await
}

/// Pages `prefix` to its end. A page error, an undecodable row, or a
/// continuation past `limit` fails the whole read instead of returning the part
/// that happened to load.
async fn read_complete<R, F>(
    prefix: Key,
    limit: usize,
    reader: R,
) -> Result<Vec<JobRecordEnvelope>, FamilyReadError>
where
    R: Fn(Key, Option<Key>) -> F,
    F: Future<Output = Result<RecordPage, String>>,
{
    let mut records = Vec::new();
    let mut cursor: Option<Key> = None;
    loop {
        let (values, next) = reader(prefix.clone(), cursor)
            .await
            .map_err(FamilyReadError::Storage)?;
        for (key, value) in values {
            JobRecordKey::from_bytes(&key).map_err(|_| FamilyReadError::MalformedKey)?;
            records.push(from_bytes::<JobRecordEnvelope>(&value)?);
        }
        match next {
            Some(_) if records.len() >= limit => {
                return Err(FamilyReadError::LimitExceeded {
                    loaded: records.len(),
                });
            }
            Some(next) => cursor = Some(next),
            None => return Ok(records),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use aruna_core::structs::PhysicalExecutionState;

    use super::*;
    use crate::jobs::records::keys::record_key;
    use crate::jobs::records::rows::to_bytes;
    use crate::jobs::records::tests::fixture::Family;

    fn row(envelope: &JobRecordEnvelope) -> (Key, Value) {
        (
            record_key(&envelope.key()),
            Value::from(to_bytes(envelope).expect("record encodes").as_slice()),
        )
    }

    /// One page of stored rows, with a continuation when more follow it.
    fn page(records: &[JobRecordEnvelope], more: bool) -> Result<RecordPage, String> {
        let values: Vec<(Key, Value)> = records.iter().map(row).collect();
        let next = more.then(|| record_key(&records.last().expect("page carries a record").key()));
        Ok((values, next))
    }

    async fn read(
        pages: Vec<Result<RecordPage, String>>,
        limit: usize,
    ) -> Result<Vec<JobRecordEnvelope>, FamilyReadError> {
        let served = Cell::new(0usize);
        read_complete(Key::from([7u8].as_slice()), limit, |_, _| {
            let page = pages
                .get(served.get())
                .cloned()
                .unwrap_or_else(|| Ok((Vec::new(), None)));
            served.set(served.get() + 1);
            async move { page }
        })
        .await
    }

    fn records() -> Vec<JobRecordEnvelope> {
        Family::new([1u8; 32]).run(1, 0, PhysicalExecutionState::Succeeded)
    }

    #[tokio::test]
    async fn reads_all_pages() {
        let records = records();
        let (head, tail) = records.split_at(3);
        let loaded = read(vec![page(head, true), page(tail, false)], 64)
            .await
            .expect("both pages load");
        assert_eq!(loaded.len(), records.len());
    }

    #[tokio::test]
    async fn rejects_page_error() {
        // A later page failing must never surface as the pages before it: the
        // receipt on page two would otherwise look absent.
        let records = records();
        let (head, _) = records.split_at(3);
        assert_eq!(
            read(vec![page(head, true), Err("read error".to_string())], 64).await,
            Err(FamilyReadError::Storage("read error".to_string()))
        );
    }

    #[tokio::test]
    async fn rejects_undecodable_row() {
        let records = records();
        let mut values: Vec<(Key, Value)> = records.iter().map(row).collect();
        values[1].1 = Value::from([0xffu8; 8].as_slice());
        assert!(matches!(
            read(vec![Ok((values, None))], 64).await,
            Err(FamilyReadError::Decode(_))
        ));
    }

    #[tokio::test]
    async fn rejects_malformed_key() {
        let records = records();
        let mut values: Vec<(Key, Value)> = records.iter().map(row).collect();
        values[0].0 = Key::from([1u8; 4].as_slice());
        assert_eq!(
            read(vec![Ok((values, None))], 64).await,
            Err(FamilyReadError::MalformedKey)
        );
    }

    #[tokio::test]
    async fn stops_at_limit() {
        // A family larger than the read bound is reported, never truncated into
        // a decision.
        let records = records();
        let (head, tail) = records.split_at(3);
        assert_eq!(
            read(vec![page(head, true), page(tail, false)], 3).await,
            Err(FamilyReadError::LimitExceeded { loaded: 3 })
        );
    }
}
