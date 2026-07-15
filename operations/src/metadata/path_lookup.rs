//! Path-keyed resolution surface (DEC-PATH, #416).
//!
//! Every committed `MetaResourceId` stays retrievable by id; a lookup BY PATH
//! instead returns the single deterministic winner served under the path and
//! exposes the retained losers as conflicts. The winner is a pure function of
//! the retained claim set (see [`resolve_path_claim`]), so a lookup on any
//! causally complete holder converges on the same answer regardless of arrival
//! order.

use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::METADATA_PATH_CLAIM_KEYSPACE;
use aruna_core::storage_entries::metadata_path_claim_prefix;
use aruna_core::structs::{MetadataRegistryRecord, PathClaimRecord, PathResolution, RealmId};
use aruna_core::types::{GroupId, Key};
use thiserror::Error;

use crate::driver::DriverContext;

/// Page size for the claim prefix scan. Contended paths hold a handful of
/// claims at most; one page covers every realistic conflict set.
const PATH_CLAIM_SCAN_PAGE: usize = 256;

#[derive(Debug, Error)]
pub enum PathLookupError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] aruna_core::errors::ConversionError),
    #[error("unexpected storage event resolving path claim: {0}")]
    UnexpectedStorageEvent(String),
}

/// Resolves the winner and retained conflicts for a `document_path` under
/// `(realm_id, group_id)`. `Ok(None)` when no id claims the path. The path is
/// normalized to the same canonical form the claim keys are written with.
pub async fn resolve_metadata_path(
    context: &DriverContext,
    realm_id: RealmId,
    group_id: GroupId,
    document_path: &str,
) -> Result<Option<PathResolution>, PathLookupError> {
    let normalized = MetadataRegistryRecord::normalize_document_path(document_path);
    let prefix = metadata_path_claim_prefix(&realm_id, group_id, &normalized);
    let claims = scan_path_claims(context, prefix).await?;
    Ok(aruna_core::structs::resolve_path_claim(&claims))
}

async fn scan_path_claims(
    context: &DriverContext,
    prefix: Key,
) -> Result<Vec<PathClaimRecord>, PathLookupError> {
    let mut claims = Vec::new();
    let mut start: Option<IterStart> = None;
    loop {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: METADATA_PATH_CLAIM_KEYSPACE.to_string(),
                prefix: Some(prefix.clone()),
                start,
                limit: PATH_CLAIM_SCAN_PAGE,
                txn_id: None,
            })
            .await;
        let (values, next) = match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            other => {
                return Err(PathLookupError::UnexpectedStorageEvent(format!("{other:?}")));
            }
        };
        for (_, value) in values {
            claims.push(postcard::from_bytes(&value).map_err(aruna_core::errors::ConversionError::from)?);
        }
        match next {
            Some(cursor) => start = Some(IterStart::After(cursor)),
            None => break,
        }
    }
    Ok(claims)
}
