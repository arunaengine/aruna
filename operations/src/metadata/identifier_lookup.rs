//! Finds the readable documents this node indexes under one external identifier.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::drive;
use std::pin::pin;

use aruna_core::keyspaces::SECONDARY_ID_KEYSPACE;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::secondary_id::{SecondaryIdKind, index_document, secondary_id_prefix};
use byteview::ByteView;
use futures_util::TryStreamExt;

use crate::driver::DriverContext;
use crate::metadata::api::MetadataApiError;
use crate::metadata::api::read::can_read_record;
use crate::metadata::persistent_id::read_mapping;
use crate::metadata::record_reads::DocumentRecordOperation;
use crate::metadata::secondary_ids::IdentifierMatch;
use crate::storage_read::scan_pages;

/// Every readable document in this node's reverse index that holds the identifier.
/// Unreadable and deleted documents are skipped, so a miss never reveals that they exist.
pub async fn lookup_local(
    ctx: &DriverContext,
    realm_id: RealmId,
    auth: Option<&AuthContext>,
    kind: SecondaryIdKind,
    value: &str,
    endpoint: Option<&str>,
) -> Result<Vec<IdentifierMatch>, MetadataApiError> {
    let prefix = ByteView::from(secondary_id_prefix(kind, value, endpoint));
    let mut pages = pin!(scan_pages(
        &ctx.storage_handle,
        SECONDARY_ID_KEYSPACE,
        Some(prefix)
    ));
    let mut documents = Vec::new();
    while let Some(page) = pages.try_next().await.map_err(MetadataApiError::Internal)? {
        documents.extend(
            page.iter()
                .filter_map(|(key, _)| index_document(key.as_ref())),
        );
    }
    documents.sort();
    documents.dedup();
    let mut matches = Vec::new();
    for document_id in documents {
        let record = drive(DocumentRecordOperation::new(document_id), ctx)
            .await
            .map_err(|_| MetadataApiError::ServiceUnavailable)?;
        let Some(record) = record else {
            continue;
        };
        if !can_read_record(ctx, realm_id, auth, &record).await? {
            continue;
        }
        let mapping = read_mapping(ctx, document_id)
            .await
            .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
        let Some(mapping) = mapping.filter(|mapping| !mapping.is_retired()) else {
            continue;
        };
        let origin = mapping
            .secondary_identifiers
            .iter()
            .filter(|held| {
                held.kind == kind
                    && held.value == value
                    && endpoint.is_none_or(|endpoint| held.endpoint.as_deref() == Some(endpoint))
            })
            .map(|held| held.origin)
            .min();
        let Some(origin) = origin else {
            continue;
        };
        matches.push(IdentifierMatch {
            document_id,
            pid: mapping.is_active().then_some(mapping.pid),
            origin,
        });
    }
    Ok(matches)
}
