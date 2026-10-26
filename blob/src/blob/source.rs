//! Runs staging source effects: check, head, read and list against a resolved source access.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::BlobHandler;
use crate::opendal::{
    check_staging_source, head_staging_source, list_staging_source, read_staging_source,
};
use aruna_core::events::StagingSourceEvent;
use aruna_core::structs::execution::source_access::{
    ListRequest, ResolvedSourceAccess, SourceMetadata,
};

impl BlobHandler {
    pub(crate) async fn check_staging_source(
        &self,
        access: ResolvedSourceAccess,
    ) -> StagingSourceEvent {
        match check_staging_source(&self.egress, &access).await {
            Ok(()) => StagingSourceEvent::CheckResult,
            Err(error) => StagingSourceEvent::Error { error },
        }
    }

    pub(crate) async fn head_staging_source(
        &self,
        access: ResolvedSourceAccess,
    ) -> StagingSourceEvent {
        match head_staging_source(&self.egress, &access).await {
            Ok(metadata) => StagingSourceEvent::HeadResult { metadata },
            Err(error) => StagingSourceEvent::Error { error },
        }
    }

    pub(crate) async fn read_staging_source(
        &self,
        access: ResolvedSourceAccess,
        range: Option<std::ops::Range<u64>>,
        expected: Option<SourceMetadata>,
    ) -> StagingSourceEvent {
        match read_staging_source(&self.egress, &access, range, expected.as_ref()).await {
            Ok((metadata, stream)) => StagingSourceEvent::ReadResult { metadata, stream },
            Err(error) => StagingSourceEvent::Error { error },
        }
    }

    pub(crate) async fn list_staging_source(
        &self,
        access: ResolvedSourceAccess,
        request: ListRequest,
    ) -> StagingSourceEvent {
        match list_staging_source(&self.egress, &access, &request).await {
            Ok((entries, next_cursor)) => StagingSourceEvent::ListResult {
                entries,
                next_cursor,
            },
            Err(error) => StagingSourceEvent::Error { error },
        }
    }
}
