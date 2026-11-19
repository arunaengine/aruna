//! Holds the metadata export request and result types and their page limits.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{AuthContext, Ulid};
use aruna_core::metadata_api::RoCrateExportView;

/// Data entities a page holds when the request names no limit, and at most.
pub(crate) const PAGE_DEFAULT: usize = 100;

pub(crate) const PAGE_MAX: usize = 1_000;

/// Bytes a page's reader may add around each entity, which the page's size bound counts.
pub(crate) const ENTRY_ROOM: usize = 8;

#[derive(Debug, Clone, PartialEq)]
pub struct GetVisibleRequest {
    pub document_id: Ulid,
    pub auth: Option<AuthContext>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExportMetadataRequest {
    pub document_id: Ulid,
    pub auth: Option<AuthContext>,
    pub view: RoCrateExportView,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub after: Option<String>,
}
