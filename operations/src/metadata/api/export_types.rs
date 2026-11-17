//! Holds the metadata export request and result types and their page limits.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{
    AuthContext, Deserialize, MetadataRegistryRecord, MetadataRoCratePage, Serialize, Ulid,
};

/// Data entities a page holds when the request names no limit, and at most.
pub(crate) const PAGE_DEFAULT: usize = 100;

pub(crate) const PAGE_MAX: usize = 1_000;

/// Bytes a page's reader may add around each entity, which the page's size bound counts.
pub(crate) const ENTRY_ROOM: usize = 8;

#[derive(Debug, Clone)]
pub struct GetVisibleRequest {
    pub document_id: Ulid,
    pub auth: Option<AuthContext>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoCrateExportView {
    Full,
    Summary,
    Page,
    Raw,
}

#[derive(Debug, Clone)]
pub struct ExportMetadataRequest {
    pub document_id: Ulid,
    pub auth: Option<AuthContext>,
    pub view: RoCrateExportView,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub after: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ExportMetadataResult {
    Full {
        record: MetadataRegistryRecord,
        jsonld: String,
    },
    Summary {
        record: MetadataRegistryRecord,
        jsonld: String,
    },
    Page {
        record: MetadataRegistryRecord,
        page: MetadataRoCratePage,
    },
    Raw {
        record: MetadataRegistryRecord,
        raw: crate::metadata::revision_read::MetadataRawView,
        dataset_digest: Option<[u8; 32]>,
    },
}
