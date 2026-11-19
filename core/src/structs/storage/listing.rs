//! The pages an S3 version or multipart upload listing answers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::SystemTime;

use ulid::Ulid;

use crate::structs::execution::source_access::SourceMetadata;
use crate::structs::storage::blob::BackendLocation;
use crate::structs::storage::multipart::MultipartUpload;

#[derive(Debug, Clone, PartialEq)]
pub enum ListVersionsItem {
    Version {
        key: String,
        version_id: Ulid,
        is_latest: bool,
        location: Option<BackendLocation>,
        source_metadata: Option<SourceMetadata>,
        created_at: SystemTime,
        /// A link's binding, so a listing shows it only to callers who may read its target.
        link: Option<crate::structs::execution::staging::VersionSourceBinding>,
    },
    DeleteMarker {
        key: String,
        version_id: Ulid,
        is_latest: bool,
        created_at: SystemTime,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListVersionsResult {
    pub items: Vec<ListVersionsItem>,
    pub common_prefixes: Vec<String>,
    pub is_truncated: bool,
    pub next_key_marker: Option<String>,
    pub next_version_marker: Option<Ulid>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListUploadsResult {
    pub uploads: Vec<MultipartUpload>,
    pub common_prefixes: Vec<String>,
    pub is_truncated: bool,
    pub next_key_marker: Option<String>,
    pub next_upload_marker: Option<Ulid>,
}
