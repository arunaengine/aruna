//! Re-exports the archive reading and document rewrite helpers used by import tests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub(crate) use crate::jobs::import::rewrite::{RewriteTarget, rewrite_document, validate_document};
pub(crate) use aruna_blob::archive::{inspect_archive, open_archive, read_metadata};
pub(crate) use aruna_core::rocrate_archive::{
    file_id_candidates, payload_entries, signature_entry,
};
