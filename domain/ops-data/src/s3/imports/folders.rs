//! Decides whether import rules read the same part of a backend.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::backend_import::BackendImport;

/// Whether two folders of one bucket share keys.
pub fn folders_overlap(left: &str, right: &str) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

/// Whether `other` is another rule that reads an overlapping folder of the same backend.
pub fn shares_folder(rule: &BackendImport, other: &BackendImport) -> bool {
    other.import_id != rule.import_id
        && other.backend_id == rule.backend_id
        && folders_overlap(&other.backend_prefix, &rule.backend_prefix)
}
