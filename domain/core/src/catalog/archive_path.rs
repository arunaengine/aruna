//! Lays exported files out as archive paths: authored keys, synthesized names and safe paths.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::path::{Component, Path};

use serde::{Deserialize, Serialize};
use unicode_normalization::UnicodeNormalization;

use crate::structs::storage::blob::ensure_confined_path;

/// The bucket and key an entity was authored against, used to lay the archive
/// out like the source prefix instead of by content hash.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StorageKey {
    pub bucket: String,
    pub key: String,
}

pub fn safe_zip_path(value: &str) -> Option<String> {
    let mut value = value;
    while let Some(stripped) = value.strip_prefix("./") {
        value = stripped;
    }
    let normalized = value.nfc().collect::<String>();
    let lower = normalized.to_ascii_lowercase();
    if normalized.is_empty()
        || normalized.ends_with('/')
        || normalized.contains('\\')
        || lower.contains("%2f")
        || lower.contains("%5c")
        || normalized
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
        || ensure_confined_path(Path::new(&normalized)).is_err()
        || Path::new(&normalized)
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return None;
    }
    Some(normalized)
}

pub fn synthesized_path(hash: [u8; 32], entity_id: &str) -> String {
    let suffix = blake3::hash(entity_id.as_bytes()).to_hex();
    format!("data/{}-{}", hex::encode(hash), &suffix[..12])
}

/// How keys become archive paths: one bucket drops the directory prefix every
/// payload shares, several buckets keep the whole key under the bucket name.
pub struct KeyLayout {
    dropped: usize,
    with_bucket: bool,
}

impl KeyLayout {
    pub fn new(sources: &[Option<StorageKey>]) -> Self {
        let mut buckets = BTreeSet::new();
        let mut shared: Option<Vec<&str>> = None;
        for source in sources.iter().flatten() {
            buckets.insert(source.bucket.as_str());
            let parents = key_parents(&source.key);
            shared = Some(match shared {
                Some(shared) => common_prefix(shared, &parents),
                None => parents,
            });
        }
        let single = buckets.len() == 1;
        Self {
            dropped: if single {
                shared.map_or(0, |shared| shared.len())
            } else {
                0
            },
            with_bucket: !single,
        }
    }

    pub fn path(&self, source: &StorageKey) -> Option<String> {
        let relative = source
            .key
            .split('/')
            .skip(self.dropped)
            .collect::<Vec<_>>()
            .join("/");
        let candidate = if self.with_bucket {
            format!("{}/{relative}", source.bucket)
        } else {
            relative
        };
        safe_zip_path(&candidate)
    }
}

fn key_parents(key: &str) -> Vec<&str> {
    let mut parents = key.split('/').collect::<Vec<_>>();
    parents.pop();
    parents
}

fn common_prefix<'a>(mut shared: Vec<&'a str>, parents: &[&str]) -> Vec<&'a str> {
    let common = shared
        .iter()
        .zip(parents)
        .take_while(|(left, right)| left == right)
        .count();
    shared.truncate(common);
    shared
}
