//! Checks that a repository draft holds exactly the requested metadata and files.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_blob::hash::Hasher;
use aruna_blob::invenio::InvenioClient;
use aruna_core::repository::RepositoryRecord;
use aruna_core::repository::invenio::normalize_metadata;
use http::Method;
use serde_json::{Value, json};

use super::export::invalid;
use crate::jobs::repository::TransferError;

pub(super) async fn verify_files(
    client: &InvenioClient<'_>,
    record: &RepositoryRecord,
    published: bool,
    expected: &std::collections::BTreeMap<String, (Hasher, u64)>,
) -> Result<(), TransferError> {
    let url = if published {
        client.url(&["records", &record.id, "files"])?
    } else {
        client.url(&["records", &record.id, "draft", "files"])?
    };
    let files = client.json(Method::GET, url, None).await?;
    let entries = files["entries"]
        .as_array()
        .ok_or_else(|| invalid("missing repository files"))?;
    if entries.len() != expected.len()
        || files["links"]["next"]
            .as_str()
            .is_some_and(|link| !link.is_empty())
    {
        return Err(invalid("repository file set changed"));
    }
    let mut keys = std::collections::HashSet::new();
    for file in entries {
        let key = file["key"]
            .as_str()
            .ok_or_else(|| invalid("missing file key"))?;
        if !keys.insert(key) {
            return Err(invalid("duplicate repository file"));
        }
        let (hash, size) = expected
            .get(key)
            .ok_or_else(|| invalid("unexpected repository file"))?;
        verify_file(file, hash, *size, true)?;
    }
    Ok(())
}

pub(super) fn metadata_digest(record: &Value) -> [u8; 32] {
    let fields = json!({"metadata": record["metadata"], "custom_fields": record.get("custom_fields").cloned().unwrap_or_else(|| json!({}))});
    *blake3::hash(fields.to_string().as_bytes()).as_bytes()
}

pub(super) fn verify_metadata(expected: &Value, record: &Value) -> Result<(), TransferError> {
    if !matches_fields(&expected["metadata"], &record["metadata"])
        || (expected.get("custom_fields").is_some()
            && !complete_fields(&expected["custom_fields"], &record["custom_fields"]))
    {
        return Err(invalid(
            "repository metadata differs from the requested crate",
        ));
    }
    Ok(())
}

pub(super) fn complete_metadata(expected: &Value, actual: &Value) -> bool {
    let mut actual = actual.clone();
    normalize_metadata(&mut actual, Some(expected));
    complete_fields(expected, &actual)
}

pub(super) fn complete_fields(expected: &Value, actual: &Value) -> bool {
    if actual.is_null()
        && (expected.as_object().is_some_and(serde_json::Map::is_empty)
            || expected.as_array().is_some_and(Vec::is_empty))
    {
        return true;
    }
    match (expected, actual) {
        (Value::Object(expected), Value::Object(actual)) => {
            expected
                .iter()
                .all(|(key, value)| complete_fields(value, actual.get(key).unwrap_or(&Value::Null)))
                && actual.iter().all(|(key, value)| {
                    expected.contains_key(key)
                        || value.is_null()
                        || value.as_array().is_some_and(Vec::is_empty)
                })
        }
        (Value::Array(expected), Value::Array(actual)) => {
            expected.len() == actual.len()
                && expected
                    .iter()
                    .zip(actual)
                    .all(|(expected, actual)| complete_fields(expected, actual))
        }
        _ => expected == actual,
    }
}

fn matches_fields(expected: &Value, actual: &Value) -> bool {
    if actual.is_null() && expected.as_array().is_some_and(Vec::is_empty) {
        return true;
    }
    match expected {
        Value::Object(fields) => {
            actual.is_object()
                && fields
                    .iter()
                    .all(|(key, value)| matches_fields(value, &actual[key]))
        }
        Value::Array(values) => actual.as_array().is_some_and(|actual| {
            values.len() == actual.len()
                && values
                    .iter()
                    .zip(actual)
                    .all(|(expected, actual)| matches_fields(expected, actual))
        }),
        _ => expected == actual,
    }
}

pub(super) fn verify_file(
    file: &Value,
    hasher: &Hasher,
    size: u64,
    completed: bool,
) -> Result<(), TransferError> {
    let checksum = file["checksum"]
        .as_str()
        .ok_or_else(|| invalid("missing uploaded checksum"))?;
    let (algorithm, digest) = checksum
        .split_once(':')
        .ok_or_else(|| invalid("invalid uploaded checksum"))?;
    let hashes = hasher.to_map();
    if (completed && file["status"] != "completed")
        || file["size"].as_u64() != Some(size)
        || hashes.get(algorithm).map(hex::encode).as_deref() != Some(digest)
    {
        return Err(invalid("repository file checksum or size mismatch"));
    }
    Ok(())
}
