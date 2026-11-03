//! Writes the current objects of a bucket from a node's store as record rows, which a table
//! import of kind `record` writes back without a request to any source.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::error::CliError;
use crate::explorer::ExplorerError;
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE};
use aruna_core::structs::checksum::ChecksumAlgorithm;
use aruna_core::structs::execution::source_access::DigestScope;
use aruna_core::structs::storage::blob::{
    BlobHeadKey, BlobVersion, BlobVersionState, CurrentVersionPointer, VersionKey,
};
use chrono::{DateTime, SecondsFormat, Utc};
use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, Readable};
use serde::Serialize;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

/// Columns of every record file; the import names them in its `record` rows.
pub const RECORD_HEADER: &str =
    "key\tsource\tsize\tmodified\tetag\tcontent_type\tmd5\tsha1\tsha256\tmetadata\n";
/// File of the empty objects, which have no source.
const EMPTY_FILE: &str = "empty";

#[derive(Debug, Default, Serialize)]
pub struct ExportOutput {
    pub bucket: String,
    /// Rows per file: one file per connector id, `empty` for empty objects.
    pub files: BTreeMap<String, u64>,
    /// Current objects without a record row, by reason.
    pub skipped: BTreeMap<&'static str, u64>,
}

pub async fn export_bucket(
    database_path: String,
    bucket: String,
    target: String,
) -> Result<(), CliError> {
    let output = tokio::task::spawn_blocking(move || export_rows(&database_path, &bucket, &target))
        .await
        .map_err(std::io::Error::other)??;
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn decode(error: impl ToString) -> ExplorerError {
    ExplorerError::Decode(error.to_string())
}

fn export_rows(
    database_path: &str,
    bucket: &str,
    target: &str,
) -> Result<ExportOutput, ExplorerError> {
    let db = OptimisticTxDatabase::builder(Path::new(database_path)).open()?;
    let heads = db.keyspace(BLOB_HEAD_KEYSPACE, KeyspaceCreateOptions::default)?;
    let versions = db.keyspace(BLOB_VERSIONS_KEYSPACE, KeyspaceCreateOptions::default)?;
    std::fs::create_dir_all(target)?;
    let read = db.read_tx();
    let mut output = ExportOutput {
        bucket: bucket.to_string(),
        ..Default::default()
    };
    let mut files: BTreeMap<String, BufWriter<File>> = BTreeMap::new();
    let prefix = BlobHeadKey::bucket_prefix(bucket).map_err(decode)?;
    for entry in read.prefix(&heads, prefix) {
        let (key, value) = entry.into_inner()?;
        let head = BlobHeadKey::from_bytes(&key).map_err(decode)?;
        let pointer = CurrentVersionPointer::from_bytes(&value).map_err(decode)?;
        let version_key = VersionKey::new(bucket, &head.key, pointer.version_id);
        let Some(row) = read.get(&versions, version_key.to_bytes().map_err(decode)?)? else {
            *output.skipped.entry("version missing").or_default() += 1;
            continue;
        };
        let version = BlobVersion::from_bytes(&row).map_err(decode)?;
        let (name, line) = match record_row(&head.key, &version) {
            Ok(row) => row,
            Err(reason) => {
                *output.skipped.entry(reason).or_default() += 1;
                continue;
            }
        };
        let file = match files.entry(name.clone()) {
            std::collections::btree_map::Entry::Occupied(file) => file.into_mut(),
            std::collections::btree_map::Entry::Vacant(slot) => {
                let path = Path::new(target).join(format!("{name}.tsv"));
                let mut file = BufWriter::new(File::create(path)?);
                file.write_all(RECORD_HEADER.as_bytes())?;
                slot.insert(file)
            }
        };
        file.write_all(line.as_bytes())?;
        *output.files.entry(name).or_default() += 1;
    }
    for file in files.values_mut() {
        file.flush()?;
    }
    Ok(output)
}

/// The file and line of one current version, or why it has no record row.
fn record_row(key: &str, version: &BlobVersion) -> Result<(String, String), &'static str> {
    let ordered: BTreeMap<&String, &String> = version.metadata.iter().collect();
    let metadata = serde_json::to_string(&ordered).map_err(|_| "metadata not encodable")?;
    let (name, fields) = match &version.state {
        BlobVersionState::Reference {
            source,
            cached_metadata: seen,
            ..
        } => {
            let digest = |algorithm: ChecksumAlgorithm| {
                (seen.digests.get(&algorithm))
                    .filter(|digest| digest.scope == DigestScope::FullObject)
                    .map(|digest| hex::encode(&digest.digest))
                    .unwrap_or_default()
            };
            let modified = (seen.last_modified)
                .map(|time| {
                    DateTime::<Utc>::from(time).to_rfc3339_opts(SecondsFormat::AutoSi, true)
                })
                .unwrap_or_default();
            let name =
                (source.connector_id).map_or_else(|| "unbound".to_string(), |id| id.to_string());
            let fields = [
                source.descriptor.source_path.clone(),
                seen.content_length.to_string(),
                modified,
                seen.etag.clone().unwrap_or_default(),
                seen.content_type.clone().unwrap_or_default(),
                digest(ChecksumAlgorithm::Md5),
                digest(ChecksumAlgorithm::Sha1),
                digest(ChecksumAlgorithm::Sha256),
            ];
            (name, fields)
        }
        BlobVersionState::Materialized { blob_hash, .. }
            if blob_hash == blake3::hash(b"").as_bytes() =>
        {
            let mut fields: [String; 8] = Default::default();
            fields[1] = "0".to_string();
            (EMPTY_FILE.to_string(), fields)
        }
        BlobVersionState::Materialized { .. } => return Err("stored bytes"),
        BlobVersionState::PendingContent { .. } => return Err("pending content"),
        BlobVersionState::Deleted => return Err("delete marker"),
    };
    let mut line = vec![key.to_string()];
    line.extend(fields);
    line.push(metadata);
    if line.iter().any(|field| field.contains(['\t', '\n', '\r'])) {
        return Err("tab or line break in a field");
    }
    Ok((name, line.join("\t") + "\n"))
}

#[cfg(test)]
#[path = "export_tests.rs"]
mod tests;
