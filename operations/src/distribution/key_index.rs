//! Keeps one tantivy key index per range of a distributed bucket, in a directory per index
//! generation: a raw key field for exact deletes, key order and prefixes, and a tokenized
//! name field that splits keys at path and name separators.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tantivy::directory::MmapDirectory;
use tantivy::schema::{FAST, Field, IndexRecordOption, STORED, STRING, Schema, TEXT};
use tantivy::tokenizer::TokenStream;
use tantivy::{
    DocSet, Index, IndexReader, IndexWriter, ReloadPolicy, TERMINATED, TantivyDocument, Term,
};
use thiserror::Error;
use tokio::sync::Semaphore;
use ulid::Ulid;

const KEY_FIELD: &str = "key";
const NAME_FIELD: &str = "name";
const SIZE_FIELD: &str = "size";
const VERSION_FIELD: &str = "version";
/// Writer memory per batch; the smallest budget tantivy accepts for one thread.
const WRITER_BYTES: usize = 15_000_000;
/// Open indexes kept for queries and writes; the cache is cleared when it is full.
const OPEN_INDEXES: usize = 256;
/// Index tasks that run at once on the blocking pool.
const INDEX_PERMITS: usize = 4;

#[derive(Debug, Error)]
pub enum KeyIndexError {
    #[error(transparent)]
    Tantivy(#[from] tantivy::TantivyError),
    #[error(transparent)]
    Directory(#[from] tantivy::directory::error::OpenDirectoryError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("range index {0} does not exist")]
    Missing(PathBuf),
    #[error("range index task failed: {0}")]
    Task(String),
}

/// One live key of a range: its current version and size.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexedKey {
    pub key: String,
    pub size: u64,
    pub version_id: Ulid,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KeyChange {
    Put(IndexedKey),
    Remove(String),
}

/// Keys between `start` and `end` in key order, examining at most `limit` keys per index
/// segment; with `names`, only keys that hold every name token of that text.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyQuery {
    pub start: Bound<String>,
    pub end: Bound<String>,
    pub names: Option<String>,
    pub limit: usize,
}

/// The live matching keys of the examined part of a window, in key order.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct KeyPage {
    pub keys: Vec<String>,
    /// Keys looked at, live, deleted or not matching.
    pub examined: usize,
    /// The last examined key when the window continues after it; `None` once it is read.
    pub last: Option<String>,
}

#[derive(Clone, Copy)]
struct Fields {
    key: Field,
    name: Field,
    size: Field,
    version: Field,
}

fn schema() -> Schema {
    let mut builder = Schema::builder();
    builder.add_text_field(KEY_FIELD, STRING | FAST | STORED);
    builder.add_text_field(NAME_FIELD, TEXT);
    builder.add_u64_field(SIZE_FIELD, STORED);
    builder.add_text_field(VERSION_FIELD, STORED);
    builder.build()
}

fn fields(schema: &Schema) -> Result<Fields, KeyIndexError> {
    Ok(Fields {
        key: schema.get_field(KEY_FIELD)?,
        name: schema.get_field(NAME_FIELD)?,
        size: schema.get_field(SIZE_FIELD)?,
        version: schema.get_field(VERSION_FIELD)?,
    })
}

#[derive(Clone)]
struct OpenIndex {
    index: Index,
    reader: IndexReader,
}

/// The range indexes of this node below one root directory:
/// `{root}/{set id}/{range id}/{generation}`.
pub struct RangeIndexes {
    root: PathBuf,
    open: Mutex<HashMap<PathBuf, OpenIndex>>,
    permits: Arc<Semaphore>,
}

impl std::fmt::Debug for RangeIndexes {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RangeIndexes")
            .field("root", &self.root)
            .finish()
    }
}

impl RangeIndexes {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            open: Mutex::new(HashMap::new()),
            permits: Arc::new(Semaphore::new(INDEX_PERMITS)),
        }
    }

    fn range_dir(&self, set_id: Ulid, range_id: Ulid) -> PathBuf {
        self.root
            .join(set_id.to_string())
            .join(range_id.to_string())
    }

    pub fn directory(&self, set_id: Ulid, range_id: Ulid, generation: u64) -> PathBuf {
        self.range_dir(set_id, range_id)
            .join(generation.to_string())
    }

    /// Applies `changes` in order to a generation, creating it when missing, and commits.
    pub async fn apply(
        self: &Arc<Self>,
        set_id: Ulid,
        range_id: Ulid,
        generation: u64,
        changes: Vec<KeyChange>,
    ) -> Result<(), KeyIndexError> {
        let dir = self.directory(set_id, range_id, generation);
        self.blocking(move |indexes| {
            let open = indexes.open_index(&dir, true)?;
            write_changes(&open, &changes)
        })
        .await
    }

    /// Searches an existing generation; a missing one is an error, never an empty index.
    pub async fn search(
        self: &Arc<Self>,
        set_id: Ulid,
        range_id: Ulid,
        generation: u64,
        query: KeyQuery,
    ) -> Result<KeyPage, KeyIndexError> {
        let dir = self.directory(set_id, range_id, generation);
        self.blocking(move |indexes| {
            let open = indexes.open_index(&dir, false)?;
            search_keys(&open, &query)
        })
        .await
    }

    /// Whether a generation's directory holds an index.
    pub async fn exists(
        self: &Arc<Self>,
        set_id: Ulid,
        range_id: Ulid,
        generation: u64,
    ) -> Result<bool, KeyIndexError> {
        let dir = self.directory(set_id, range_id, generation);
        self.blocking(move |_| Ok(dir.join("meta.json").exists()))
            .await
    }

    /// Removes every generation of a range except `keep`.
    pub async fn prune(
        self: &Arc<Self>,
        set_id: Ulid,
        range_id: Ulid,
        keep: Option<u64>,
    ) -> Result<(), KeyIndexError> {
        let dir = self.range_dir(set_id, range_id);
        let keep = keep.map(|generation| generation.to_string());
        self.blocking(move |indexes| {
            let entries = match std::fs::read_dir(&dir) {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(error) => return Err(error.into()),
            };
            for entry in entries {
                let entry = entry?;
                if keep.as_deref() == entry.file_name().to_str() {
                    continue;
                }
                indexes.forget(&entry.path());
                std::fs::remove_dir_all(entry.path())?;
            }
            if keep.is_none() {
                std::fs::remove_dir_all(&dir).or_else(ignore_missing)?;
            }
            Ok(())
        })
        .await
    }

    async fn blocking<T, F>(self: &Arc<Self>, work: F) -> Result<T, KeyIndexError>
    where
        T: Send + 'static,
        F: FnOnce(&RangeIndexes) -> Result<T, KeyIndexError> + Send + 'static,
    {
        let permit = (self.permits.clone().acquire_owned().await)
            .map_err(|error| KeyIndexError::Task(error.to_string()))?;
        let indexes = self.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            work(&indexes)
        })
        .await
        .map_err(|error| KeyIndexError::Task(error.to_string()))?
    }

    fn open_index(&self, dir: &Path, create: bool) -> Result<OpenIndex, KeyIndexError> {
        let mut open = self
            .open
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if let Some(index) = open.get(dir) {
            return Ok(index.clone());
        }
        if !dir.join("meta.json").exists() {
            if !create {
                return Err(KeyIndexError::Missing(dir.to_path_buf()));
            }
            std::fs::create_dir_all(dir)?;
        }
        let index = Index::open_or_create(MmapDirectory::open(dir)?, schema())?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        if open.len() >= OPEN_INDEXES {
            open.clear();
        }
        let index = OpenIndex { index, reader };
        open.insert(dir.to_path_buf(), index.clone());
        Ok(index)
    }

    fn forget(&self, dir: &Path) {
        let mut open = self
            .open
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        open.remove(dir);
    }
}

fn ignore_missing(error: std::io::Error) -> std::io::Result<()> {
    match error.kind() {
        std::io::ErrorKind::NotFound => Ok(()),
        _ => Err(error),
    }
}

fn write_changes(open: &OpenIndex, changes: &[KeyChange]) -> Result<(), KeyIndexError> {
    if changes.is_empty() {
        return Ok(());
    }
    let fields = fields(&open.index.schema())?;
    let mut writer: IndexWriter = open.index.writer_with_num_threads(1, WRITER_BYTES)?;
    for change in changes {
        let key = match change {
            KeyChange::Put(entry) => &entry.key,
            KeyChange::Remove(key) => key,
        };
        // A delete only removes documents added before it, so a put replaces the key.
        writer.delete_term(Term::from_field_text(fields.key, key));
        if let KeyChange::Put(entry) = change {
            let mut document = TantivyDocument::default();
            document.add_text(fields.key, &entry.key);
            document.add_text(fields.name, &entry.key);
            document.add_u64(fields.size, entry.size);
            document.add_text(fields.version, entry.version_id.to_string());
            writer.add_document(document)?;
        }
    }
    writer.commit()?;
    writer.wait_merging_threads()?;
    open.reader.reload()?;
    Ok(())
}

/// Walks the key terms of every segment in key order, each at most `query.limit` terms, so
/// the work is bounded by the limit and never by the range's size. Keys up to the smallest
/// segment end are examined everywhere; the page continues after that key.
fn search_keys(open: &OpenIndex, query: &KeyQuery) -> Result<KeyPage, KeyIndexError> {
    let fields = fields(&open.index.schema())?;
    let mut analyzer = open.index.tokenizer_for_field(fields.name)?;
    let mut tokens = |text: &str| {
        let mut stream = analyzer.token_stream(text);
        let mut found = Vec::new();
        while let Some(token) = stream.next() {
            found.push(token.text.clone());
        }
        found
    };
    let wanted = query.names.as_deref().map(&mut tokens);
    let limit = query.limit.max(1);
    let searcher = open.reader.searcher();
    let mut seen = std::collections::BTreeMap::<String, bool>::new();
    let mut end: Option<String> = None;
    for segment in searcher.segment_readers() {
        let inverted = segment.inverted_index(fields.key)?;
        let mut range = inverted.terms().range();
        range = match &query.start {
            Bound::Included(start) => range.ge(start.as_bytes()),
            Bound::Excluded(start) => range.gt(start.as_bytes()),
            Bound::Unbounded => range,
        };
        range = match &query.end {
            Bound::Included(stop) => range.le(stop.as_bytes()),
            Bound::Excluded(stop) => range.lt(stop.as_bytes()),
            Bound::Unbounded => range,
        };
        let mut stream = range.into_stream()?;
        let mut read = 0usize;
        while read < limit && stream.advance() {
            read += 1;
            let key = String::from_utf8_lossy(stream.key()).into_owned();
            let postings = IndexRecordOption::Basic;
            let mut docs = inverted.read_postings_from_terminfo(stream.value(), postings)?;
            let mut alive = false;
            while docs.doc() != TERMINATED && !alive {
                alive = !segment.is_deleted(docs.doc());
                docs.advance();
            }
            *seen.entry(key.clone()).or_default() |= alive;
            if read == limit {
                end = Some(end.map_or(key.clone(), |end| end.min(key)));
            }
        }
    }
    if let Some(end) = &end {
        seen.retain(|key, _| key <= end);
    }
    let examined = seen.len();
    let keys = (seen.into_iter())
        .filter(|(_, alive)| *alive)
        .map(|(key, _)| key)
        .filter(|key| {
            let Some(wanted) = &wanted else {
                return true;
            };
            let found = tokens(key);
            wanted.iter().all(|token| found.contains(token))
        })
        .collect();
    Ok(KeyPage {
        keys,
        examined,
        last: end,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set() -> Ulid {
        Ulid::from_bytes([1; 16])
    }

    fn range() -> Ulid {
        Ulid::from_bytes([2; 16])
    }

    fn put(key: &str) -> KeyChange {
        KeyChange::Put(IndexedKey {
            key: key.to_string(),
            size: 1,
            version_id: Ulid::from_bytes([3; 16]),
        })
    }

    fn query(start: Bound<&str>, end: Bound<&str>, names: Option<&str>, limit: usize) -> KeyQuery {
        KeyQuery {
            start: start.map(str::to_string),
            end: end.map(str::to_string),
            names: names.map(str::to_string),
            limit,
        }
    }

    #[tokio::test]
    async fn searches_keys_ordered() {
        let dir = tempfile::tempdir().unwrap();
        let indexes = Arc::new(RangeIndexes::new(dir.path()));
        let keys = [
            "run-2/reads.fastq",
            "run-1/sample_a.fastq",
            "run-1/sample_b.bam",
        ];
        let changes = keys.iter().map(|key| put(key)).collect();
        indexes.apply(set(), range(), 1, changes).await.unwrap();

        let all = query(Bound::Unbounded, Bound::Unbounded, None, 10);
        let found = indexes.search(set(), range(), 1, all).await.unwrap().keys;
        assert_eq!(
            found,
            [
                "run-1/sample_a.fastq",
                "run-1/sample_b.bam",
                "run-2/reads.fastq"
            ]
        );

        let prefix = query(
            Bound::Included("run-1/"),
            Bound::Excluded("run-10"),
            None,
            10,
        );
        let found = indexes
            .search(set(), range(), 1, prefix)
            .await
            .unwrap()
            .keys;
        assert_eq!(found.len(), 2, "the prefix window holds both run-1 keys");

        let names = query(Bound::Unbounded, Bound::Unbounded, Some("FASTQ"), 10);
        let found = indexes.search(set(), range(), 1, names).await.unwrap().keys;
        assert_eq!(found, ["run-1/sample_a.fastq", "run-2/reads.fastq"]);

        let after = query(
            Bound::Excluded("run-1/sample_a.fastq"),
            Bound::Unbounded,
            None,
            1,
        );
        let found = indexes.search(set(), range(), 1, after).await.unwrap();
        assert_eq!(
            found.keys,
            ["run-1/sample_b.bam"],
            "continues after the key, bounded"
        );
        assert_eq!(found.last.as_deref(), Some("run-1/sample_b.bam"));
    }

    #[tokio::test]
    async fn replaces_and_removes() {
        let dir = tempfile::tempdir().unwrap();
        let indexes = Arc::new(RangeIndexes::new(dir.path()));
        let changes = vec![put("a/x"), put("a/x"), put("b/y")];
        indexes.apply(set(), range(), 1, changes).await.unwrap();
        let all = query(Bound::Unbounded, Bound::Unbounded, None, 10);
        let found = indexes
            .search(set(), range(), 1, all.clone())
            .await
            .unwrap();
        assert_eq!(
            found.keys,
            ["a/x", "b/y"],
            "a replayed put keeps one document"
        );

        let changes = vec![KeyChange::Remove("a/x".to_string())];
        indexes.apply(set(), range(), 1, changes).await.unwrap();
        let found = indexes.search(set(), range(), 1, all).await.unwrap();
        assert_eq!(found.keys, ["b/y"]);
    }

    // Keys that do not match still count, so a query with few hits stops at its limit.
    #[tokio::test]
    async fn examined_keys_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let indexes = Arc::new(RangeIndexes::new(dir.path()));
        let changes = ["a.bam", "b.bam", "c.bam", "d.fastq"].map(put).to_vec();
        indexes.apply(set(), range(), 1, changes).await.unwrap();

        let names = query(Bound::Unbounded, Bound::Unbounded, Some("fastq"), 2);
        let first = indexes.search(set(), range(), 1, names).await.unwrap();
        assert_eq!((first.keys.len(), first.examined), (0, 2));
        assert_eq!(first.last.as_deref(), Some("b.bam"));
        let rest = query(Bound::Excluded("b.bam"), Bound::Unbounded, Some("fastq"), 2);
        let second = indexes.search(set(), range(), 1, rest).await.unwrap();
        assert_eq!(second.keys, ["d.fastq"]);
        assert_eq!(second.last.as_deref(), Some("d.fastq"));
    }

    #[tokio::test]
    async fn missing_generation_fails() {
        let dir = tempfile::tempdir().unwrap();
        let indexes = Arc::new(RangeIndexes::new(dir.path()));
        let all = query(Bound::Unbounded, Bound::Unbounded, None, 10);
        let missing = indexes.search(set(), range(), 7, all.clone()).await;
        assert!(matches!(missing, Err(KeyIndexError::Missing(_))));

        indexes
            .apply(set(), range(), 1, vec![put("k")])
            .await
            .unwrap();
        indexes
            .apply(set(), range(), 2, vec![put("k")])
            .await
            .unwrap();
        indexes.prune(set(), range(), Some(2)).await.unwrap();
        assert!(!indexes.exists(set(), range(), 1).await.unwrap());
        assert!(indexes.exists(set(), range(), 2).await.unwrap());
        indexes.prune(set(), range(), None).await.unwrap();
        assert!(indexes.search(set(), range(), 2, all).await.is_err());
    }
}
