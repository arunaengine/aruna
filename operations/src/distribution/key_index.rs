//! Keeps one tantivy key index per range of a distributed bucket, in a directory per index
//! generation.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// A raw key field serves exact deletes, key order and prefixes; a tokenized name field splits
// keys at path and name separators. Term queries find matches; the key fast field orders them.

use std::collections::{BTreeSet, BinaryHeap, HashMap, VecDeque};
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tantivy::directory::MmapDirectory;
use tantivy::query::{BooleanQuery, EnableScoring, Occur, Query, TermQuery};
use tantivy::schema::{FAST, Field, IndexRecordOption, STORED, STRING, Schema, TEXT};
use tantivy::tokenizer::TokenStream;
use tantivy::{
    DocSet, Index, IndexReader, IndexWriter, ReloadPolicy, Searcher, TERMINATED, TantivyDocument,
    TantivyError, Term,
};
use thiserror::Error;
use tokio::sync::Semaphore;
use ulid::Ulid;

const KEY_FIELD: &str = "key";
const NAME_FIELD: &str = "name";
const SIZE_FIELD: &str = "size";
const VERSION_FIELD: &str = "version";
/// The key of a searchable version, so metadata queries walk only those keys.
const META_KEY_FIELD: &str = "meta_key";
/// The free text tokens of a searchable version, one indexed term each.
const META_FIELD: &str = "meta";
/// The `column=value` terms of a searchable version, one indexed term each.
const EXACT_FIELD: &str = "exact";
/// Writer memory per batch; the smallest budget tantivy accepts for one thread.
const WRITER_BYTES: usize = 15_000_000;
/// Open indexes kept for queries and writes; the cache is cleared when it is full.
const OPEN_INDEXES: usize = 256;
/// Writers kept open between batches, each with its writer memory; the oldest idle one goes.
const OPEN_WRITERS: usize = 8;
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
    /// The search terms of a searchable version.
    pub terms: Option<MetaTerms>,
}

/// Free text tokens and exact `column=value` terms of one version's metadata.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetaTerms {
    pub text: BTreeSet<String>,
    pub exact: BTreeSet<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KeyChange {
    Put(IndexedKey),
    Remove(String),
}

/// Keys between `start` and `end` in key order; with `names`, only keys that hold every name
/// token of that text, and with `meta`, only searchable keys holding every term of the query.
/// A query with terms returns at most `limit` matches; one without examines at most `limit` keys
/// per index segment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyQuery {
    pub start: Bound<String>,
    pub end: Bound<String>,
    pub names: Option<String>,
    pub meta: Option<MetaTerms>,
    pub limit: usize,
}

/// The live matching keys of the examined part of a window, in key order.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct KeyPage {
    pub keys: Vec<String>,
    /// Keys looked at: the matches of a query with terms, every walked key of one without.
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
    meta_key: Field,
    meta: Field,
    exact: Field,
}

fn schema() -> Schema {
    let mut builder = Schema::builder();
    builder.add_text_field(KEY_FIELD, STRING | FAST | STORED);
    builder.add_text_field(NAME_FIELD, TEXT);
    builder.add_u64_field(SIZE_FIELD, STORED);
    builder.add_text_field(VERSION_FIELD, STORED);
    builder.add_text_field(META_KEY_FIELD, STRING);
    builder.add_text_field(META_FIELD, STRING);
    builder.add_text_field(EXACT_FIELD, STRING);
    builder.build()
}

fn fields(schema: &Schema) -> Result<Fields, KeyIndexError> {
    Ok(Fields {
        key: schema.get_field(KEY_FIELD)?,
        name: schema.get_field(NAME_FIELD)?,
        size: schema.get_field(SIZE_FIELD)?,
        version: schema.get_field(VERSION_FIELD)?,
        meta_key: schema.get_field(META_KEY_FIELD)?,
        meta: schema.get_field(META_FIELD)?,
        exact: schema.get_field(EXACT_FIELD)?,
    })
}

#[derive(Clone)]
struct OpenIndex {
    index: Index,
    reader: IndexReader,
    /// Kept between batches, so a batch neither opens a writer nor waits for merges.
    writer: Arc<Mutex<Option<IndexWriter>>>,
}

/// The range indexes of this node below one root directory:
/// `{root}/{set id}/{range id}/{generation}`.
pub struct RangeIndexes {
    root: PathBuf,
    open: Mutex<HashMap<PathBuf, OpenIndex>>,
    /// Directories whose index holds a writer, the most recently used last.
    writers: Mutex<VecDeque<PathBuf>>,
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
            writers: Mutex::new(VecDeque::new()),
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
            indexes.write_changes(&dir, &open, &changes)
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

    /// Whether a generation's directory holds an index of the current schema; an index of an
    /// older schema counts as missing, so the feed builds the next generation beside it.
    pub async fn exists(
        self: &Arc<Self>,
        set_id: Ulid,
        range_id: Ulid,
        generation: u64,
    ) -> Result<bool, KeyIndexError> {
        let dir = self.directory(set_id, range_id, generation);
        self.blocking(move |indexes| match indexes.open_index(&dir, false) {
            Ok(_) => Ok(true),
            Err(
                KeyIndexError::Missing(_) | KeyIndexError::Tantivy(TantivyError::SchemaError(_)),
            ) => Ok(false),
            Err(error) => Err(error),
        })
        .await
    }

    /// Whether a generation's directory holds an index of an older schema.
    pub async fn outdated(
        self: &Arc<Self>,
        set_id: Ulid,
        range_id: Ulid,
        generation: u64,
    ) -> Result<bool, KeyIndexError> {
        let dir = self.directory(set_id, range_id, generation);
        self.blocking(move |indexes| match indexes.open_index(&dir, false) {
            Err(KeyIndexError::Tantivy(TantivyError::SchemaError(_))) => Ok(true),
            Ok(_) | Err(KeyIndexError::Missing(_)) => Ok(false),
            Err(error) => Err(error),
        })
        .await
    }

    /// Live keys in a generation as of its last commit; a missing generation holds none.
    pub async fn count(
        self: &Arc<Self>,
        set_id: Ulid,
        range_id: Ulid,
        generation: u64,
    ) -> Result<u64, KeyIndexError> {
        let dir = self.directory(set_id, range_id, generation);
        self.blocking(move |indexes| match indexes.open_index(&dir, false) {
            Ok(open) => Ok(open.reader.searcher().num_docs()),
            Err(KeyIndexError::Missing(_)) => Ok(0),
            Err(error) => Err(error),
        })
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
                // The set's directory goes with its last range; another range keeps it.
                if let Some(set_dir) = dir.parent() {
                    match std::fs::remove_dir(set_dir) {
                        Err(error) if error.kind() == std::io::ErrorKind::DirectoryNotEmpty => {}
                        other => other.or_else(ignore_missing)?,
                    }
                }
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
        let index = OpenIndex {
            index,
            reader,
            writer: Arc::new(Mutex::new(None)),
        };
        open.insert(dir.to_path_buf(), index.clone());
        Ok(index)
    }

    /// Applies `changes` with the index's kept writer and commits them, so they are durable and
    /// visible to searches when this returns. A failed batch drops the writer.
    fn write_changes(
        &self,
        dir: &Path,
        open: &OpenIndex,
        changes: &[KeyChange],
    ) -> Result<(), KeyIndexError> {
        if changes.is_empty() {
            return Ok(());
        }
        let mut slot = (open.writer.lock()).unwrap_or_else(|poison| poison.into_inner());
        if slot.is_none() {
            *slot = Some(open.index.writer_with_num_threads(1, WRITER_BYTES)?);
        }
        self.keep_writer(dir);
        let written = match slot.as_mut() {
            Some(writer) => write_batch(writer, open, changes),
            None => Ok(()),
        };
        if written.is_err() {
            *slot = None;
        }
        written
    }

    /// Marks the writer of `dir` as the most recently used and drops the oldest idle writers
    /// beyond [`OPEN_WRITERS`]; a writer in use stays until a later batch.
    fn keep_writer(&self, dir: &Path) {
        let mut writers = (self.writers.lock()).unwrap_or_else(|poison| poison.into_inner());
        writers.retain(|kept| kept != dir);
        writers.push_back(dir.to_path_buf());
        let open = self
            .open
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let mut kept = writers.len();
        writers.retain(|candidate| {
            if kept <= OPEN_WRITERS || candidate == dir {
                return true;
            }
            let Some(index) = open.get(candidate) else {
                kept -= 1;
                return false;
            };
            match index.writer.try_lock() {
                Ok(mut writer) => {
                    *writer = None;
                    kept -= 1;
                    false
                }
                Err(_) => true,
            }
        });
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

fn write_batch(
    writer: &mut IndexWriter,
    open: &OpenIndex,
    changes: &[KeyChange],
) -> Result<(), KeyIndexError> {
    let fields = fields(&open.index.schema())?;
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
            if let Some(terms) = &entry.terms {
                document.add_text(fields.meta_key, &entry.key);
                for token in &terms.text {
                    document.add_text(fields.meta, token);
                }
                for term in &terms.exact {
                    document.add_text(fields.exact, term);
                }
            }
            writer.add_document(document)?;
        }
    }
    writer.commit()?;
    open.reader.reload()?;
    Ok(())
}

/// The live keys of the window that hold every term of the query, in key order. A query with
/// terms is answered from the inverted index; one without walks the key terms.
fn search_keys(open: &OpenIndex, query: &KeyQuery) -> Result<KeyPage, KeyIndexError> {
    let fields = fields(&open.index.schema())?;
    let mut analyzer = open.index.tokenizer_for_field(fields.name)?;
    let mut terms = Vec::new();
    if let Some(names) = &query.names {
        let mut stream = analyzer.token_stream(names);
        while let Some(token) = stream.next() {
            terms.push(Term::from_field_text(fields.name, &token.text));
        }
    }
    if let Some(meta) = &query.meta {
        let text = meta.text.iter().map(|token| (fields.meta, token));
        let exact = meta.exact.iter().map(|term| (fields.exact, term));
        terms.extend(
            text.chain(exact)
                .map(|(field, term)| Term::from_field_text(field, term)),
        );
    }
    let searcher = open.reader.searcher();
    match terms.is_empty() {
        true => walk_keys(&searcher, fields, query),
        false => matched_keys(&searcher, terms, query),
    }
}

/// The first `query.limit` live keys of the window that hold every term. Each segment keeps
/// its smallest matching key ordinals, so the work follows the matches, never the range size.
fn matched_keys(
    searcher: &Searcher,
    terms: Vec<Term>,
    query: &KeyQuery,
) -> Result<KeyPage, KeyIndexError> {
    let clauses = (terms.into_iter())
        .map(|term| {
            let query: Box<dyn Query> = Box::new(TermQuery::new(term, IndexRecordOption::Basic));
            (Occur::Must, query)
        })
        .collect();
    let weight =
        BooleanQuery::new(clauses).weight(EnableScoring::disabled_from_searcher(searcher))?;
    let limit = query.limit.max(1);
    let mut found = BTreeSet::new();
    for segment in searcher.segment_readers() {
        let Some(column) = segment.fast_fields().str(KEY_FIELD)? else {
            continue;
        };
        // Key ordinals follow key order, so the window is one ordinal interval.
        let window =
            (column.dictionary()).term_bounds_to_ord(query.start.as_ref(), query.end.as_ref())?;
        // One more than the limit tells whether the window continues after the page.
        let mut smallest = BinaryHeap::with_capacity(limit + 2);
        let mut scorer = weight.scorer(segment, 1.0)?;
        let mut doc = scorer.doc();
        while doc != TERMINATED {
            let ordinal = (!segment.is_deleted(doc))
                .then(|| column.ords().first(doc))
                .flatten();
            if let Some(ordinal) = ordinal.filter(|ordinal| window.contains(ordinal)) {
                smallest.push(ordinal);
                if smallest.len() > limit + 1 {
                    smallest.pop();
                }
            }
            doc = scorer.advance();
        }
        for ordinal in smallest {
            let mut key = String::new();
            column.ord_to_str(ordinal, &mut key)?;
            found.insert(key);
        }
    }
    let more = found.len() > limit;
    let keys: Vec<String> = found.into_iter().take(limit).collect();
    let last = more.then(|| keys.last().cloned()).flatten();
    Ok(KeyPage {
        examined: keys.len(),
        keys,
        last,
    })
}

/// Walks the key terms of every segment in key order, each at most `query.limit` terms, so
/// the work is bounded by the limit and never by the range's size. Keys up to the smallest
/// segment end are examined everywhere; the page continues after that key.
fn walk_keys(
    searcher: &Searcher,
    fields: Fields,
    query: &KeyQuery,
) -> Result<KeyPage, KeyIndexError> {
    let limit = query.limit.max(1);
    let mut seen = std::collections::BTreeMap::<String, bool>::new();
    let mut end: Option<String> = None;
    let walked = match query.meta {
        Some(_) => fields.meta_key,
        None => fields.key,
    };
    for segment in searcher.segment_readers() {
        let inverted = segment.inverted_index(walked)?;
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
            terms: None,
        })
    }

    fn query(start: Bound<&str>, end: Bound<&str>, names: Option<&str>, limit: usize) -> KeyQuery {
        KeyQuery {
            start: start.map(str::to_string),
            end: end.map(str::to_string),
            names: names.map(str::to_string),
            meta: None,
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

    // A rare name far into the range is the first page's hit, with no empty pages before it.
    #[tokio::test]
    async fn rare_name_found() {
        let dir = tempfile::tempdir().unwrap();
        let indexes = Arc::new(RangeIndexes::new(dir.path()));
        let mut changes: Vec<_> = (0..3_000).map(|n| put(&format!("k/{n:05}.bam"))).collect();
        changes.push(put("k/02990.fastq"));
        indexes.apply(set(), range(), 1, changes).await.unwrap();

        let names = query(Bound::Unbounded, Bound::Unbounded, Some("fastq"), 2);
        let first = indexes.search(set(), range(), 1, names).await.unwrap();
        assert_eq!(first.keys, ["k/02990.fastq"]);
        assert_eq!(first.last, None, "an empty rest means no more hits");
    }

    // Every name token must match, and a page continues right after its last hit.
    #[tokio::test]
    async fn names_paged_hits() {
        let dir = tempfile::tempdir().unwrap();
        let indexes = Arc::new(RangeIndexes::new(dir.path()));
        let keys = [
            "a/s1.fastq",
            "b/s1.bam",
            "c/s1.fastq",
            "d/s2.fastq",
            "e/s1.fastq",
        ];
        let changes = keys.map(put).to_vec();
        indexes.apply(set(), range(), 1, changes).await.unwrap();

        let both = query(Bound::Unbounded, Bound::Unbounded, Some("s1 FASTQ"), 2);
        let first = indexes.search(set(), range(), 1, both).await.unwrap();
        assert_eq!(first.keys, ["a/s1.fastq", "c/s1.fastq"]);
        assert_eq!(first.last.as_deref(), Some("c/s1.fastq"));
        let rest = query(
            Bound::Excluded("c/s1.fastq"),
            Bound::Unbounded,
            Some("s1 fastq"),
            2,
        );
        let second = indexes.search(set(), range(), 1, rest).await.unwrap();
        assert_eq!(second.keys, ["e/s1.fastq"]);
        assert_eq!(second.last, None);

        // Removed and replaced keys stay out of term matches like they do of the key walk.
        let changes = vec![
            KeyChange::Remove("a/s1.fastq".to_string()),
            put("c/s1.fastq"),
        ];
        indexes.apply(set(), range(), 1, changes).await.unwrap();
        let again = query(Bound::Unbounded, Bound::Unbounded, Some("s1 fastq"), 10);
        let found = indexes.search(set(), range(), 1, again).await.unwrap();
        assert_eq!(found.keys, ["c/s1.fastq", "e/s1.fastq"]);
    }

    // An index written with an older schema reads as missing, so the feed builds it again.
    #[tokio::test]
    async fn older_schema_outdated() {
        let dir = tempfile::tempdir().unwrap();
        let indexes = Arc::new(RangeIndexes::new(dir.path()));
        let older = indexes.directory(set(), range(), 1);
        std::fs::create_dir_all(&older).unwrap();
        let mut builder = Schema::builder();
        builder.add_text_field(KEY_FIELD, STRING | FAST | STORED);
        builder.add_text_field(META_FIELD, STORED);
        Index::create_in_dir(&older, builder.build()).unwrap();

        assert!(!indexes.exists(set(), range(), 1).await.unwrap());
        assert!(indexes.outdated(set(), range(), 1).await.unwrap());
        let all = query(Bound::Unbounded, Bound::Unbounded, None, 10);
        assert!(indexes.search(set(), range(), 1, all).await.is_err());
        indexes
            .apply(set(), range(), 2, vec![put("k")])
            .await
            .unwrap();
        assert!(!indexes.outdated(set(), range(), 2).await.unwrap());
    }

    fn described(key: &str, text: &[&str], exact: &[&str]) -> KeyChange {
        let set = |terms: &[&str]| terms.iter().map(|term| term.to_string()).collect();
        KeyChange::Put(IndexedKey {
            terms: Some(MetaTerms {
                text: set(text),
                exact: set(exact),
            }),
            ..match put(key) {
                KeyChange::Put(indexed) => indexed,
                KeyChange::Remove(_) => unreachable!(),
            }
        })
    }

    fn meta(text: &[&str], exact: &[&str], limit: usize, after: Option<&str>) -> KeyQuery {
        let set = |terms: &[&str]| terms.iter().map(|term| term.to_string()).collect();
        KeyQuery {
            meta: Some(MetaTerms {
                text: set(text),
                exact: set(exact),
            }),
            ..query(
                after.map_or(Bound::Unbounded, Bound::Excluded),
                Bound::Unbounded,
                None,
                limit,
            )
        }
    }

    // Metadata queries match the indexed terms of searchable keys and continue after the last
    // hit of a full page.
    #[tokio::test]
    async fn metadata_terms_match() {
        let dir = tempfile::tempdir().unwrap();
        let indexes = Arc::new(RangeIndexes::new(dir.path()));
        let changes = vec![
            described(
                "a/",
                &["escherichia", "coli"],
                &["taxid=511145", "species_taxid=562"],
            ),
            put("a/genomic.fna"),
            described("b/", &["salmonella"], &["taxid=99287"]),
            described(
                "c/",
                &["escherichia", "coli"],
                &["taxid=562", "species_taxid=562"],
            ),
        ];
        indexes.apply(set(), range(), 1, changes).await.unwrap();
        let search = |query| indexes.search(set(), range(), 1, query);

        let coli = search(meta(&["coli"], &["species_taxid=562"], 10, None))
            .await
            .unwrap();
        assert_eq!(coli.keys, ["a/", "c/"]);
        let exact = search(meta(&[], &["taxid=562"], 10, None)).await.unwrap();
        assert_eq!(exact.keys, ["c/"]);
        // `escherichia` is in two of three folders; a page stops at its limit of hits.
        let first = search(meta(&["escherichia"], &[], 1, None)).await.unwrap();
        assert_eq!(first.keys, ["a/"]);
        assert_eq!(first.last.as_deref(), Some("a/"));
        let rest = search(meta(&["escherichia"], &[], 1, Some("a/")))
            .await
            .unwrap();
        assert_eq!((rest.keys, rest.last), (vec!["c/".to_string()], None));
        // A multiline exact value is one term; its lines never become terms of their own.
        let changes = vec![described("d/", &[], &["note=x\ntaxid=562"])];
        indexes.apply(set(), range(), 1, changes).await.unwrap();
        let split = search(meta(&[], &["taxid=562"], 10, None)).await.unwrap();
        assert_eq!(split.keys, ["c/"]);
        let whole = search(meta(&[], &["note=x\ntaxid=562"], 10, None))
            .await
            .unwrap();
        assert_eq!(whole.keys, ["d/"]);
        // A key replaced without terms or removed no longer matches.
        let changes = vec![put("a/"), KeyChange::Remove("c/".to_string())];
        indexes.apply(set(), range(), 1, changes).await.unwrap();
        let none = search(meta(&["coli"], &[], 10, None)).await.unwrap();
        assert!(none.keys.is_empty());
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
        assert!(
            !dir.path().join(set().to_string()).exists(),
            "no empty set directory"
        );
    }
}
