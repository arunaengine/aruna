//! Describes a table import: a CSV or TSV list whose rows name files or folders to import, with
//! the columns kept as object metadata and the columns that search may match.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, Read};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::structs::checksum::ChecksumAlgorithm;

/// What a row says about its file: size, checksums and modification time.
type Described = (
    Option<u64>,
    Vec<(ChecksumAlgorithm, Vec<u8>)>,
    Option<SystemTime>,
);

/// Table bytes one step reads; a header and every row must fit into one chunk.
pub const TABLE_CHUNK: usize = 256 * 1024;
/// Metadata entry naming the columns free text search matches, comma separated.
pub const SEARCH_TEXT_KEY: &str = "aruna-search-text";
/// Metadata entry naming the columns exact filters match, comma separated.
pub const SEARCH_EXACT_KEY: &str = "aruna-search-exact";
/// Metadata entries one object carries at most, the two selection entries included.
pub const MAX_METADATA_ENTRIES: usize = 128;
/// Bytes of one metadata key at most.
pub const MAX_METADATA_KEY: usize = 128;
/// Bytes of one metadata value at most.
pub const MAX_METADATA_VALUE: usize = 4 * 1024;
/// Column of a record row's source ETag.
pub const RECORD_ETAG: &str = "etag";
/// Column of a record row's content type.
pub const RECORD_CONTENT_TYPE: &str = "content_type";
/// Column of a record row's metadata as a JSON object of strings.
pub const RECORD_METADATA: &str = "metadata";
/// Whether version metadata names columns that metadata search matches.
pub fn searchable(metadata: &std::collections::HashMap<String, String>) -> bool {
    metadata.contains_key(SEARCH_TEXT_KEY) || metadata.contains_key(SEARCH_EXACT_KEY)
}

/// The lowercase alphanumeric words of `text` that free text search matches.
pub fn search_tokens(text: &str) -> Vec<String> {
    (text.split(|c: char| !c.is_alphanumeric()))
        .filter(|word| !word.is_empty() && word.len() <= 40)
        .map(str::to_lowercase)
        .collect()
}

/// The free text tokens and `column=value` terms a version's own selection entries name.
pub fn search_terms(
    metadata: &std::collections::HashMap<String, String>,
) -> (BTreeSet<String>, BTreeSet<String>) {
    let columns = |key: &str| {
        let names = metadata.get(key).map(String::as_str).unwrap_or_default();
        let names: Vec<&str> = names.split(',').filter(|name| !name.is_empty()).collect();
        (names.into_iter()).filter_map(|name| Some((name, metadata.get(name)?)))
    };
    let text = (columns(SEARCH_TEXT_KEY))
        .flat_map(|(_, value)| search_tokens(value))
        .collect();
    let exact = (columns(SEARCH_EXACT_KEY))
        .map(|(name, value)| format!("{name}={value}"))
        .collect();
    (text, exact)
}

/// Splits a metadata query into free text and exact `column=value` terms; a value with spaces
/// is quoted, as in `assembly_level="Complete Genome"`.
pub fn query_terms(query: &str) -> (String, Vec<String>) {
    let (mut text, mut exact) = (Vec::new(), Vec::new());
    let mut rest = query.trim_start();
    while !rest.is_empty() {
        let word_end = rest.find(char::is_whitespace).unwrap_or(rest.len());
        let word = &rest[..word_end];
        match word.split_once('=') {
            Some((column, value)) if !column.is_empty() => {
                let (value, end) = match value.starts_with('"') {
                    true => {
                        let start = word_end - value.len() + 1;
                        let close = quoted_end(&rest[start..]);
                        (
                            &rest[start..start + close],
                            (start + close + 1).min(rest.len()),
                        )
                    }
                    false => (value, word_end),
                };
                exact.push(format!("{}={value}", column.to_ascii_lowercase()));
                rest = &rest[end..];
            }
            _ => {
                text.push(word);
                rest = &rest[word_end..];
            }
        }
        rest = rest.trim_start();
    }
    (text.join(" "), exact)
}

fn quoted_end(text: &str) -> usize {
    text.find('"').unwrap_or(text.len())
}

/// Whether a version's metadata holds every token of `text` and every exact `column=value`.
pub fn metadata_matches(
    metadata: &std::collections::HashMap<String, String>,
    text: &str,
    exact: &[String],
) -> bool {
    if !searchable(metadata) {
        return false;
    }
    let (tokens, terms) = search_terms(metadata);
    search_tokens(text)
        .iter()
        .all(|token| tokens.contains(token))
        && exact.iter().all(|term| terms.contains(term))
}

/// Requests per second one node sends to the read connector unless the import names a rate.
pub const DEFAULT_TABLE_RATE: u32 = 10;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableImport {
    pub source: TableSource,
    #[serde(default)]
    pub format: TableFormat,
    pub rows: TableRows,
    /// Requests per second per node toward the read connector; `None` uses the default.
    #[serde(default)]
    pub rate: Option<u32>,
}

/// Where the table is read: below the job's read connector, or an object stored in Aruna.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TableSource {
    /// A connector path, or a URL below the connector's base.
    Connector(String),
    Object {
        bucket: String,
        key: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableFormat {
    #[serde(default = "comma")]
    pub delimiter: char,
    /// `None` reads quote characters as plain text.
    #[serde(default = "double_quote")]
    pub quote: Option<char>,
    #[serde(default)]
    pub header: TableHeader,
    #[serde(default)]
    pub comment: Option<char>,
    #[serde(default = "empty_null")]
    pub nulls: Vec<String>,
    /// `None` decides by a `.gz` name or the gzip magic bytes.
    #[serde(default)]
    pub gzip: Option<bool>,
}

fn comma() -> char {
    ','
}

fn double_quote() -> Option<char> {
    Some('"')
}

fn empty_null() -> Vec<String> {
    vec![String::new()]
}

impl Default for TableFormat {
    fn default() -> Self {
        Self {
            delimiter: comma(),
            quote: double_quote(),
            header: TableHeader::default(),
            comment: None,
            nulls: empty_null(),
            gzip: None,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TableHeader {
    #[default]
    FirstRow,
    /// The header is this 1-based line; the lines before it are skipped.
    Row(u32),
    /// The last of the leading comment lines, without the comment character.
    LastComment,
    /// The table has no header row; these name its columns.
    Columns(Vec<String>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RowKind {
    /// A row names one file.
    File,
    /// A row names a directory whose files are listed.
    Folder,
    /// A row is a complete object record whose values are trusted, so the source is never
    /// asked: size, time and checksums as for files, plus the `etag`, `content_type` and
    /// `metadata` columns when present. A row without a source writes an empty object.
    Record,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableRows {
    pub kind: RowKind,
    /// Template of the source path or URL, such as `{ftp_path}`.
    pub source: String,
    /// Template of the target key of a file, or the key prefix of a folder.
    pub target: String,
    #[serde(default)]
    pub filter: Vec<RowFilter>,
    /// Columns stored as metadata; `None` keeps every column.
    #[serde(default)]
    pub keep: Option<Vec<String>>,
    #[serde(default)]
    pub text: Vec<String>,
    #[serde(default)]
    pub exact: Vec<String>,
    /// Column of a file's size in bytes; a row with a size is registered without a HEAD.
    #[serde(default)]
    pub size: Option<String>,
    /// Columns of a file's hex checksums, recorded on the reference and checked on reads.
    #[serde(default)]
    pub checksums: Vec<ChecksumColumn>,
    /// Column of a file's modification time: RFC 3339, or a date that is only checked.
    #[serde(default)]
    pub modified: Option<String>,
    /// Further sources of each file: a path or URL template read through another connector.
    #[serde(default)]
    pub mirrors: Vec<MirrorColumn>,
}

/// A template of another source of a row's file, such as `{aws_path}`, below `connector_id`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MirrorColumn {
    pub connector_id: Ulid,
    pub source: String,
}

/// A column holding a file's checksum; `algorithm` is `md5`, `sha1` or `sha256`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChecksumColumn {
    pub algorithm: String,
    pub column: String,
}

fn checksum_algorithm(name: &str) -> Result<ChecksumAlgorithm, TableError> {
    match name {
        "md5" => Ok(ChecksumAlgorithm::Md5),
        "sha1" => Ok(ChecksumAlgorithm::Sha1),
        "sha256" => Ok(ChecksumAlgorithm::Sha256),
        other => Err(TableError::Mapping(format!(
            "unknown checksum algorithm `{other}`"
        ))),
    }
}

/// Keeps rows whose column holds one of the values.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowFilter {
    pub column: String,
    pub any_of: Vec<String>,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum TableError {
    #[error("the table format is invalid: {0}")]
    Format(String),
    #[error("the row mapping is invalid: {0}")]
    Mapping(String),
    #[error("the table header is invalid: {0}")]
    Header(String),
    #[error("a table row exceeds {TABLE_CHUNK} bytes")]
    RowTooLong,
    #[error("the gzip table is invalid: {0}")]
    Gzip(String),
}

/// One part of a template: literal text or a column value.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Part {
    Text(String),
    Column { name: String, basename: bool },
}

fn template_parts(template: &str) -> Result<Vec<Part>, TableError> {
    let invalid = |reason: &str| TableError::Mapping(format!("template `{template}` {reason}"));
    let mut parts = Vec::new();
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        if open > 0 {
            parts.push(Part::Text(rest[..open].to_string()));
        }
        let close = (rest[open..].find('}')).ok_or_else(|| invalid("has an open brace"))?;
        let inner = &rest[open + 1..open + close];
        let (name, basename) = match inner.split_once('|') {
            Some((name, "basename")) => (name, true),
            Some(_) => return Err(invalid("uses an unknown function")),
            None => (inner, false),
        };
        if name.is_empty() || name.contains('{') {
            return Err(invalid("names no column"));
        }
        parts.push(Part::Column {
            name: name.to_string(),
            basename,
        });
        rest = &rest[open + close + 1..];
    }
    if rest.contains('}') {
        return Err(invalid("has a closing brace without an opening one"));
    }
    if !rest.is_empty() {
        parts.push(Part::Text(rest.to_string()));
    }
    Ok(parts)
}

fn ascii_byte(value: char, field: &str) -> Result<u8, TableError> {
    match value.is_ascii() && value != '\n' && value != '\r' {
        true => Ok(value as u8),
        false => Err(TableError::Format(format!(
            "`{field}` must be one ASCII character"
        ))),
    }
}

impl TableImport {
    /// Checks the descriptor before any byte is read.
    pub fn validate(&self) -> Result<(), TableError> {
        let format = &self.format;
        let delimiter = ascii_byte(format.delimiter, "delimiter")?;
        let quote = format
            .quote
            .map(|quote| ascii_byte(quote, "quote"))
            .transpose()?;
        let comment = (format.comment)
            .map(|comment| ascii_byte(comment, "comment"))
            .transpose()?;
        if quote == Some(delimiter) || comment == Some(delimiter) {
            return Err(TableError::Format("the delimiter must differ".to_string()));
        }
        match &format.header {
            TableHeader::Row(0) => {
                return Err(TableError::Format("header rows count from 1".to_string()));
            }
            TableHeader::LastComment if comment.is_none() => {
                let reason = "a comment header needs a comment character";
                return Err(TableError::Format(reason.to_string()));
            }
            TableHeader::Columns(columns) => check_names(columns)?,
            _ => {}
        }
        if self.rate == Some(0) {
            return Err(TableError::Format("the rate must be positive".to_string()));
        }
        let rows = &self.rows;
        let source = template_parts(&rows.source)?;
        let target = template_parts(&rows.target)?;
        for parts in [&source, &target] {
            if !parts.iter().any(|part| matches!(part, Part::Column { .. })) {
                let reason = "source and target templates must name a column";
                return Err(TableError::Mapping(reason.to_string()));
            }
        }
        let described = rows.size.is_some() || rows.modified.is_some();
        if rows.kind == RowKind::Folder && (described || !rows.checksums.is_empty()) {
            let reason = "size, checksums and modified describe file rows only";
            return Err(TableError::Mapping(reason.to_string()));
        }
        if rows.kind == RowKind::Folder && !rows.mirrors.is_empty() {
            let reason = "mirror templates describe file rows only; folders use job mappings";
            return Err(TableError::Mapping(reason.to_string()));
        }
        for mirror in &rows.mirrors {
            template_parts(&mirror.source)?;
        }
        if rows.kind == RowKind::Record && rows.size.is_none() {
            let reason = "record rows need a size column";
            return Err(TableError::Mapping(reason.to_string()));
        }
        let mut algorithms = BTreeSet::new();
        for checksum in &rows.checksums {
            if !algorithms.insert(checksum_algorithm(&checksum.algorithm)?) {
                let reason = format!("checksum `{}` is named twice", checksum.algorithm);
                return Err(TableError::Mapping(reason));
            }
        }
        if let TableSource::Connector(path) = &self.source
            && path.trim().is_empty()
        {
            return Err(TableError::Mapping("the table path is empty".to_string()));
        }
        Ok(())
    }

    pub fn rate(&self) -> u32 {
        self.rate.unwrap_or(DEFAULT_TABLE_RATE).max(1)
    }
}

fn check_names(columns: &[String]) -> Result<(), TableError> {
    let mut seen = BTreeSet::new();
    for column in columns {
        if column.is_empty() || !seen.insert(column.as_str()) {
            let reason = format!("column `{column}` is empty or repeated");
            return Err(TableError::Header(reason));
        }
    }
    Ok(())
}

/// Whether a metadata key travels safely as an `x-amz-meta-*` header.
fn header_safe(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= MAX_METADATA_KEY
        && (key.bytes()).all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
}

/// Whether the table is gzip compressed: as the format says, else by name or magic bytes.
pub fn is_gzip(format: &TableFormat, name: &str, first: &[u8]) -> bool {
    format
        .gzip
        .unwrap_or_else(|| name.ends_with(".gz") || first.starts_with(&[0x1f, 0x8b]))
}

/// The header of a table and the bytes of the first chunk it takes, a leading BOM included.
pub fn read_header(
    format: &TableFormat,
    bytes: &[u8],
    complete: bool,
) -> Result<(Vec<String>, usize), TableError> {
    let start = match bytes.starts_with(b"\xef\xbb\xbf") {
        true => 3,
        false => 0,
    };
    let body = &bytes[start..];
    let line_end = |from: usize| -> Result<usize, TableError> {
        match body[from..].iter().position(|byte| *byte == b'\n') {
            Some(newline) => Ok(from + newline + 1),
            None if complete => Ok(body.len()),
            None => Err(TableError::Header(
                "the header is not in the first chunk".to_string(),
            )),
        }
    };
    let (columns, used) = match &format.header {
        TableHeader::Columns(columns) => (columns.clone(), 0),
        TableHeader::FirstRow | TableHeader::Row(_) => {
            let mut skip = 0;
            if let TableHeader::Row(row) = format.header {
                for _ in 1..row {
                    skip = line_end(skip)?;
                }
            }
            let (rows, _) = parse_rows(format, None, &body[skip..], complete)?;
            let Some((Ok(fields), end)) = rows.into_iter().next() else {
                return Err(TableError::Header(
                    "the table has no header row".to_string(),
                ));
            };
            let names = fields.into_iter().map(Option::unwrap_or_default).collect();
            (names, skip + end)
        }
        TableHeader::LastComment => {
            let comment = format.comment.map(|comment| comment as u8);
            let mut at = 0;
            let mut last = None;
            while at < body.len() && Some(body[at]) == comment {
                let end = line_end(at)?;
                last = Some((at + 1, end));
                at = end;
            }
            if at == body.len() && !complete {
                return Err(TableError::Header(
                    "the header is not in the first chunk".to_string(),
                ));
            }
            let (from, to) = last.ok_or_else(|| {
                TableError::Header("the table has no comment header line".to_string())
            })?;
            let line = std::str::from_utf8(&body[from..to])
                .map_err(|_| TableError::Header("the header is not UTF-8".to_string()))?;
            let mut plain = format.clone();
            plain.comment = None;
            let (rows, _) = parse_rows(&plain, None, line.trim().as_bytes(), true)?;
            let Some((Ok(fields), _)) = rows.into_iter().next() else {
                return Err(TableError::Header(
                    "the comment header is empty".to_string(),
                ));
            };
            (
                fields.into_iter().map(Option::unwrap_or_default).collect(),
                at,
            )
        }
    };
    let columns: Vec<String> = columns.iter().map(|name| name.trim().to_string()).collect();
    check_names(&columns)?;
    Ok((columns, start + used))
}

/// One parsed row: its fields, `None` for a null value, or why it is unusable.
pub type ParsedRow = Result<Vec<Option<String>>, String>;

/// Parses the rows of `bytes` with the end offset of each. Unless `complete`, the last row may
/// be cut, so it is left for the next chunk; the second value is the bytes the rows used.
pub fn parse_rows(
    format: &TableFormat,
    width: Option<usize>,
    bytes: &[u8],
    complete: bool,
) -> Result<(Vec<(ParsedRow, usize)>, usize), TableError> {
    let mut builder = csv::ReaderBuilder::new();
    builder
        .delimiter(format.delimiter as u8)
        .has_headers(false)
        .flexible(true)
        .comment(format.comment.map(|comment| comment as u8));
    match format.quote {
        Some(quote) => builder.quote(quote as u8),
        None => builder.quoting(false),
    };
    let mut reader = builder.from_reader(bytes);
    let mut record = csv::ByteRecord::new();
    let mut rows = Vec::new();
    loop {
        match reader.read_byte_record(&mut record) {
            Ok(true) => {}
            Ok(false) => break,
            Err(error) => return Err(TableError::Format(error.to_string())),
        }
        let end = reader.position().byte() as usize;
        rows.push((parse_fields(format, width, &record), end));
    }
    if complete {
        return Ok((rows, bytes.len()));
    }
    // Only a record reaching the chunk's end may be cut; one followed by more bytes is whole.
    if rows.last().is_some_and(|(_, end)| *end >= bytes.len()) {
        rows.pop();
    }
    let used = skip_lines(format, bytes, rows.last().map_or(0, |(_, end)| *end));
    if used == 0 && bytes.len() >= TABLE_CHUNK {
        return Err(TableError::RowTooLong);
    }
    Ok((rows, used))
}

/// The offset after the whole comment and blank lines that start at `at`.
fn skip_lines(format: &TableFormat, bytes: &[u8], mut at: usize) -> usize {
    loop {
        let rest = &bytes[at..];
        let blank = rest.starts_with(b"\n") || rest.starts_with(b"\r\n");
        let comment =
            (format.comment).is_some_and(|comment| rest.first() == Some(&(comment as u8)));
        let Some(newline) = rest.iter().position(|byte| *byte == b'\n') else {
            return at;
        };
        if !blank && !comment {
            return at;
        }
        at += newline + 1;
    }
}

fn parse_fields(format: &TableFormat, width: Option<usize>, record: &csv::ByteRecord) -> ParsedRow {
    if let Some(width) = width
        && record.len() != width
    {
        return Err(format!("the row has {} columns, not {width}", record.len()));
    }
    (record.iter())
        .map(|field| {
            let field = std::str::from_utf8(field).map_err(|_| "the row is not UTF-8")?;
            let null = format.nulls.iter().any(|null| null == field);
            Ok((!null).then(|| field.to_string()))
        })
        .collect()
}

/// What one row of a table becomes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RowOutcome {
    /// A filter left the row out.
    Filtered,
    /// A column the templates need holds no value.
    Skipped,
    Failed(String),
    Import(TableEntry),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableEntry {
    /// The connector path; a folder's ends with `/`.
    pub source_path: String,
    /// The object key of a file, or the key prefix of a folder without its last `/`.
    pub target: String,
    pub metadata: BTreeMap<String, String>,
    /// The file's size from the row; the file then needs no HEAD.
    pub size: Option<u64>,
    pub digests: Vec<(ChecksumAlgorithm, Vec<u8>)>,
    /// The file's modification time from the row, when it names one to the second.
    pub modified: Option<SystemTime>,
    pub etag: Option<String>,
    pub content_type: Option<String>,
    /// Further sources as the row renders them, each with its connector; a null value gives none.
    pub mirrors: Vec<(Ulid, String)>,
}

/// A template's parts, each column with its index in the table.
type Template = Vec<(Part, Option<usize>)>;

/// The row mapping resolved against one table's columns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowMapper {
    kind: RowKind,
    source: Template,
    target: Template,
    filters: Vec<(usize, BTreeSet<String>)>,
    /// Column index and metadata key of each kept column.
    kept: Vec<(usize, String)>,
    selections: Vec<(String, String)>,
    size: Option<usize>,
    checksums: Vec<(ChecksumAlgorithm, usize)>,
    modified: Option<usize>,
    /// Columns of a record row's ETag, content type and metadata object.
    record: [Option<usize>; 3],
    mirrors: Vec<(Ulid, Template)>,
}

impl RowMapper {
    pub fn new(rows: &TableRows, columns: &[String]) -> Result<Self, TableError> {
        let index = |name: &str| {
            (columns.iter().position(|column| column == name))
                .ok_or_else(|| TableError::Header(format!("the table has no column `{name}`")))
        };
        let resolve = |template: &str| -> Result<Vec<(Part, Option<usize>)>, TableError> {
            (template_parts(template)?.into_iter())
                .map(|part| match &part {
                    Part::Column { name, .. } => Ok((part.clone(), Some(index(name)?))),
                    Part::Text(_) => Ok((part, None)),
                })
                .collect()
        };
        let filters = (rows.filter.iter())
            .map(|filter| {
                Ok((
                    index(&filter.column)?,
                    filter.any_of.iter().cloned().collect(),
                ))
            })
            .collect::<Result<_, TableError>>()?;
        let mut names: BTreeSet<&str> = match &rows.keep {
            Some(keep) => keep.iter().map(String::as_str).collect(),
            None => columns.iter().map(String::as_str).collect(),
        };
        names.extend(rows.text.iter().chain(&rows.exact).map(String::as_str));
        let mut kept = Vec::with_capacity(names.len());
        let mut keys = BTreeSet::new();
        for name in names {
            let key = name.to_ascii_lowercase();
            if !header_safe(&key) || key.starts_with("aruna-") || !keys.insert(key.clone()) {
                let reason = format!("column `{name}` cannot be a metadata key");
                return Err(TableError::Mapping(reason));
            }
            kept.push((index(name)?, key));
        }
        let mut selections = Vec::new();
        for (key, columns) in [
            (SEARCH_TEXT_KEY, &rows.text),
            (SEARCH_EXACT_KEY, &rows.exact),
        ] {
            let lower: Vec<String> = columns
                .iter()
                .map(|name| name.to_ascii_lowercase())
                .collect();
            if !lower.is_empty() {
                selections.push((key.to_string(), lower.join(",")));
            }
        }
        if kept.len() + selections.len() > MAX_METADATA_ENTRIES {
            let reason = format!("at most {MAX_METADATA_ENTRIES} metadata entries fit an object");
            return Err(TableError::Mapping(reason));
        }
        let checksums = (rows.checksums.iter())
            .map(|checksum| {
                let algorithm = checksum_algorithm(&checksum.algorithm)?;
                Ok((algorithm, index(&checksum.column)?))
            })
            .collect::<Result<_, TableError>>()?;
        Ok(Self {
            kind: rows.kind,
            source: resolve(&rows.source)?,
            target: resolve(&rows.target)?,
            filters,
            kept,
            selections,
            size: rows.size.as_deref().map(index).transpose()?,
            checksums,
            modified: rows.modified.as_deref().map(index).transpose()?,
            record: [RECORD_ETAG, RECORD_CONTENT_TYPE, RECORD_METADATA].map(|name| {
                let found = columns.iter().position(|column| column == name);
                found.filter(|_| rows.kind == RowKind::Record)
            }),
            mirrors: (rows.mirrors.iter())
                .map(|mirror| Ok((mirror.connector_id, resolve(&mirror.source)?)))
                .collect::<Result<_, TableError>>()?,
        })
    }

    pub fn kind(&self) -> RowKind {
        self.kind
    }

    /// Maps one row; `base` is the read connector's base URL that absolute sources must start with.
    pub fn map(&self, fields: &[Option<String>], base: Option<&str>) -> RowOutcome {
        let value = |index: usize| fields.get(index).and_then(Option::as_deref);
        let included = self
            .filters
            .iter()
            .all(|(index, values)| value(*index).is_some_and(|value| values.contains(value)));
        if !included {
            return RowOutcome::Filtered;
        }
        let source = render(&self.source, &value);
        let (Some(rendered), true) = (
            render(&self.target, &value),
            source.is_some() || self.kind == RowKind::Record,
        ) else {
            return RowOutcome::Skipped;
        };
        let source = match source.map(|source| source_path(&source, base)).transpose() {
            Ok(source) => source,
            Err(reason) => return RowOutcome::Failed(reason),
        };
        let Some(mut target) = canonical_path(&rendered) else {
            return RowOutcome::Failed(format!("the target `{rendered}` is not a canonical key"));
        };
        let source_path = match (self.kind, source) {
            (RowKind::Folder, Some(source)) => format!("{source}/"),
            (_, Some(source)) => source,
            // A record without a source is an empty object; a folder marker keeps its `/`.
            (_, None) => {
                if rendered.ends_with('/') {
                    target.push('/');
                }
                String::new()
            }
        };
        let mut metadata = BTreeMap::new();
        for (index, key) in &self.kept {
            let Some(value) = value(*index) else {
                continue;
            };
            if value.len() > MAX_METADATA_VALUE {
                return RowOutcome::Failed(format!("the value of `{key}` is too long"));
            }
            metadata.insert(key.clone(), value.to_string());
        }
        metadata.extend(self.selections.iter().cloned());
        let (size, digests, modified) = match self.described(&value) {
            Ok(described) => described,
            Err(reason) => return RowOutcome::Failed(reason),
        };
        let [etag, content_type, object] = self.record.map(|index| index.and_then(value));
        if let Some(object) = object
            && let Err(reason) = record_metadata(object, &mut metadata)
        {
            return RowOutcome::Failed(reason);
        }
        if source_path.is_empty() && size != Some(0) {
            return RowOutcome::Failed("a record without a source must be empty".to_string());
        }
        RowOutcome::Import(TableEntry {
            source_path,
            target,
            metadata,
            size,
            digests,
            modified,
            etag: etag.map(str::to_string),
            content_type: content_type.map(str::to_string),
            mirrors: (self.mirrors.iter())
                .filter_map(|(connector_id, parts)| Some((*connector_id, render(parts, &value)?)))
                .collect(),
        })
    }

    /// The key a row maps to, if its target renders as a canonical key; names a failed row.
    pub fn target_key(&self, fields: &[Option<String>]) -> Option<String> {
        let value = |index: usize| fields.get(index).and_then(Option::as_deref);
        render(&self.target, &value).and_then(|target| canonical_path(&target))
    }

    /// The size, checksums and modification time a row gives; a null value gives none.
    fn described<'a>(
        &self,
        value: &impl Fn(usize) -> Option<&'a str>,
    ) -> Result<Described, String> {
        let size = (self.size.and_then(value))
            .map(|size| {
                size.parse::<u64>()
                    .map_err(|_| format!("size `{size}` is not a byte count"))
            })
            .transpose()?;
        let mut digests = Vec::new();
        for (algorithm, index) in &self.checksums {
            let Some(checksum) = value(*index) else {
                continue;
            };
            let digest = hex::decode(checksum).ok();
            let Some(digest) = digest.filter(|digest| digest.len() == algorithm.digest_len())
            else {
                let name = algorithm.hash_key();
                return Err(format!("checksum `{checksum}` is not a hex {name} digest"));
            };
            digests.push((*algorithm, digest));
        }
        let modified = match self.modified.and_then(value) {
            None => None,
            Some(time) => match chrono::DateTime::parse_from_rfc3339(time) {
                Ok(time) => Some(SystemTime::from(time)),
                Err(_) if chrono::NaiveDate::parse_from_str(time, "%Y-%m-%d").is_ok() => None,
                Err(_) => return Err(format!("modified `{time}` is no RFC 3339 time or date")),
            },
        };
        Ok((size, digests, modified))
    }
}

/// Adds a record row's metadata object; the search selection entries are the only `aruna-`
/// keys it may carry, as the import itself writes them.
fn record_metadata(object: &str, metadata: &mut BTreeMap<String, String>) -> Result<(), String> {
    let entries: BTreeMap<String, String> = serde_json::from_str(object)
        .map_err(|error| format!("the metadata is no JSON object of strings: {error}"))?;
    for (key, value) in entries {
        let selection = key == SEARCH_TEXT_KEY || key == SEARCH_EXACT_KEY;
        if !header_safe(&key) || (key.starts_with("aruna-") && !selection) {
            return Err(format!("`{key}` cannot be a metadata key"));
        }
        if value.len() > MAX_METADATA_VALUE {
            return Err(format!("the value of `{key}` is too long"));
        }
        metadata.entry(key).or_insert(value);
    }
    match metadata.len() <= MAX_METADATA_ENTRIES {
        true => Ok(()),
        false => Err(format!(
            "at most {MAX_METADATA_ENTRIES} metadata entries fit an object"
        )),
    }
}

fn render<'a>(
    parts: &[(Part, Option<usize>)],
    value: &impl Fn(usize) -> Option<&'a str>,
) -> Option<String> {
    let mut rendered = String::new();
    for (part, index) in parts {
        match (part, index) {
            (Part::Text(text), _) => rendered.push_str(text),
            (Part::Column { basename, .. }, Some(index)) => {
                let value = value(*index).filter(|value| !value.is_empty())?;
                match basename {
                    true => rendered.push_str(value.trim_end_matches('/').rsplit('/').next()?),
                    false => rendered.push_str(value),
                }
            }
            (Part::Column { .. }, None) => return None,
        }
    }
    Some(rendered)
}

/// A source value as a canonical path below the connector, whose base a URL must start with.
pub fn source_path(value: &str, base: Option<&str>) -> Result<String, String> {
    let path = connector_path(value, base)?;
    canonical_path(&path).ok_or_else(|| format!("the source `{value}` is not a canonical path"))
}

fn connector_path(value: &str, base: Option<&str>) -> Result<String, String> {
    if !value.contains("://") {
        return Ok(value.to_string());
    }
    let outside = || format!("the source `{value}` is outside the connector's base");
    let base = base.ok_or_else(outside)?;
    let base = base.trim_end_matches('/');
    match value.strip_prefix(base) {
        Some(rest) if rest.is_empty() || rest.starts_with('/') => decoded(rest)
            .ok_or_else(|| format!("the source `{value}` holds an invalid escaped segment")),
        _ => Err(outside()),
    }
}

/// A URL path with each segment decoded once; an escaped `/` would change its structure.
fn decoded(path: &str) -> Option<String> {
    let segments = path.split('/').map(|segment| {
        let segment = percent_encoding::percent_decode_str(segment)
            .decode_utf8()
            .ok()?;
        (!segment.contains('/')).then(|| segment.into_owned())
    });
    Some(segments.collect::<Option<Vec<_>>>()?.join("/"))
}

/// The path without leading and trailing `/`; `None` for empty, `.` or `..` segments.
fn canonical_path(path: &str) -> Option<String> {
    let path = path.trim_matches('/');
    let invalid = path.is_empty()
        || path.chars().any(|c| c.is_control() || c == '\\')
        || (path.split('/')).any(|segment| segment.is_empty() || segment == "." || segment == "..");
    (!invalid).then(|| path.to_string())
}

/// Compressed bytes a cursor received and has not decoded yet. While the transport has not
/// ended, empty input answers `WouldBlock`, so the decoder keeps its state for the next push.
struct Pending {
    bytes: Vec<u8>,
    read: usize,
    ended: bool,
}

impl Read for Pending {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let available = self.fill_buf()?;
        let count = available.len().min(out.len());
        out[..count].copy_from_slice(&available[..count]);
        self.consume(count);
        Ok(count)
    }
}

impl BufRead for Pending {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        match (self.read < self.bytes.len(), self.ended) {
            (true, _) => Ok(&self.bytes[self.read..]),
            (false, true) => Ok(&[]),
            (false, false) => Err(std::io::ErrorKind::WouldBlock.into()),
        }
    }

    fn consume(&mut self, amount: usize) {
        self.read += amount;
    }
}

/// Decompresses a gzip table in order and keeps its decoder between the chunks one read takes,
/// so every compressed byte is decoded once. It decodes only as far as the next chunk needs and
/// discards bytes before the read position as they are produced, so memory stays bounded.
pub struct GzipCursor {
    decoder: flate2::bufread::MultiGzDecoder<Pending>,
    /// Decoded bytes from the read position on: the chunk and one byte that shows more follows.
    kept: Vec<u8>,
    limit: usize,
    /// Decoded bytes so far, kept or discarded.
    decoded: u64,
    /// The decompressed position the next chunk starts at.
    at: u64,
    ended: bool,
}

impl GzipCursor {
    /// A cursor that decodes from the table's start and keeps chunks of `limit` bytes from `at`.
    pub fn new(at: u64, limit: usize) -> Self {
        let pending = Pending {
            bytes: Vec::new(),
            read: 0,
            ended: false,
        };
        Self {
            decoder: flate2::bufread::MultiGzDecoder::new(pending),
            kept: Vec::new(),
            limit,
            decoded: 0,
            at,
            ended: false,
        }
    }

    /// Moves the read position forward to `at`; `false` for a position before the current one.
    pub fn advance(&mut self, at: u64) -> bool {
        if at < self.at {
            return false;
        }
        let start = self.decoded - self.kept.len() as u64;
        let dropped = (at.saturating_sub(start)).min(self.kept.len() as u64);
        self.kept.drain(..dropped as usize);
        self.at = at;
        true
    }

    /// Adds compressed bytes; [`Self::fill`] decodes them.
    pub fn push(&mut self, bytes: &[u8]) {
        let pending = self.decoder.get_mut();
        pending.bytes.drain(..pending.read);
        pending.read = 0;
        pending.bytes.extend_from_slice(bytes);
    }

    /// Marks the compressed input as complete.
    pub fn finish(&mut self) {
        self.decoder.get_mut().ended = true;
    }

    /// Decodes until a chunk and one byte beyond it are kept, the table ends, or the input
    /// runs out; `true` once the next chunk can be taken. A cut or corrupt stream is an error.
    pub fn fill(&mut self) -> Result<bool, TableError> {
        let mut scratch = [0u8; 16 * 1024];
        while !self.ended && self.kept.len() <= self.limit {
            let skipped = self.at.saturating_sub(self.decoded);
            let wanted = match skipped {
                0 => self.limit + 1 - self.kept.len(),
                skipped => usize::try_from(skipped).unwrap_or(usize::MAX),
            };
            let wanted = wanted.min(scratch.len());
            match self.decoder.read(&mut scratch[..wanted]) {
                Ok(0) => self.ended = true,
                Ok(count) => {
                    self.decoded += count as u64;
                    if skipped == 0 {
                        self.kept.extend_from_slice(&scratch[..count]);
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(false),
                Err(error) => return Err(TableError::Gzip(error.to_string())),
            }
        }
        Ok(true)
    }

    /// Up to `limit` bytes from the read position and whether they reach the table's end.
    pub fn chunk(&self) -> (Vec<u8>, bool) {
        let taken = self.kept.len().min(self.limit);
        (
            self.kept[..taken].to_vec(),
            self.ended && taken == self.kept.len(),
        )
    }
}

#[cfg(test)]
#[path = "table_tests.rs"]
mod tests;
