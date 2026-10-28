//! Describes a table import: a CSV or TSV list whose rows name files or folders to import, with
//! the columns kept as object metadata and the columns that search may match.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;

use serde::{Deserialize, Serialize};
use thiserror::Error;

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
    if !complete {
        rows.pop();
        if rows.is_empty() && bytes.len() >= TABLE_CHUNK {
            return Err(TableError::RowTooLong);
        }
    }
    let used = match complete {
        true => bytes.len(),
        false => rows.last().map_or(0, |(_, end)| *end),
    };
    Ok((rows, used))
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
}

/// The row mapping resolved against one table's columns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowMapper {
    kind: RowKind,
    source: Vec<(Part, Option<usize>)>,
    target: Vec<(Part, Option<usize>)>,
    filters: Vec<(usize, BTreeSet<String>)>,
    /// Column index and metadata key of each kept column.
    kept: Vec<(usize, String)>,
    selections: Vec<(String, String)>,
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
        Ok(Self {
            kind: rows.kind,
            source: resolve(&rows.source)?,
            target: resolve(&rows.target)?,
            filters,
            kept,
            selections,
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
        let (Some(source), Some(target)) =
            (render(&self.source, &value), render(&self.target, &value))
        else {
            return RowOutcome::Skipped;
        };
        let source = match connector_path(&source, base) {
            Ok(path) => path,
            Err(reason) => return RowOutcome::Failed(reason),
        };
        let Some(source) = canonical_path(&source) else {
            return RowOutcome::Failed(format!("the source `{source}` is not a canonical path"));
        };
        let Some(target) = canonical_path(&target) else {
            return RowOutcome::Failed(format!("the target `{target}` is not a canonical key"));
        };
        let source_path = match self.kind {
            RowKind::File => source,
            RowKind::Folder => format!("{source}/"),
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
        RowOutcome::Import(TableEntry {
            source_path,
            target,
            metadata,
        })
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

/// A source value as a path below the connector: a URL must start with the connector's base.
fn connector_path(value: &str, base: Option<&str>) -> Result<String, String> {
    if !value.contains("://") {
        return Ok(value.to_string());
    }
    let outside = || format!("the source `{value}` is outside the connector's base");
    let base = base.ok_or_else(outside)?;
    let base = base.trim_end_matches('/');
    match value.strip_prefix(base) {
        Some(rest) if rest.is_empty() || rest.starts_with('/') => Ok(rest.to_string()),
        _ => Err(outside()),
    }
}

/// The path without leading and trailing `/`; `None` for empty, `.` or `..` segments.
fn canonical_path(path: &str) -> Option<String> {
    let path = path.trim_matches('/');
    let invalid = path.is_empty()
        || path.chars().any(|c| c.is_control() || c == '\\')
        || (path.split('/')).any(|segment| segment.is_empty() || segment == "." || segment == "..");
    (!invalid).then(|| path.to_string())
}

/// Decompresses a gzip table from its start and keeps the bytes after `skip`, at most `limit`.
pub struct GzipWindow {
    decoder: flate2::write::MultiGzDecoder<Window>,
}

struct Window {
    skip: u64,
    limit: usize,
    kept: Vec<u8>,
    full: bool,
}

impl Write for Window {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let skipped = (self.skip.min(bytes.len() as u64)) as usize;
        self.skip -= skipped as u64;
        let rest = &bytes[skipped..];
        let room = self.limit - self.kept.len();
        if rest.len() > room {
            self.kept.extend_from_slice(&rest[..room]);
            self.full = true;
            return Err(std::io::Error::other("window full"));
        }
        self.kept.extend_from_slice(rest);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl GzipWindow {
    pub fn new(skip: u64, limit: usize) -> Self {
        let window = Window {
            skip,
            limit,
            kept: Vec::new(),
            full: false,
        };
        Self {
            decoder: flate2::write::MultiGzDecoder::new(window),
        }
    }

    /// Feeds compressed bytes; `true` once the window is full and reading may stop.
    pub fn push(&mut self, bytes: &[u8]) -> Result<bool, TableError> {
        match self.decoder.write_all(bytes) {
            Ok(()) => Ok(false),
            Err(_) if self.decoder.get_ref().full => Ok(true),
            Err(error) => Err(TableError::Gzip(error.to_string())),
        }
    }

    /// The kept bytes and whether they reach the end of the table.
    pub fn finish(mut self) -> Result<(Vec<u8>, bool), TableError> {
        if !self.decoder.get_ref().full {
            match self.decoder.try_finish() {
                Ok(()) => {}
                Err(_) if self.decoder.get_ref().full => {}
                Err(error) => return Err(TableError::Gzip(error.to_string())),
            }
        }
        let window = self.decoder.get_ref();
        let complete = !window.full;
        Ok((window.kept.clone(), complete))
    }
}

#[cfg(test)]
#[path = "table_tests.rs"]
mod tests;
