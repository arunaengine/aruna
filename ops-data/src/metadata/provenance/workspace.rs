//! The working state of one provenance export as rows of the spool keyspace, and the overlay a
//! step runs against: rows it read, rows it wrote, and rows it needed but had not read yet.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};
use ulid::Ulid;

use super::{Hop, Target};

/// The kinds of rows an export keeps, each under its own key prefix.
pub(super) const LABEL: u8 = 0;
pub(super) const ENTITY: u8 = 1;
pub(super) const PART: u8 = 2;
pub(super) const QUEUE: u8 = 3;

/// Properties a link may name; a stored link is read back to one of these.
const PROPERTIES: [&str; 6] = [
    "mainEntity",
    "isBasedOn",
    "result",
    "object",
    "hasPart",
    "subjectOf",
];

/// The key prefix of every row of `export`, or of one kind of its rows.
pub(super) fn prefix(export: Ulid, kind: Option<u8>) -> Vec<u8> {
    let mut key = export.to_bytes().to_vec();
    key.extend(kind);
    key
}

pub(super) fn row_key(export: Ulid, kind: u8, name: &[u8]) -> Vec<u8> {
    let mut key = prefix(export, Some(kind));
    key.extend_from_slice(name);
    key
}

/// The name a row of `kind` was stored under.
pub(super) fn row_name(key: &[u8]) -> &[u8] {
    key.get(17..).unwrap_or_default()
}

pub(super) fn encode_hop(hop: &Hop) -> Vec<u8> {
    let (kind, id) = match &hop.target {
        Target::Version(id) => ("version", id),
        Target::Run(id) => ("run", id),
        Target::Release(id) => ("release", id),
    };
    let record = json!({
        "kind": kind, "id": id, "from": hop.from, "property": hop.property, "digest": hop.digest,
    });
    serde_json::to_vec(&record).unwrap_or_default()
}

pub(super) fn decode_hop(bytes: &[u8]) -> Hop {
    let record: Value = serde_json::from_slice(bytes).unwrap_or_default();
    let id = record["id"].as_str().unwrap_or_default().to_string();
    let target = match record["kind"].as_str() {
        Some("run") => Target::Run(id),
        Some("release") => Target::Release(id),
        _ => Target::Version(id),
    };
    let named = record["property"].as_str().unwrap_or_default();
    let property = PROPERTIES.into_iter().find(|known| *known == named);
    Hop {
        target,
        from: record["from"].as_str().unwrap_or_default().to_string(),
        property: property.unwrap_or("isBasedOn"),
        digest: record["digest"].as_str().map(str::to_string),
    }
}

/// The rows one step of an export sees. A step that reads a row it has not loaded notes the
/// row as missing; its result is then thrown away and the step runs again once it is loaded.
/// A complete workspace holds every row, so a row it lacks does not exist.
#[derive(Debug, Default, PartialEq)]
pub struct Workspace {
    export: Ulid,
    rows: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    complete: bool,
    misses: BTreeSet<Vec<u8>>,
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    /// The store holds no queued link past those loaded.
    queue_done: bool,
    /// The step wanted a queued link that is not loaded yet.
    queue_miss: bool,
}

/// The rows a step stores and the keys it deletes.
pub(super) type StepWrites = (Vec<(Vec<u8>, Vec<u8>)>, Vec<Vec<u8>>);

impl Workspace {
    /// The overlay of a step that loads the rows of `export` as it needs them.
    pub(super) fn partial(export: Ulid) -> Self {
        Self {
            export,
            ..Self::default()
        }
    }

    /// A workspace that holds every row of its export from the start.
    pub fn complete(export: Ulid) -> Self {
        Self {
            export,
            complete: true,
            ..Self::default()
        }
    }

    pub(super) fn export(&self) -> Ulid {
        self.export
    }

    /// Adds rows read from the store: `None` where the row does not exist.
    pub(super) fn load(&mut self, rows: impl IntoIterator<Item = (Vec<u8>, Option<Vec<u8>>)>) {
        for (key, value) in rows {
            self.misses.remove(&key);
            self.rows.entry(key).or_insert(value);
        }
    }

    pub(super) fn get(&mut self, kind: u8, name: &[u8]) -> Option<Vec<u8>> {
        let key = row_key(self.export, kind, name);
        match self.writes.get(&key).or_else(|| self.rows.get(&key)) {
            Some(value) => value.clone(),
            None => {
                if !self.complete {
                    self.misses.insert(key);
                }
                None
            }
        }
    }

    pub(super) fn put(&mut self, kind: u8, name: &[u8], value: Vec<u8>) {
        let key = row_key(self.export, kind, name);
        self.writes.insert(key, Some(value));
    }

    pub(super) fn delete(&mut self, key: Vec<u8>) {
        self.writes.insert(key, None);
    }

    /// The rows of `kind` as this step sees them, in key order.
    pub fn rows_of(&self, kind: u8) -> Vec<(Vec<u8>, Vec<u8>)> {
        let start = prefix(self.export, Some(kind));
        let mut seen: BTreeMap<&Vec<u8>, &Option<Vec<u8>>> = BTreeMap::new();
        for rows in [&self.rows, &self.writes] {
            let found = rows
                .range(start.clone()..)
                .take_while(|(key, _)| key.starts_with(&start));
            seen.extend(found);
        }
        (seen.into_iter())
            .filter_map(|(key, value)| Some((key.clone(), value.clone()?)))
            .collect()
    }

    /// The oldest queued link this step sees. Links queued in this step sort after every stored
    /// one, so a stored link that is not loaded yet comes first and is noted as missing.
    pub(super) fn next_queued(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        let first = self.rows_of(QUEUE).into_iter().next();
        let stored = first
            .as_ref()
            .is_some_and(|(key, _)| self.rows.contains_key(key));
        if stored || self.complete || self.queue_done {
            return first;
        }
        self.queue_miss = true;
        None
    }

    /// The last stored queue key this step loaded, which the next page starts after.
    pub(super) fn last_queued(&self) -> Option<Vec<u8>> {
        let start = prefix(self.export, Some(QUEUE));
        (self.rows.range(start.clone()..))
            .take_while(|(key, _)| key.starts_with(&start))
            .last()
            .map(|(key, _)| key.clone())
    }

    pub(super) fn queue_missed(&self) -> bool {
        self.queue_miss
    }

    /// The store holds no further queued link.
    pub(super) fn queue_ended(&mut self) {
        self.queue_done = true;
    }

    /// Rows this step needed but had not loaded.
    pub(super) fn misses(&self) -> &BTreeSet<Vec<u8>> {
        &self.misses
    }

    /// Forgets what a step wrote and the misses it noted, before it runs again.
    pub(super) fn reset(&mut self) {
        self.writes.clear();
        self.misses.clear();
        self.queue_miss = false;
    }

    /// The writes of the step, rows to store and rows to delete; they count as read after.
    pub(super) fn take_writes(&mut self) -> StepWrites {
        let (mut stored, mut deleted) = (Vec::new(), Vec::new());
        for (key, value) in std::mem::take(&mut self.writes) {
            self.rows.insert(key.clone(), value.clone());
            match value {
                Some(value) => stored.push((key, value)),
                None => deleted.push(key),
            }
        }
        (stored, deleted)
    }
}
