//! Sync progress in plugin state: link records with their reservations and marks, the link and
//! mark indexes, and the paged mark report.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::compare::{Origin, Side, Snapshot};
use crate::host::{Fault, Host, Kind, Result};
use crate::values::Field;

/// Every state value stays below the host's bound of 256 KiB.
pub const MAX_VALUE: usize = 256 * 1024;
pub const LINK_PAGE: usize = 2000;
pub const MARK_PAGE: usize = 100;
/// A report page stays below this many bytes of run output.
pub const REPORT_BYTES: usize = 48 * 1024;
/// A reservation older than this belongs to a run that has ended; a run lasts at most 60 s.
pub const STALE_MS: u64 = 10 * 60 * 1000;
pub const DAY_MS: u64 = 24 * 60 * 60 * 1000;
/// Compare-and-set attempts on a shared index value before the change is left to a later run.
const ATTEMPTS: usize = 8;

/// Why a link needs attention; the report lists links by these.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mark {
    Conflict,
    OutOfSync,
    ChangedInStorage,
    TooLarge,
    Gone,
    Blocked,
    NoWrite,
    UnknownCreate,
    Failed,
}

/// What a reservation is for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Reserved {
    /// The keyed dataset create of an experiment.
    Dataset,
    /// The `POST` that creates an experiment for a dataset made in Aruna.
    Experiment,
    Change,
}

/// What a run is about to change, written before the change; never a request or a body.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reservation {
    pub kind: Reserved,
    pub run: String,
    pub at: u64,
    /// The Aruna revision a change to eLabFTW was sent from, and the prints of what was sent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<(String, Side)>,
    /// The `POST` outcome is unknown, so no new create follows until it is settled.
    #[serde(default)]
    pub unknown: bool,
}

impl Reservation {
    pub fn new(kind: Reserved, run: &str, at: u64) -> Self {
        Self {
            kind,
            run: run.to_string(),
            at,
            source: None,
            unknown: false,
        }
    }

    /// Whether another run holds it and may still be running.
    pub fn busy(&self, run: &str, now: u64) -> bool {
        self.run != run && now < self.at.saturating_add(STALE_MS)
    }
}

/// One link, at `link/<experiment>`. A dataset made in Aruna waits at `dataset/<id>` until its
/// experiment exists; then that key only names the experiment.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Link {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub experiment: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dataset: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<Snapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reservation: Option<Reservation>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub marks: BTreeMap<Mark, Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mark_page: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_at: Option<u64>,
    /// eLabFTW's `modified_at` right after the plugin's own last write.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub written: Option<String>,
    /// A settled conflict: these fields take the named side's values on the next sync.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force: Option<(Origin, Vec<Field>)>,
    /// The full pass that last checked this link.
    #[serde(default)]
    pub pass: u64,
    /// Experiment creates settled as absent; the next `POST` uses a new key.
    #[serde(default)]
    pub attempt: u32,
}

impl Link {
    /// Sets or clears one mark; details are cut so a report page stays small.
    pub fn mark(&mut self, mark: Mark, details: Option<Vec<String>>) {
        match details {
            Some(details) => {
                let cut = |detail: String| detail.chars().take(120).collect();
                self.marks
                    .insert(mark, details.into_iter().take(8).map(cut).collect());
            }
            None => {
                self.marks.remove(&mark);
            }
        }
    }

    /// A failed step: the reason, and when the next run should try again.
    pub fn fail(&mut self, reason: &str, now: u64, delay: u64) {
        self.mark(Mark::Failed, Some(vec![reason.to_string()]));
        self.retry_at = Some(now.saturating_add(delay));
    }
}

pub fn link_key(experiment: u64) -> String {
    format!("link/{experiment}")
}

pub fn dataset_key(dataset: &str) -> String {
    format!("dataset/{dataset}")
}

/// Reads a typed state value and its version.
pub fn load<T: DeserializeOwned>(host: &dyn Host, key: &str) -> Result<Option<(u64, T)>> {
    let Some((version, bytes)) = host.get(key)? else {
        return Ok(None);
    };
    let value = serde_json::from_slice(&bytes)
        .map_err(|error| Fault::new(Kind::Invalid, format!("state {key}: {error}")))?;
    Ok(Some((version, value)))
}

/// Writes a typed state value if its version is still `expected`; refuses a value that would
/// reach the host's bound before sending it.
pub fn store<T: Serialize>(
    host: &dyn Host,
    key: &str,
    value: &T,
    expected: Option<u64>,
) -> Result<u64> {
    let bytes =
        serde_json::to_vec(value).map_err(|error| Fault::new(Kind::Invalid, error.to_string()))?;
    if bytes.len() >= MAX_VALUE {
        let message = format!("state {key} would hold {} bytes", bytes.len());
        return Err(Fault::new(Kind::Limit, message));
    }
    host.put(key, &bytes, expected)
}

/// A loaded link record: what was read, its version, and the record being changed.
#[derive(Clone, Debug, Default)]
pub struct Record {
    pub key: String,
    pub version: Option<u64>,
    before: Link,
    pub link: Link,
}

impl Record {
    pub fn load(host: &dyn Host, key: &str) -> Result<Self> {
        let (version, link) = match load::<Link>(host, key)? {
            Some((version, link)) => (Some(version), link),
            None => (None, Link::default()),
        };
        Ok(Self {
            key: key.to_string(),
            version,
            before: link.clone(),
            link,
        })
    }

    /// Writes the record by compare-and-set, then the link and mark indexes it changed. A tag
    /// set that makes the record too large is dropped to its print first.
    pub fn save(&mut self, host: &dyn Host) -> Result<()> {
        if self.link.marks != self.before.marks
            && self.link.mark_page.is_none()
            && !self.link.marks.is_empty()
        {
            self.link.mark_page = Some(free_page(host)?);
        }
        let written = match store(host, &self.key, &self.link, self.version) {
            Err(fault) if fault.kind == Kind::Limit => {
                if let Some(snapshot) = &mut self.link.snapshot {
                    snapshot.elab.tag_set = None;
                    snapshot.aruna.tag_set = None;
                }
                store(host, &self.key, &self.link, self.version)?
            }
            other => other?,
        };
        let added = self.version.is_none() && self.key.starts_with("link/");
        self.version = Some(written);
        if added && let Some(experiment) = self.link.experiment {
            index_link(host, experiment)?;
        }
        if self.link.marks != self.before.marks {
            index_marks(host, &self.key, &self.before, &self.link)?;
        }
        self.before = self.link.clone();
        Ok(())
    }
}

/// Retries a read-change-write of one shared value until its compare-and-set holds.
fn update<T, F>(host: &dyn Host, key: &str, mut change: F) -> Result<T>
where
    T: Serialize + DeserializeOwned + Default,
    F: FnMut(&mut T),
{
    for _ in 0..ATTEMPTS {
        let (version, mut value) = match load::<T>(host, key)? {
            Some((version, value)) => (Some(version), value),
            None => (None, T::default()),
        };
        change(&mut value);
        match store(host, key, &value, version) {
            Ok(_) => return Ok(value),
            Err(fault) if fault.kind == Kind::Conflict => continue,
            Err(fault) => return Err(fault),
        }
    }
    Err(Fault::new(
        Kind::Unavailable,
        format!("state {key} kept changing"),
    ))
}

/// The pages of linked experiment ids, so a full pass can walk every link.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LinkIndex {
    pub pages: u64,
}

fn index_link(host: &dyn Host, experiment: u64) -> Result<()> {
    let index: LinkIndex = load(host, "links")?
        .map(|(_, index)| index)
        .unwrap_or_default();
    let last = index.pages.saturating_sub(1);
    let page = update::<Vec<u64>, _>(host, &format!("links/{last}"), |ids| {
        if ids.len() < LINK_PAGE && !ids.contains(&experiment) {
            ids.push(experiment);
        }
    })?;
    if page.contains(&experiment) {
        if index.pages == 0 {
            update::<LinkIndex, _>(host, "links", |index| index.pages = index.pages.max(1))?;
        }
        return Ok(());
    }
    let next = update::<LinkIndex, _>(host, "links", |index| {
        index.pages = index.pages.max(last + 2)
    })?;
    update::<Vec<u64>, _>(host, &format!("links/{}", next.pages - 1), |ids| {
        if !ids.contains(&experiment) {
            ids.push(experiment);
        }
    })?;
    Ok(())
}

/// One page of the link index: the experiment ids and whether more pages follow.
pub fn link_page(host: &dyn Host, page: u64) -> Result<(Vec<u64>, bool)> {
    let index: LinkIndex = load(host, "links")?
        .map(|(_, index)| index)
        .unwrap_or_default();
    let ids = load::<Vec<u64>>(host, &format!("links/{page}"))?.map(|(_, ids)| ids);
    Ok((ids.unwrap_or_default(), page + 1 < index.pages))
}

/// The mark index's totals per kind and its page count.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MarkIndex {
    pub pages: u64,
    pub totals: BTreeMap<Mark, u64>,
}

/// One marked link in the index.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub experiment: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dataset: Option<String>,
    pub marks: BTreeMap<Mark, Vec<String>>,
}

/// A mark page with room: the last one, or a new one after it.
fn free_page(host: &dyn Host) -> Result<u64> {
    let index: MarkIndex = load(host, "marks")?
        .map(|(_, index)| index)
        .unwrap_or_default();
    let last = index.pages.saturating_sub(1);
    let page = load::<BTreeMap<String, Entry>>(host, &format!("marks/{last}"))?;
    let full = page.is_some_and(|(_, entries)| entries.len() >= MARK_PAGE);
    let wanted = if full { last + 1 } else { last };
    if wanted >= index.pages {
        update::<MarkIndex, _>(host, "marks", |index| {
            index.pages = index.pages.max(wanted + 1)
        })?;
    }
    Ok(wanted)
}

fn index_marks(host: &dyn Host, key: &str, before: &Link, link: &Link) -> Result<()> {
    let Some(page) = link.mark_page else {
        return Ok(());
    };
    update::<BTreeMap<String, Entry>, _>(host, &format!("marks/{page}"), |entries| {
        if link.marks.is_empty() {
            entries.remove(key);
            return;
        }
        // Short details keep a page of 100 entries well below the state bound; the link
        // record keeps the full ones.
        let short = |details: &Vec<String>| {
            let cut = |detail: &String| detail.chars().take(40).collect::<String>();
            details.iter().take(4).map(cut).collect::<Vec<_>>()
        };
        let marks = link
            .marks
            .iter()
            .map(|(mark, details)| (*mark, short(details)));
        let entry = Entry {
            experiment: link.experiment,
            dataset: link.dataset.clone(),
            marks: marks.collect(),
        };
        entries.insert(key.to_string(), entry);
    })?;
    let kinds = |link: &Link| link.marks.keys().copied().collect::<BTreeSet<_>>();
    let (old, new) = (kinds(before), kinds(link));
    update::<MarkIndex, _>(host, "marks", |index| {
        for mark in new.difference(&old) {
            *index.totals.entry(*mark).or_default() += 1;
        }
        for mark in old.difference(&new) {
            let total = index.totals.entry(*mark).or_default();
            *total = total.saturating_sub(1);
        }
    })?;
    Ok(())
}

/// The keys of failed links on one mark page, and whether more pages follow.
pub fn failed_page(host: &dyn Host, page: u64) -> Result<(Vec<String>, bool)> {
    let index: MarkIndex = load(host, "marks")?
        .map(|(_, index)| index)
        .unwrap_or_default();
    let entries = load::<BTreeMap<String, Entry>>(host, &format!("marks/{page}"))?;
    let keys = (entries
        .map(|(_, entries)| entries)
        .unwrap_or_default()
        .into_iter())
    .filter(|(_, entry)| {
        entry.marks.contains_key(&Mark::Failed) || entry.marks.contains_key(&Mark::UnknownCreate)
    })
    .map(|(key, _)| key)
    .collect();
    Ok((keys, page + 1 < index.pages))
}

/// One report page from mark page `after` on: at most [`MARK_PAGE`] links below
/// [`REPORT_BYTES`], the totals per kind, and `next` while more pages follow.
pub fn report(host: &dyn Host, after: u64) -> Result<Value> {
    let index: MarkIndex = load(host, "marks")?
        .map(|(_, index)| index)
        .unwrap_or_default();
    let mut page = after;
    let mut entries = Vec::new();
    while entries.is_empty() && page < index.pages {
        let stored = load::<BTreeMap<String, Entry>>(host, &format!("marks/{page}"))?;
        entries = stored
            .map(|(_, entries)| entries.into_values().collect())
            .unwrap_or_default();
        page += 1;
    }
    let next = (page < index.pages).then_some(page);
    let mut answer = json!({ "marks": entries, "totals": index.totals, "next": next });
    // Details shrink until the page fits; a page holds at most 100 entries of short details.
    while serde_json::to_vec(&answer).map_or(0, |bytes| bytes.len()) >= REPORT_BYTES {
        let mut shrunk = false;
        for entry in answer["marks"].as_array_mut().into_iter().flatten() {
            let marks = entry["marks"]
                .as_object_mut()
                .into_iter()
                .flat_map(|marks| marks.values_mut());
            for details in marks {
                shrunk |= details.as_array_mut().and_then(Vec::pop).is_some();
            }
        }
        if !shrunk {
            break;
        }
    }
    Ok(answer)
}
