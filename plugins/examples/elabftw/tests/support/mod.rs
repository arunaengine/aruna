//! A fake eLabFTW API v2 and a fake Aruna group behind the sync's host trait, shared by the
//! native sync tests and the guest test.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#![allow(dead_code)]

mod elab;

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, MutexGuard};

use aruna_plugins::limits::Limits;
use elabftw_sync::host::{
    Created, Document, Fault, FilePage, Host, Kind, Meter, Method, Reply, Request, Result,
};
use elabftw_sync::sync::{Budget, Room, Sync};
use elabftw_sync::values::Settings;
use serde_json::{Value, json};

pub const SYNC_USER: u64 = 7;
pub const PERSON: u64 = 2;
pub const BUCKET: &str = "/r/g/lab/data/node/shared";
/// File entities per page, small so paging is exercised.
const FILE_PAGE: usize = 2;
const MAX_STATE: usize = 256 * 1024;

pub fn settings() -> Settings {
    serde_json::from_value(json!({
        "elab_url": "https://elab.example.org",
        "sync_user": SYNC_USER,
        "writers": "3, group:4",
        "bucket": BUCKET,
        "prefix": "elabftw",
        "aruna_url": "https://portal.example.org",
    }))
    .unwrap()
}

/// One stored dataset: its root entity and its file entities by `@id`.
#[derive(Clone, Debug, Default)]
pub struct Dataset {
    pub path: String,
    pub entity: Value,
    pub files: BTreeMap<String, Value>,
    pub revision: String,
}

/// Everything both fakes hold, and switches that make them fail like the real services.
#[derive(Default)]
pub struct Inner {
    pub experiments: BTreeMap<u64, Value>,
    pub uploads: BTreeMap<u64, Vec<Value>>,
    pub tag_ids: BTreeMap<String, u64>,
    pub clock: u64,
    pub datasets: BTreeMap<String, Dataset>,
    pub creates: BTreeMap<String, String>,
    pub revisions: u64,
    pub state: BTreeMap<String, (u64, Vec<u8>)>,
    pub objects: BTreeMap<String, u64>,
    pub log: Vec<String>,
    pub posts: u64,
    /// eLabFTW answers changing requests but keeps nothing, like a lost write.
    pub drop_writes: bool,
    /// A user holds exclusive edit mode: every `PATCH` is refused.
    pub edit_mode: bool,
    /// The next experiment `POST`: its outcome kind and whether it took effect anyway.
    pub post_outcome: Option<(Kind, bool)>,
    /// The next dataset create: an unknown outcome that still created it.
    pub create_unknown: bool,
    /// Experiments whose answer is over the per-call bound.
    pub huge: BTreeSet<u64>,
    /// Aruna edits a dataset right after the next patch is read, so the patch is refused.
    pub race_patch: Option<Value>,
    /// The same, for the next file entity change.
    pub race_file: Option<Value>,
    /// The node stops right after the next experiment `POST` lands: state writes fail until
    /// the test clears `state_down`.
    /// File entities per page; the default when 0.
    pub file_page: usize,
    pub stop_after_post: bool,
    /// The same, right after the next change of an experiment lands.
    pub stop_after_patch: bool,
    /// Bytes the current run moved, against the host's bound per run.
    pub run_bytes: u64,
    pub state_down: bool,
}

#[derive(Default)]
pub struct World {
    inner: Mutex<Inner>,
}

fn fault(kind: Kind, message: &str) -> Fault {
    Fault::new(kind, message)
}

/// Applies a JSON merge patch.
fn merge(target: &mut Value, patch: &Value) {
    let (Value::Object(target), Value::Object(patch)) = (target, patch) else {
        return;
    };
    for (key, value) in patch {
        match value {
            Value::Null => {
                target.remove(key);
            }
            Value::Object(_) if target.get(key).is_some_and(Value::is_object) => {
                merge(target.get_mut(key).unwrap(), value);
            }
            other => {
                target.insert(key.clone(), other.clone());
            }
        }
    }
}

impl Inner {
    fn revision(&mut self) -> String {
        self.revisions += 1;
        format!("r{:08}", self.revisions)
    }
}

impl World {
    pub fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap()
    }

    pub fn dataset(&self, id: &str) -> Dataset {
        self.lock().datasets[id].clone()
    }

    /// The only dataset, for tests that make one.
    pub fn only_dataset(&self) -> (String, Dataset) {
        let inner = self.lock();
        assert_eq!(inner.datasets.len(), 1, "one dataset");
        let (id, dataset) = inner.datasets.iter().next().unwrap();
        (id.clone(), dataset.clone())
    }

    /// A person's change to a dataset entity, as a merge patch.
    pub fn aruna_edit(&self, id: &str, patch: Value) -> String {
        let mut inner = self.lock();
        let revision = inner.revision();
        let dataset = inner.datasets.get_mut(id).unwrap();
        merge(&mut dataset.entity, &patch);
        dataset.revision = revision.clone();
        revision
    }

    /// A dataset a person made in Aruna at `path`.
    pub fn aruna_dataset(&self, path: &str, entity: Value) -> String {
        let mut inner = self.lock();
        let revision = inner.revision();
        let id = format!("01JD{:022}", inner.datasets.len() + 1);
        let dataset = Dataset {
            path: path.to_string(),
            entity,
            files: BTreeMap::new(),
            revision,
        };
        inner.datasets.insert(id.clone(), dataset);
        id
    }

    pub fn aruna_file(&self, id: &str, file: Value) {
        let mut inner = self.lock();
        let revision = inner.revision();
        let dataset = inner.datasets.get_mut(id).unwrap();
        dataset
            .files
            .insert(file["@id"].as_str().unwrap().to_string(), file);
        dataset.revision = revision;
    }

    pub fn put_state(&self, key: &str, value: &Value) {
        let mut inner = self.lock();
        let version = inner.state.get(key).map_or(1, |(version, _)| version + 1);
        inner
            .state
            .insert(key.to_string(), (version, value.to_string().into_bytes()));
    }

    pub fn state_keys(&self) -> Vec<String> {
        self.lock().state.keys().cloned().collect()
    }

    pub fn state(&self, key: &str) -> Option<Value> {
        let inner = self.lock();
        inner
            .state
            .get(key)
            .map(|(_, bytes)| serde_json::from_slice(bytes).unwrap())
    }
}

/// The world behind the host's bounds of `plugins/src/limits.rs`: a request or a reply of one
/// call up to `call_bytes`, and a run up to `bytes` in all; anything more is refused.
pub struct Bounded<'a>(pub &'a World);

impl Bounded<'_> {
    fn bound<T>(
        &self,
        sent: usize,
        call: impl FnOnce() -> Result<T>,
        size: impl Fn(&T) -> usize,
    ) -> Result<T> {
        let limits = Limits::default();
        if sent > limits.call_bytes {
            return Err(fault(
                Kind::Limit,
                "the request is larger than the call allows",
            ));
        }
        self.charge(sent)?;
        let answer = call()?;
        let received = size(&answer);
        self.charge(received)?;
        match received > limits.call_bytes {
            true => Err(fault(
                Kind::Limit,
                "the answer is larger than the call allows",
            )),
            false => Ok(answer),
        }
    }

    fn charge(&self, bytes: usize) -> Result<()> {
        let mut inner = self.0.lock();
        inner.run_bytes += bytes as u64;
        match inner.run_bytes > Limits::default().bytes {
            true => Err(fault(Kind::Limit, "the run moved more bytes than it may")),
            false => Ok(()),
        }
    }
}

fn json_len(value: &Value) -> usize {
    serde_json::to_vec(value).map_or(0, |bytes| bytes.len())
}

impl Host for Bounded<'_> {
    fn elab(&self, request: &Request) -> Result<Reply> {
        let query: usize = (request.query.iter())
            .map(|(name, value)| name.len() + value.len())
            .sum();
        let sent = request.path.len() + query + request.body.as_ref().map_or(0, json_len);
        self.bound(sent, || self.0.elab(request), |reply| reply.body.len())
    }

    fn summary(&self, dataset: &str) -> Result<Document> {
        self.bound(
            dataset.len(),
            || self.0.summary(dataset),
            |doc| json_len(&doc.json),
        )
    }

    fn files(&self, dataset: &str, cursor: Option<&str>) -> Result<FilePage> {
        let answer = || self.0.files(dataset, cursor);
        self.bound(dataset.len(), answer, |page| {
            page.files.iter().map(json_len).sum()
        })
    }

    fn patch(&self, dataset: &str, patch: &Value, expected: &str) -> Result<String> {
        self.bound(
            json_len(patch),
            || self.0.patch(dataset, patch, expected),
            String::len,
        )
    }

    fn put_file(&self, dataset: &str, file: &Value, expected: &str) -> Result<String> {
        self.bound(
            json_len(file),
            || self.0.put_file(dataset, file, expected),
            String::len,
        )
    }

    fn remove_file(&self, dataset: &str, id: &str, expected: &str) -> Result<String> {
        self.bound(
            id.len(),
            || self.0.remove_file(dataset, id, expected),
            String::len,
        )
    }

    fn create(&self, path: &str, dataset: &Value, key: &str) -> Result<Created> {
        self.bound(
            json_len(dataset),
            || self.0.create(path, dataset, key),
            |_| 64,
        )
    }

    fn object_size(&self, path: &str) -> Result<u64> {
        self.bound(path.len(), || self.0.object_size(path), |_| 8)
    }

    fn objects(&self, prefix: &str) -> Result<()> {
        self.bound(prefix.len(), || self.0.objects(prefix), |_| 0)
    }

    fn get(&self, key: &str) -> Result<Option<(u64, Vec<u8>)>> {
        let size =
            |entry: &Option<(u64, Vec<u8>)>| entry.as_ref().map_or(0, |(_, value)| value.len());
        self.bound(key.len(), || Host::get(self.0, key), size)
    }

    fn put(&self, key: &str, value: &[u8], expected: Option<u64>) -> Result<u64> {
        self.bound(value.len(), || self.0.put(key, value, expected), |_| 8)
    }

    fn log(&self, line: &str) {
        self.0.log(line);
    }
}

impl Host for World {
    fn elab(&self, request: &Request) -> Result<Reply> {
        // Like the host, a key names a request only within its run: a later run sends again.
        let mut inner = self.lock();
        let reply = inner.elab(request)?;
        let landed =
            request.method == Method::Post && request.path.is_empty() && reply.status == 201;
        if landed && std::mem::take(&mut inner.stop_after_post) {
            inner.state_down = true;
        }
        if request.method == Method::Patch
            && reply.status == 200
            && std::mem::take(&mut inner.stop_after_patch)
        {
            inner.state_down = true;
        }
        Ok(reply)
    }

    fn summary(&self, dataset: &str) -> Result<Document> {
        let inner = self.lock();
        let found = inner
            .datasets
            .get(dataset)
            .ok_or_else(|| fault(Kind::NotFound, "no such dataset"))?;
        Ok(Document {
            revision: found.revision.clone(),
            json: found.entity.clone(),
        })
    }

    fn files(&self, dataset: &str, cursor: Option<&str>) -> Result<FilePage> {
        let inner = self.lock();
        let found = inner
            .datasets
            .get(dataset)
            .ok_or_else(|| fault(Kind::NotFound, "no such dataset"))?;
        let after =
            (found.files.iter()).filter(|(id, _)| cursor.is_none_or(|cursor| id.as_str() > cursor));
        let size = if inner.file_page == 0 {
            FILE_PAGE
        } else {
            inner.file_page
        };
        let page: Vec<(&String, &Value)> = after.take(size + 1).collect();
        let more = page.len() > size;
        let files: Vec<Value> = page
            .iter()
            .take(size)
            .map(|(_, file)| (*file).clone())
            .collect();
        let cursor = more.then(|| page[size - 1].0.clone());
        Ok(FilePage { files, cursor })
    }

    fn patch(&self, dataset: &str, patch: &Value, expected: &str) -> Result<String> {
        let mut inner = self.lock();
        if let Some(race) = inner.race_patch.take() {
            let revision = inner.revision();
            let found = inner.datasets.get_mut(dataset).unwrap();
            merge(&mut found.entity, &race);
            found.revision = revision;
        }
        let revision = inner.revision();
        let found = inner
            .datasets
            .get_mut(dataset)
            .ok_or_else(|| fault(Kind::NotFound, "no such dataset"))?;
        if found.revision != expected {
            return Err(fault(Kind::Conflict, "the dataset is at another revision"));
        }
        merge(&mut found.entity, patch);
        found.revision = revision.clone();
        Ok(revision)
    }

    fn put_file(&self, dataset: &str, file: &Value, expected: &str) -> Result<String> {
        let mut inner = self.lock();
        if let Some(race) = inner.race_file.take() {
            let revision = inner.revision();
            let found = inner.datasets.get_mut(dataset).unwrap();
            merge(&mut found.entity, &race);
            found.revision = revision;
        }
        let url = file["contentUrl"].as_str().unwrap_or_default();
        if !inner.objects.contains_key(url) {
            return Err(fault(Kind::Invalid, "the file names no readable object"));
        }
        let revision = inner.revision();
        let found = inner
            .datasets
            .get_mut(dataset)
            .ok_or_else(|| fault(Kind::NotFound, "no such dataset"))?;
        if found.revision != expected {
            return Err(fault(Kind::Conflict, "the dataset is at another revision"));
        }
        found
            .files
            .insert(file["@id"].as_str().unwrap().to_string(), file.clone());
        found.revision = revision.clone();
        Ok(revision)
    }

    fn remove_file(&self, dataset: &str, id: &str, expected: &str) -> Result<String> {
        let mut inner = self.lock();
        let revision = inner.revision();
        let found = inner
            .datasets
            .get_mut(dataset)
            .ok_or_else(|| fault(Kind::NotFound, "no such dataset"))?;
        if found.revision != expected {
            return Err(fault(Kind::Conflict, "the dataset is at another revision"));
        }
        found.files.remove(id);
        found.revision = revision.clone();
        Ok(revision)
    }

    fn create(&self, path: &str, dataset: &Value, key: &str) -> Result<Created> {
        let mut inner = self.lock();
        if let Some(id) = inner.creates.get(key).cloned() {
            let found = &inner.datasets[&id];
            return Ok(Created {
                resource: id,
                revision: found.revision.clone(),
            });
        }
        if dataset["name"].as_str().unwrap_or_default().is_empty() {
            return Err(fault(Kind::Invalid, "a dataset needs a name"));
        }
        let revision = inner.revision();
        let id = format!("01JC{:022}", inner.datasets.len() + 1);
        let mut entity = dataset.clone();
        entity["@id"] = json!("./");
        let created = Dataset {
            path: path.to_string(),
            entity,
            files: BTreeMap::new(),
            revision: revision.clone(),
        };
        inner.datasets.insert(id.clone(), created);
        inner.creates.insert(key.to_string(), id.clone());
        if std::mem::take(&mut inner.create_unknown) {
            return Err(fault(Kind::Unknown, "the create may have happened"));
        }
        Ok(Created {
            resource: id,
            revision,
        })
    }

    fn object_size(&self, path: &str) -> Result<u64> {
        let inner = self.lock();
        inner
            .objects
            .get(path)
            .copied()
            .ok_or_else(|| fault(Kind::NotFound, "no such object"))
    }

    fn objects(&self, prefix: &str) -> Result<()> {
        match prefix.starts_with(&format!("{BUCKET}/")) {
            true => Ok(()),
            false => Err(fault(Kind::NotFound, "no such bucket on this node")),
        }
    }

    fn get(&self, key: &str) -> Result<Option<(u64, Vec<u8>)>> {
        Ok(self.lock().state.get(key).cloned())
    }

    fn put(&self, key: &str, value: &[u8], expected: Option<u64>) -> Result<u64> {
        let mut inner = self.lock();
        if inner.state_down {
            return Err(fault(Kind::Unavailable, "the node stopped"));
        }
        if value.len() > MAX_STATE {
            return Err(fault(Kind::Limit, "the state value is too large"));
        }
        let current = inner.state.get(key).map(|(version, _)| *version);
        if current != expected {
            return Err(fault(Kind::Conflict, "the state value moved on"));
        }
        let version = current.unwrap_or_default() + 1;
        inner
            .state
            .insert(key.to_string(), (version, value.to_vec()));
        Ok(version)
    }

    fn log(&self, line: &str) {
        self.lock().log.push(line.to_string());
    }
}

pub const START: u64 = 1_800_000_000_000;

/// Runs handlers at a moving time, each as its own run.
pub struct Runner {
    pub world: World,
    pub now: Cell<u64>,
    runs: Cell<u64>,
    /// The budget of each run, and what the last run used of it.
    pub budget: Cell<Budget>,
    pub used: Cell<Room>,
}

impl Runner {
    pub fn new() -> Self {
        Self {
            world: World::default(),
            now: Cell::new(START),
            runs: Cell::new(0),
            budget: Cell::new(Budget::default()),
            used: Cell::new(Room { calls: 0, bytes: 0 }),
        }
    }

    pub fn at<T>(&self, step: impl FnOnce(&Sync) -> T) -> T {
        let settings = settings();
        self.runs.set(self.runs.get() + 1);
        let run = format!("run-{}", self.runs.get());
        self.world.lock().run_bytes = 0;
        let bounded = Bounded(&self.world);
        let meter = Meter::new(&bounded);
        let sync = Sync::new(&meter, &settings, &run, self.now.get());
        let sync = sync.with_budget(self.budget.get());
        let answer = step(&sync);
        self.used.set(sync.used());
        answer
    }

    pub fn poll(&self) -> Value {
        self.now.set(self.now.get() + 5 * 60 * 1000);
        self.at(|sync| sync.poll()).unwrap()
    }

    pub fn event(&self, dataset: &str) {
        let found = self.world.dataset(dataset);
        let path = format!("/r/g/lab/meta/{}@{dataset}", found.path);
        self.at(|sync| sync.changed(dataset, &path, &found.revision))
            .unwrap();
    }

    pub fn manual(&self, input: Value) -> Value {
        self.at(|sync| sync.manual(&input)).unwrap()
    }

    pub fn link(&self, experiment: u64) -> Value {
        self.world
            .state(&format!("link/{experiment}"))
            .unwrap_or_default()
    }

    pub fn marks(&self, experiment: u64) -> Value {
        self.link(experiment)["marks"].clone()
    }
}
