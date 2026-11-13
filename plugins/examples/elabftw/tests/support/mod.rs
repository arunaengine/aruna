//! A fake eLabFTW API v2 and a fake Aruna group behind the sync's host trait, shared by the
//! native sync tests and the guest test.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#![allow(dead_code)]

mod elab;

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, MutexGuard};

use elabftw_sync::host::{Created, Document, Fault, FilePage, Host, Kind, Reply, Request, Result};
use elabftw_sync::sync::Sync;
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
    pub spent: u64,
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
    /// The first outcome of each keyed request.
    pub replies: BTreeMap<String, Result<Reply>>,
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

impl Host for World {
    fn elab(&self, request: &Request) -> Result<Reply> {
        let mut inner = self.lock();
        // Like the host, a keyed request answers its first outcome again and is not sent twice.
        if let Some(stored) = request.key.as_ref().and_then(|key| inner.replies.get(key)) {
            return stored.clone();
        }
        let outcome = inner.elab(request);
        let kept = !matches!(&outcome, Err(fault) if fault.kind == Kind::Unavailable);
        if let (Some(key), true) = (&request.key, kept) {
            inner.replies.insert(key.clone(), outcome.clone());
        }
        let reply = outcome?;
        inner.spent += reply.body.len() as u64;
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
        let page: Vec<(&String, &Value)> = after.take(FILE_PAGE + 1).collect();
        let more = page.len() > FILE_PAGE;
        let files: Vec<Value> = page
            .iter()
            .take(FILE_PAGE)
            .map(|(_, file)| (*file).clone())
            .collect();
        let cursor = more.then(|| page[FILE_PAGE - 1].0.clone());
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

    fn spent(&self) -> u64 {
        self.lock().spent
    }
}

pub const START: u64 = 1_800_000_000_000;

/// Runs handlers at a moving time, each as its own run.
pub struct Runner {
    pub world: World,
    pub now: Cell<u64>,
    runs: Cell<u64>,
}

impl Runner {
    pub fn new() -> Self {
        Self {
            world: World::default(),
            now: Cell::new(START),
            runs: Cell::new(0),
        }
    }

    pub fn at<T>(&self, step: impl FnOnce(&Sync) -> T) -> T {
        let settings = settings();
        self.runs.set(self.runs.get() + 1);
        let run = format!("run-{}", self.runs.get());
        let sync = Sync::new(&self.world, &settings, &run, self.now.get());
        step(&sync)
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
