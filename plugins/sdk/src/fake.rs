//! A fake host for the library's tests: datasets, objects and their contents in memory, every
//! call noted.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use serde_json::{Map, Value};

use crate::Level;
use crate::bindings::aruna::plugin::jobs::Status;
use crate::bindings::aruna::plugin::metadata_create::Created;
use crate::bindings::aruna::plugin::metadata_read::{Document, FilePage, Listing};
use crate::bindings::aruna::plugin::objects_read::{ObjectInfo, ObjectPage};
use crate::bindings::aruna::plugin::services::{Request, Response};
use crate::bindings::aruna::plugin::state::Entry;
use crate::bindings::aruna::plugin::types::{ErrorKind, Field, HostError};
use crate::host::{Answer, Host, replace};

/// What the fake holds and what it was asked, shared with the test.
#[derive(Default)]
pub(crate) struct State {
    pub calls: Vec<String>,
    /// Content and version by bucket path; a version starting with `m` is marked.
    pub objects: BTreeMap<String, (Vec<u8>, String)>,
    pub datasets: BTreeMap<String, Stored>,
    /// Answers by request path: status and body.
    pub replies: BTreeMap<String, (u16, Vec<u8>)>,
    pub secrets: BTreeMap<String, Vec<(String, String)>>,
    /// Values by key with their version; versions never repeat.
    pub state: BTreeMap<String, (u64, Vec<u8>)>,
    pub versions: u64,
    /// Jobs by id with their outputs.
    pub jobs: BTreeMap<String, (Status, Vec<ObjectInfo>)>,
    pub page: usize,
}

/// One dataset: its revision, entity and file entities.
#[derive(Default)]
pub(crate) struct Stored {
    pub revision: u64,
    pub entity: Map<String, Value>,
    pub files: Vec<Value>,
}

#[derive(Clone)]
pub(crate) struct Fake(pub Rc<RefCell<State>>);

/// A fake with `objects` in place of the imports.
pub(crate) fn install(objects: &[(&str, &[u8], &str)]) -> Fake {
    let mut state = State {
        page: 2,
        ..State::default()
    };
    for (path, content, version) in objects {
        let entry = (content.to_vec(), version.to_string());
        state.objects.insert(path.to_string(), entry);
    }
    let fake = Fake(Rc::new(RefCell::new(state)));
    replace(Box::new(fake.clone()));
    fake
}

impl Fake {
    pub fn calls(&self) -> Vec<String> {
        self.0.borrow().calls.clone()
    }

    /// Stores dataset `id` at revision 1.
    pub fn dataset(&self, id: &str, entity: Value) {
        let entity = entity.as_object().cloned().unwrap_or_default();
        let stored = Stored {
            revision: 1,
            entity,
            files: Vec::new(),
        };
        self.0.borrow_mut().datasets.insert(id.into(), stored);
    }

    /// Runs `change` on dataset `id` at revision `expected`, as a write does.
    fn write(&self, id: &str, expected: &str, change: impl FnOnce(&mut Stored)) -> Answer<String> {
        let mut state = self.0.borrow_mut();
        let stored =
            (state.datasets.get_mut(id)).ok_or_else(|| refusal(ErrorKind::NotFound, id))?;
        if stored.revision.to_string() != expected {
            return Err(refusal(ErrorKind::Conflict, "changed meanwhile"));
        }
        change(stored);
        stored.revision += 1;
        Ok(stored.revision.to_string())
    }

    /// Items after `cursor`, a page of them, and the cursor of the next page.
    fn paged<T: Clone>(&self, items: &[T], cursor: Option<&str>) -> (Vec<T>, Option<String>) {
        let start: usize = cursor.map_or(0, |cursor| cursor.parse().unwrap());
        let page = self.0.borrow().page;
        let taken = items.iter().skip(start).take(page).cloned().collect();
        let next = start + page;
        (taken, (next < items.len()).then(|| next.to_string()))
    }

    fn note(&self, call: String) {
        self.0.borrow_mut().calls.push(call);
    }

    fn info(&self, path: &str) -> Option<ObjectInfo> {
        let state = self.0.borrow();
        let (content, version) = state.objects.get(path)?;
        Some(ObjectInfo {
            path: path.into(),
            size: content.len() as u64,
            version: version.clone(),
            marked: version.starts_with('m'),
        })
    }
}

/// Applies a JSON merge patch by the host's rules; `merge_cases.json` holds both to the same cases.
pub(crate) fn merge_into(target: &mut Value, patch: Value) {
    let Value::Object(changes) = patch else {
        *target = patch;
        return;
    };
    if !target.is_object() {
        *target = Value::Object(Map::new());
    }
    let Value::Object(fields) = target else {
        return;
    };
    for (key, value) in changes {
        match value {
            Value::Null => drop(fields.remove(&key)),
            value => merge_into(fields.entry(key).or_insert(Value::Null), value),
        }
    }
}

pub(crate) fn refusal(kind: ErrorKind, message: &str) -> HostError {
    HostError {
        kind,
        message: message.into(),
    }
}

impl Host for Fake {
    fn log(&self, level: Level, message: &str) {
        self.note(format!("log {level:?} {message}"));
    }

    fn object_info(&self, path: &str) -> Answer<ObjectInfo> {
        self.note(format!("info {path}"));
        self.info(path)
            .ok_or_else(|| refusal(ErrorKind::NotFound, path))
    }

    fn object_list(&self, prefix: &str, cursor: Option<&str>) -> Answer<ObjectPage> {
        self.note(format!("list {prefix} {}", cursor.unwrap_or("-")));
        let (page, paths) = {
            let state = self.0.borrow();
            let after = |path: &&String| cursor.is_none_or(|cursor| path.as_str() > cursor);
            let below = state.objects.keys().filter(|path| path.starts_with(prefix));
            let paths: Vec<String> = below.filter(after).cloned().collect();
            (state.page, paths)
        };
        let objects: Vec<ObjectInfo> = paths
            .iter()
            .take(page)
            .filter_map(|path| self.info(path))
            .collect();
        let cursor = (paths.len() > page).then(|| objects.last().map(|object| object.path.clone()));
        Ok(ObjectPage {
            objects,
            cursor: cursor.flatten(),
        })
    }

    fn dataset_get(&self, id: &str, whole: bool) -> Answer<Document> {
        self.note(format!("get {id} {whole}"));
        let state = self.0.borrow();
        let stored = (state.datasets.get(id)).ok_or_else(|| refusal(ErrorKind::NotFound, id))?;
        let mut entity = stored.entity.clone();
        if whole {
            let parts: Vec<Value> = stored
                .files
                .iter()
                .map(|file| serde_json::json!({ "@id": file["@id"] }))
                .collect();
            entity.insert("hasPart".into(), parts.into());
        }
        Ok(Document {
            resource: id.into(),
            revision: stored.revision.to_string(),
            json: Value::Object(entity).to_string(),
        })
    }

    fn dataset_list(&self, cursor: Option<&str>) -> Answer<Listing> {
        self.note(format!("datasets {}", cursor.unwrap_or("-")));
        let ids: Vec<String> = self.0.borrow().datasets.keys().cloned().collect();
        let (resources, cursor) = self.paged(&ids, cursor);
        Ok(Listing { resources, cursor })
    }

    fn dataset_files(&self, id: &str, cursor: Option<&str>) -> Answer<FilePage> {
        self.note(format!("files {id} {}", cursor.unwrap_or("-")));
        let files: Vec<String> = (self.0.borrow().datasets.get(id).iter())
            .flat_map(|stored| stored.files.iter().map(Value::to_string))
            .collect();
        let (files, cursor) = self.paged(&files, cursor);
        Ok(FilePage { files, cursor })
    }

    fn dataset_patch(&self, id: &str, patch: &str, expected: &str) -> Answer<String> {
        self.note(format!("patch {id} {patch} {expected}"));
        let patch: Value = serde_json::from_str(patch).unwrap();
        self.write(id, expected, |stored| {
            let mut entity = Value::Object(std::mem::take(&mut stored.entity));
            merge_into(&mut entity, patch);
            stored.entity = entity.as_object().cloned().unwrap_or_default();
        })
    }

    fn file_put(&self, id: &str, file: &str, expected: &str) -> Answer<String> {
        self.note(format!("put {id} {file} {expected}"));
        let file: Value = serde_json::from_str(file).unwrap();
        self.write(id, expected, |stored| {
            stored.files.retain(|kept| kept["@id"] != file["@id"]);
            stored.files.push(file);
        })
    }

    fn file_remove(&self, id: &str, file: &str, expected: &str) -> Answer<String> {
        self.note(format!("remove {id} {file} {expected}"));
        self.write(id, expected, |stored| {
            stored.files.retain(|kept| kept["@id"] != file)
        })
    }

    fn dataset_create(&self, path: &str, dataset: &str) -> Answer<Created> {
        self.note(format!("create {path}"));
        let id = format!("D-{path}");
        if !self.0.borrow().datasets.contains_key(&id) {
            self.dataset(&id, serde_json::from_str(dataset).unwrap());
        }
        Ok(Created {
            resource: id,
            revision: "1".into(),
        })
    }

    fn send(&self, request: &Request) -> Answer<Response> {
        let body = String::from_utf8_lossy(&request.body);
        self.note(format!(
            "send {} {:?} {} {body}",
            request.service, request.method, request.path
        ));
        let state = self.0.borrow();
        let (status, body) = state
            .replies
            .get(&request.path)
            .cloned()
            .unwrap_or((404, Vec::new()));
        Ok(Response {
            status,
            headers: vec![Field {
                name: "Content-Type".into(),
                value: "application/json".into(),
            }],
            body,
        })
    }

    fn secret(&self, name: &str) -> Answer<Vec<Field>> {
        let state = self.0.borrow();
        let fields =
            (state.secrets.get(name)).ok_or_else(|| refusal(ErrorKind::NotGranted, name))?;
        let fields = fields.iter().map(|(name, value)| Field {
            name: name.clone(),
            value: value.clone(),
        });
        Ok(fields.collect())
    }

    fn state_get(&self, key: &str) -> Answer<Option<Entry>> {
        self.note(format!("state get {key}"));
        let state = self.0.borrow();
        let entry = state.state.get(key).map(|(version, value)| Entry {
            version: *version,
            value: value.clone(),
        });
        Ok(entry)
    }

    fn state_put(&self, key: &str, value: &[u8], expected: Option<u64>) -> Answer<u64> {
        self.note(format!(
            "state put {key} {}",
            String::from_utf8_lossy(value)
        ));
        let mut state = self.0.borrow_mut();
        if state.state.get(key).map(|(version, _)| *version) != expected {
            return Err(refusal(ErrorKind::Conflict, key));
        }
        state.versions += 1;
        let version = state.versions;
        state.state.insert(key.into(), (version, value.to_vec()));
        Ok(version)
    }

    fn state_remove(&self, key: &str, expected: u64) -> Answer<()> {
        self.note(format!("state remove {key} {expected}"));
        let mut state = self.0.borrow_mut();
        if state.state.get(key).map(|(version, _)| *version) != Some(expected) {
            return Err(refusal(ErrorKind::Conflict, key));
        }
        state.state.remove(key);
        Ok(())
    }

    fn job_copy(&self, source: &str, version: Option<&str>, target: &str) -> Answer<String> {
        self.note(format!("copy {source} {} {target}", version.unwrap_or("-")));
        Ok(format!("job-{}", self.0.borrow().jobs.len() + 1))
    }

    fn job_get(&self, id: &str) -> Answer<Status> {
        self.note(format!("job {id}"));
        let state = self.0.borrow();
        let (status, _) = state
            .jobs
            .get(id)
            .ok_or_else(|| refusal(ErrorKind::NotFound, id))?;
        Ok(status.clone())
    }

    fn job_outputs(&self, id: &str, cursor: Option<&str>) -> Answer<ObjectPage> {
        self.note(format!("outputs {id} {}", cursor.unwrap_or("-")));
        let outputs = (self
            .0
            .borrow()
            .jobs
            .get(id)
            .map(|(_, outputs)| outputs.clone()))
        .unwrap_or_default();
        let (objects, cursor) = self.paged(&outputs, cursor);
        Ok(ObjectPage { objects, cursor })
    }

    fn job_cancel(&self, id: &str) -> Answer<()> {
        self.note(format!("cancel {id}"));
        Ok(())
    }

    fn object_read(&self, path: &str, version: Option<&str>) -> Answer<Vec<u8>> {
        self.note(format!("read {path} {}", version.unwrap_or("-")));
        let info = self
            .info(path)
            .ok_or_else(|| refusal(ErrorKind::NotFound, path))?;
        if info.marked {
            return Err(refusal(ErrorKind::Denied, "written by a job with a secret"));
        }
        Ok(self.0.borrow().objects[path].0.clone())
    }
}
