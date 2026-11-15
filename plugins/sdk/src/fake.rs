//! A fake host for the library's tests: objects and their contents in memory, every call noted.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use crate::Level;
use crate::bindings::aruna::plugin::objects_read::{ObjectInfo, ObjectPage};
use crate::bindings::aruna::plugin::types::{ErrorKind, HostError};
use crate::host::{Answer, Host, replace};

/// What the fake holds and what it was asked, shared with the test.
#[derive(Default)]
pub(crate) struct State {
    pub calls: Vec<String>,
    /// Content and version by bucket path; a version starting with `m` is marked.
    pub objects: BTreeMap<String, (Vec<u8>, String)>,
    pub page: usize,
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
