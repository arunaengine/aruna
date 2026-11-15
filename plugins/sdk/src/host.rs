//! The host calls the library makes, behind one trait, so its logic is tested against a fake.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::cell::RefCell;

use crate::Level;
use crate::bindings::aruna::plugin::jobs::Status;
use crate::bindings::aruna::plugin::metadata_create::Created;
use crate::bindings::aruna::plugin::metadata_read::{Document, FilePage, Listing};
use crate::bindings::aruna::plugin::objects_read::{ObjectInfo, ObjectPage};
use crate::bindings::aruna::plugin::services::{Request, Response};
use crate::bindings::aruna::plugin::state::Entry;
use crate::bindings::aruna::plugin::types::{Field, HostError};
use crate::bindings::aruna::plugin::{
    jobs, log, metadata_create, metadata_read, metadata_write, objects_read, secrets, services,
    state,
};

/// One host call's answer.
pub(crate) type Answer<T> = Result<T, HostError>;

/// Every host call of the library; the component's imports, or a fake in tests.
pub(crate) trait Host {
    fn log(&self, level: Level, message: &str);
    fn object_info(&self, path: &str) -> Answer<ObjectInfo>;
    fn object_list(&self, prefix: &str, cursor: Option<&str>) -> Answer<ObjectPage>;
    fn object_read(&self, path: &str, version: Option<&str>) -> Answer<Vec<u8>>;
    /// The dataset entity without its file links, or with them when `whole`.
    fn dataset_get(&self, id: &str, whole: bool) -> Answer<Document>;
    fn dataset_list(&self, cursor: Option<&str>) -> Answer<Listing>;
    fn dataset_files(&self, id: &str, cursor: Option<&str>) -> Answer<FilePage>;
    fn dataset_patch(&self, id: &str, patch: &str, expected: &str) -> Answer<String>;
    fn file_put(&self, id: &str, file: &str, expected: &str) -> Answer<String>;
    fn file_remove(&self, id: &str, file: &str, expected: &str) -> Answer<String>;
    fn dataset_create(&self, path: &str, dataset: &str) -> Answer<Created>;
    fn send(&self, request: &Request) -> Answer<Response>;
    fn secret(&self, name: &str) -> Answer<Vec<Field>>;
    fn state_get(&self, key: &str) -> Answer<Option<Entry>>;
    fn state_put(&self, key: &str, value: &[u8], expected: Option<u64>) -> Answer<u64>;
    fn state_remove(&self, key: &str, expected: u64) -> Answer<()>;
    fn job_copy(&self, source: &str, version: Option<&str>, target: &str) -> Answer<String>;
    fn job_get(&self, id: &str) -> Answer<Status>;
    fn job_outputs(&self, id: &str, cursor: Option<&str>) -> Answer<ObjectPage>;
    fn job_cancel(&self, id: &str) -> Answer<()>;
}

/// The component's own imports.
struct Imports;

impl Host for Imports {
    fn log(&self, level: Level, message: &str) {
        log::log(level, message);
    }

    fn object_info(&self, path: &str) -> Answer<ObjectInfo> {
        objects_read::info(path)
    }

    fn object_list(&self, prefix: &str, cursor: Option<&str>) -> Answer<ObjectPage> {
        objects_read::list(prefix, cursor)
    }

    fn object_read(&self, path: &str, version: Option<&str>) -> Answer<Vec<u8>> {
        objects_read::read(path, version)
    }

    fn dataset_get(&self, id: &str, whole: bool) -> Answer<Document> {
        match whole {
            true => metadata_read::get(id),
            false => metadata_read::summary(id),
        }
    }

    fn dataset_list(&self, cursor: Option<&str>) -> Answer<Listing> {
        metadata_read::list(cursor)
    }

    fn dataset_files(&self, id: &str, cursor: Option<&str>) -> Answer<FilePage> {
        metadata_read::files(id, cursor)
    }

    fn dataset_patch(&self, id: &str, patch: &str, expected: &str) -> Answer<String> {
        metadata_write::patch(id, patch, expected)
    }

    fn file_put(&self, id: &str, file: &str, expected: &str) -> Answer<String> {
        metadata_write::put_file(id, file, expected)
    }

    fn file_remove(&self, id: &str, file: &str, expected: &str) -> Answer<String> {
        metadata_write::remove_file(id, file, expected)
    }

    fn dataset_create(&self, path: &str, dataset: &str) -> Answer<Created> {
        metadata_create::create(path, dataset)
    }

    fn send(&self, request: &Request) -> Answer<Response> {
        services::send(request)
    }

    fn secret(&self, name: &str) -> Answer<Vec<Field>> {
        secrets::value(name)
    }

    fn state_get(&self, key: &str) -> Answer<Option<Entry>> {
        state::get(key)
    }

    fn state_put(&self, key: &str, value: &[u8], expected: Option<u64>) -> Answer<u64> {
        state::put(key, value, expected)
    }

    fn state_remove(&self, key: &str, expected: u64) -> Answer<()> {
        state::remove(key, expected)
    }

    fn job_copy(&self, source: &str, version: Option<&str>, target: &str) -> Answer<String> {
        jobs::copy(source, version, target)
    }

    fn job_get(&self, id: &str) -> Answer<Status> {
        jobs::get(id)
    }

    fn job_outputs(&self, id: &str, cursor: Option<&str>) -> Answer<ObjectPage> {
        jobs::outputs(id, cursor)
    }

    fn job_cancel(&self, id: &str) -> Answer<()> {
        jobs::cancel(id)
    }
}

thread_local! {
    static HOST: RefCell<Box<dyn Host>> = RefCell::new(Box::new(Imports));
}

/// Runs one host call.
pub(crate) fn with<R>(call: impl FnOnce(&dyn Host) -> R) -> R {
    HOST.with(|host| call(host.borrow().as_ref()))
}

/// Puts a fake in place of the imports for this thread.
#[cfg(test)]
pub(crate) fn replace(fake: Box<dyn Host>) {
    HOST.with(|host| *host.borrow_mut() = fake);
}

#[cfg(test)]
#[path = "fake.rs"]
pub(crate) mod fake;
