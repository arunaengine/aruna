//! The host calls the library makes, behind one trait, so its logic is tested against a fake.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::cell::RefCell;

use crate::Level;
use crate::bindings::aruna::plugin::objects_read::{ObjectInfo, ObjectPage};
use crate::bindings::aruna::plugin::types::HostError;
use crate::bindings::aruna::plugin::{log, objects_read};

/// One host call's answer.
pub(crate) type Answer<T> = Result<T, HostError>;

/// Every host call of the library; the component's imports, or a fake in tests.
pub(crate) trait Host {
    fn log(&self, level: Level, message: &str);
    fn object_info(&self, path: &str) -> Answer<ObjectInfo>;
    fn object_list(&self, prefix: &str, cursor: Option<&str>) -> Answer<ObjectPage>;
    fn object_read(&self, path: &str, version: Option<&str>) -> Answer<Vec<u8>>;
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
