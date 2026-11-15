//! The host calls the library makes, behind one trait, so its logic is tested against a fake.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::cell::RefCell;

use crate::Level;
use crate::bindings::aruna::plugin::log;

/// Every host call of the library; the component's imports, or a fake in tests.
pub(crate) trait Host {
    fn log(&self, level: Level, message: &str);
}

/// The component's own imports.
struct Imports;

impl Host for Imports {
    fn log(&self, level: Level, message: &str) {
        log::log(level, message);
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
