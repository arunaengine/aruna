//! Objects of the install's group on its node: information, listings and small contents, named
//! by bucket paths such as `raw-data/run-7/reads.fastq.gz`.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::de::DeserializeOwned;

use crate::bindings::aruna::plugin::objects_read::ObjectInfo;
use crate::datasets::Pages;
use crate::host;
use crate::jobs::Copy;
use crate::{Error, Result};

/// One object version. A `marked` version was written by a job with a secret: a plugin cannot
/// read, copy or upload it, though it may still link it into a dataset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Object {
    /// The bucket path, `bucket/key`.
    pub path: String,
    /// The canonical address, `s3://bucket/key`.
    pub url: String,
    pub size: u64,
    pub version: String,
    pub marked: bool,
}

impl Object {
    pub(crate) fn from_info(info: ObjectInfo) -> Self {
        Self {
            url: format!("s3://{}", info.path),
            path: info.path,
            size: info.size,
            version: info.version,
            marked: info.marked,
        }
    }

    /// The bytes of this version, at most 1 MiB; a larger object is for a task.
    pub fn read(&self) -> Result<Vec<u8>> {
        let version = (!self.version.is_empty()).then_some(self.version.as_str());
        Ok(host::with(|host| host.object_read(&self.path, version))?)
    }

    pub fn read_text(&self) -> Result<String> {
        String::from_utf8(self.read()?)
            .map_err(|_| Error::Fail(format!("{} is not UTF-8", self.path)))
    }

    pub fn read_json<T: DeserializeOwned>(&self) -> Result<T> {
        Ok(serde_json::from_slice(&self.read()?)?)
    }
}

/// The current version of the object at `path`.
pub fn get(path: &str) -> Result<Object> {
    Ok(Object::from_info(host::with(|host| {
        host.object_info(path)
    })?))
}

/// A copy of the object at `source` to `target`, started with `.start()`.
pub fn copy(source: &str, target: &str) -> Copy {
    Copy {
        source: source.to_string(),
        target: target.to_string(),
        version: None,
    }
}

/// Every object below `prefix` the install may read, fetched one page at a time.
pub fn list(prefix: &str) -> impl Iterator<Item = Result<Object>> + use<> {
    let prefix = prefix.to_string();
    Pages::new(move |cursor| {
        let page = host::with(|host| host.object_list(&prefix, cursor.as_deref()))?;
        let objects = page.objects.into_iter().map(Object::from_info).collect();
        Ok((objects, page.cursor))
    })
}

#[cfg(test)]
#[path = "objects_tests.rs"]
mod tests;
