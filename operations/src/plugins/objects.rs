//! Resolves the objects plugin calls and work name: the local bucket and key of a permission
//! path, and the version a read may use, which is refused when a job with a secret wrote it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::str::FromStr;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::GroupId;
use aruna_plugins::broker::{ErrorKind, HostError};
use ulid::Ulid;

use super::check::CallError;
use super::marks::{MARKED, is_marked};
use super::rows::{StoreError, mapped_child};
use super::unavailable;
use crate::s3::bucket::get::GetBucketOperation;
use crate::s3::object::head::{HeadObjectError, HeadObjectInput, HeadObjectOperation};

/// Whether a head found no such object or version.
pub(crate) fn absent(error: &HeadObjectError) -> bool {
    matches!(
        error,
        HeadObjectError::NoSuchKey | HeadObjectError::NoSuchVersion | HeadObjectError::DeleteMarker
    )
}

/// The bucket and key on `local` of an object path below the install's group data; a bucket of
/// another node or another group is refused.
#[derive(Debug, PartialEq)]
pub(crate) struct LocalObject {
    realm_id: RealmId,
    local: NodeId,
    group_id: GroupId,
    path: String,
    place: Option<(String, String)>,
}

impl LocalObject {
    pub(crate) fn new(
        (realm_id, local): (RealmId, NodeId),
        install: &PluginInstall,
        path: &str,
    ) -> Self {
        Self {
            realm_id,
            local,
            group_id: install.group_id,
            path: path.to_string(),
            place: None,
        }
    }

    fn parse(&self) -> Result<(String, String), HostError> {
        let path = &self.path;
        let invalid =
            || HostError::new(ErrorKind::Invalid, format!("{path} is not an object path"));
        let root = format!("/{}/g/{}/data/", self.realm_id, self.group_id);
        let rest = path.strip_prefix(&root).ok_or_else(invalid)?;
        let mut parts = rest.splitn(3, '/');
        let (node, bucket) = (
            parts.next().unwrap_or_default(),
            parts.next().ok_or_else(invalid)?,
        );
        let key = parts.next().unwrap_or_default().to_string();
        let node = NodeId::from_str(node).map_err(|_| invalid())?;
        if node != self.local {
            return Err(unavailable(
                "objects of another node are not reachable from plugins yet",
            ));
        }
        Ok((bucket.to_string(), key))
    }
}

impl Operation for LocalObject {
    type Output = (String, String);
    type Error = CallError;

    fn first(&mut self) -> Result<Next<Self::Output>, CallError> {
        let place = self.parse()?;
        let read = GetBucketOperation::new(place.0.clone());
        let group_id = self.group_id;
        self.place = Some(place);
        Ok(Next::Call(mapped_child(read, move |info| {
            Ok::<bool, ()>(info.is_ok_and(|info| info.group_id == group_id))
        })))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, CallError> {
        let found = result.child::<bool, ()>()?.unwrap_or(false);
        match (found, self.place.take()) {
            (true, Some(place)) => Ok(Next::Done(place)),
            _ => Err(CallError(HostError::new(
                ErrorKind::NotFound,
                format!("no bucket at {}", self.path),
            ))),
        }
    }
}

/// The version of `bucket/key` a read or copy reads (`version`, else the current one), refused
/// when it is marked; `None` when there is no such object, so nothing is read either.
#[derive(Debug, PartialEq)]
pub(crate) struct Unmarked {
    bucket: String,
    key: String,
    version: Option<Ulid>,
    resolved: Option<Ulid>,
}

impl Unmarked {
    pub(crate) fn new((bucket, key): (&str, &str), version: Option<Ulid>) -> Self {
        Self {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version,
            resolved: None,
        }
    }
}

impl Operation for Unmarked {
    type Output = Option<Ulid>;
    type Error = CallError;

    fn first(&mut self) -> Result<Next<Option<Ulid>>, CallError> {
        let head = HeadObjectOperation::new(HeadObjectInput {
            bucket: self.bucket.clone(),
            key: self.key.clone(),
            version_id: self.version,
        });
        let version = self.version;
        Ok(Next::Call(mapped_child(head, move |head| match head {
            Ok(head) => Ok(head.resolved_version_id.or(version)),
            Err(error) if absent(&error) => Ok(None),
            Err(error) => Err(error.to_string()),
        })))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<Ulid>>, CallError> {
        if let Some(resolved) = self.resolved {
            let marked = result.child::<bool, StoreError>()??;
            return match marked {
                false => Ok(Next::Done(Some(resolved))),
                true => Err(CallError(HostError::new(ErrorKind::Denied, MARKED))),
            };
        }
        let head = result.child::<Option<Ulid>, String>()?;
        let Some(resolved) = head.map_err(unavailable)? else {
            return Ok(Next::Done(None));
        };
        self.resolved = Some(resolved);
        let marked = is_marked((&self.bucket, &self.key, resolved))?;
        Ok(Next::Call(IoCall::child(marked)))
    }
}

#[cfg(test)]
#[path = "objects_tests.rs"]
mod tests;
