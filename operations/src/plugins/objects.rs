//! Resolves the objects plugin calls and work name: the local bucket and key of a permission
//! path, and the version a read may use, which is refused when a job with a secret wrote it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::str::FromStr;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::events::{BlobEvent, Event};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::GroupId;
use aruna_plugin_types::calls::{ErrorKind, HostError};
use aruna_plugin_types::permission::Permission;
use ulid::Ulid;

use super::check::{CallError, RunScope};
use super::host_calls::version_id;
use super::host_steps::{Granted, GrantedCall};
use super::marks::{MARKED, is_marked};
use super::rows::{StoreError, mapped_child};
use super::unavailable;
use crate::s3::object::get::{GetObjectError, GetObjectInput, GetObjectOperation, GetObjectResult};
use crate::s3::object::head::{HeadObjectError, HeadObjectInput, HeadObjectOperation};
use aruna_core::bucket_read::GetBucketOperation;

/// Whether a head found no such object or version.
pub fn absent(error: &HeadObjectError) -> bool {
    matches!(
        error,
        HeadObjectError::NoSuchKey | HeadObjectError::NoSuchVersion | HeadObjectError::DeleteMarker
    )
}

/// The bucket and key on `local` of an object path below the install's group data; a bucket of
/// another node or another group is refused.
#[derive(Debug, PartialEq)]
pub struct LocalObject {
    realm_id: RealmId,
    local: NodeId,
    group_id: GroupId,
    path: String,
    place: Option<(String, String)>,
}

impl LocalObject {
    pub fn new((realm_id, local): (RealmId, NodeId), install: &PluginInstall, path: &str) -> Self {
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
pub struct Unmarked {
    bucket: String,
    key: String,
    version: Option<Ulid>,
    resolved: Option<Ulid>,
}

impl Unmarked {
    pub fn new((bucket, key): (&str, &str), version: Option<Ulid>) -> Self {
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

/// A byte bound as people read it: `1 MiB` rather than `1048576 bytes`.
fn byte_size(bytes: usize) -> String {
    match bytes % (1 << 20) {
        0 => format!("{} MiB", bytes >> 20),
        _ => format!("{bytes} bytes"),
    }
}

#[derive(Debug, PartialEq)]
enum ReadPhase {
    Grant,
    Locate(Box<Granted>),
    Version(Box<Granted>, (String, String)),
    Get,
    Collect,
}

/// The bytes of the object at the full path `path` below the install's group data, the current
/// or the given version, under the read grants; one larger than `max_bytes` is for a task.
#[derive(Debug, PartialEq)]
pub struct ReadObject {
    scope: RunScope,
    path: String,
    version: Option<String>,
    max_bytes: usize,
    phase: ReadPhase,
}

impl ReadObject {
    pub fn new(scope: &RunScope, path: &str, version: Option<String>, max_bytes: usize) -> Self {
        Self {
            scope: scope.clone(),
            path: path.to_string(),
            version,
            max_bytes,
            phase: ReadPhase::Grant,
        }
    }

    fn missing(&self) -> CallError {
        let path = &self.path;
        CallError(HostError::new(
            ErrorKind::NotFound,
            format!("no object at {path}"),
        ))
    }

    fn too_large(&self) -> CallError {
        let message = format!(
            "objects larger than {} cannot be read by a plugin; give them to a task",
            byte_size(self.max_bytes)
        );
        CallError(HostError::new(ErrorKind::Limit, message))
    }
}

impl Operation for ReadObject {
    type Output = Vec<u8>;
    type Error = CallError;

    fn first(&mut self) -> Result<Next<Vec<u8>>, CallError> {
        let read = Permission::ObjectsRead;
        let check = GrantedCall::target(&self.scope, read, &self.path);
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<u8>>, CallError> {
        match std::mem::replace(&mut self.phase, ReadPhase::Get) {
            ReadPhase::Grant => {
                let granted = result.child::<Granted, CallError>()??;
                let place = (self.scope.realm_id, self.scope.local);
                let locate = LocalObject::new(place, &granted.install, &self.path);
                self.phase = ReadPhase::Locate(Box::new(granted));
                Ok(Next::Call(IoCall::child(locate)))
            }
            ReadPhase::Locate(granted) => {
                let (bucket, key) = result.child::<(String, String), CallError>()??;
                let version = (self.version.as_deref()).map(version_id).transpose()?;
                let unmarked = Unmarked::new((&bucket, &key), version);
                self.phase = ReadPhase::Version(granted, (bucket, key));
                Ok(Next::Call(IoCall::child(unmarked)))
            }
            ReadPhase::Version(granted, (bucket, key)) => {
                let version = result.child::<Option<Ulid>, CallError>()??;
                let Granted { install, auth } = *granted;
                let input = GetObjectInput {
                    bucket,
                    key,
                    version_id: Some(version.ok_or_else(|| self.missing())?),
                    range: None,
                    group_id: install.group_id,
                    user_identity: install.account,
                    realm_id: self.scope.realm_id,
                    node_id: self.scope.local,
                };
                let read = GetObjectOperation::new(input).with_restrictions(auth.path_restrictions);
                Ok(Next::Call(IoCall::child(read)))
            }
            ReadPhase::Get => {
                let got = match result.child::<GetObjectResult, GetObjectError>()? {
                    Ok(got) => got,
                    Err(GetObjectError::NoSuchKey | GetObjectError::NoSuchVersion) => {
                        return Err(self.missing());
                    }
                    Err(other) => return Err(CallError(unavailable(other.to_string()))),
                };
                if got.info.size > self.max_bytes as u64 {
                    return Err(self.too_large());
                }
                self.phase = ReadPhase::Collect;
                let collect = BlobEffect::CollectStream {
                    blob: got.blob,
                    limit: Some(self.max_bytes as u64),
                };
                Ok(Next::Call(IoCall::Effect(Box::new(Effect::Blob(collect)))))
            }
            ReadPhase::Collect => match result.event()? {
                Event::Blob(BlobEvent::StreamCollected { bytes: Ok(bytes) }) => {
                    Ok(Next::Done(bytes))
                }
                Event::Blob(BlobEvent::StreamTooLarge { .. }) => Err(self.too_large()),
                Event::Blob(BlobEvent::StreamCollected { bytes: Err(error) }) => {
                    Err(CallError(unavailable(error)))
                }
                other => Err(IoError::Unexpected {
                    expected: "BlobEvent::StreamCollected",
                    got: other.label(),
                }
                .into()),
            },
        }
    }
}

#[cfg(test)]
#[path = "objects_tests.rs"]
mod tests;
