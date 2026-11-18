//! Decides a plugin copy without moving a byte: the grants, both objects and the source version
//! that together form the copy's input, and what a source behind a locked key waits for.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::errors::ConversionError;
use aruna_core::operation::Operation;
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::structs::execution::job::KeyWait;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::blob::VersionKey;
use aruna_core::structs::storage::encryption::BucketKeyError;
use aruna_core::types::GroupId;
use ulid::Ulid;

use super::check::{CallError, CopyGrants};
use super::objects::{LocalObject, Unmarked};
use super::transfer_steps::account;
use crate::jobs::key_locks::{KeyCheckError, VersionWaitOperation};
use crate::s3::object::copy::{
    CopyObjectError, CopyObjectInput, CopyReferences, CopySourceConditions,
};
use crate::s3::object::get::GetObjectError;
use crate::s3::object::head::{
    HeadObjectError, HeadObjectInput, HeadObjectOperation, HeadObjectResult,
};

/// A bucket and key inside it.
type Place = (String, String);

/// The admitted copy one attempt prepares.
#[derive(Clone, Copy)]
pub struct CopyWork<'a> {
    pub node: (RealmId, NodeId),
    pub job: Ulid,
    pub config: &'a RealmConfigDocument,
    pub install: &'a PluginInstall,
    pub source: &'a str,
    pub version: Option<Ulid>,
    pub target: &'a str,
}

#[derive(Debug, PartialEq)]
enum CopyPhase {
    Grants,
    Source(BTreeSet<String>),
    Target(BTreeSet<String>, Place),
    Readable(BTreeSet<String>, Place, Place),
}

/// Builds the input of a copy that runs as the account, restricted to its current read patterns
/// of the source and write patterns of the target.
#[derive(Debug, PartialEq)]
pub struct PrepareCopy {
    node: (RealmId, NodeId),
    job: Ulid,
    ceiling: Option<u64>,
    install: PluginInstall,
    source: String,
    version: Option<Ulid>,
    target: String,
    phase: CopyPhase,
}

impl PrepareCopy {
    pub fn new(work: CopyWork<'_>) -> Self {
        let group = work.install.group_id;
        Self {
            node: work.node,
            job: work.job,
            ceiling: work.config.quota.effective_group_ceiling(&group),
            install: work.install.clone(),
            source: work.source.to_string(),
            version: work.version,
            target: work.target.to_string(),
            phase: CopyPhase::Grants,
        }
    }

    fn object(&self, path: &str) -> IoCall {
        IoCall::child(LocalObject::new(self.node, &self.install, path))
    }

    fn input(
        &self,
        patterns: &BTreeSet<String>,
        (source_bucket, source_key): Place,
        (dest_bucket, dest_key): Place,
        source_version_id: Option<Ulid>,
    ) -> CopyObjectInput {
        let auth = account(&self.install, patterns);
        CopyObjectInput {
            source_bucket,
            source_key,
            source_version_id,
            source_group_id: self.install.group_id,
            source_auth_context: auth.clone(),
            dest_bucket,
            dest_key,
            user_id: self.install.account,
            group_id: self.install.group_id,
            realm_id: self.node.0,
            node_id: self.node.1,
            quota_ceiling: self.ceiling,
            conditions: CopySourceConditions::default(),
            metadata: None,
            restrictions: auth.path_restrictions,
            references: CopyReferences::Materialize,
            job_id: Some(self.job),
        }
    }
}

impl Operation for PrepareCopy {
    type Output = CopyObjectInput;
    type Error = CallError;

    fn first(&mut self) -> Result<Next<CopyObjectInput>, CallError> {
        let paths = (self.source.as_str(), self.target.as_str());
        Ok(Next::Call(IoCall::child(CopyGrants::new(
            &self.install,
            paths,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<CopyObjectInput>, CallError> {
        match std::mem::replace(&mut self.phase, CopyPhase::Grants) {
            CopyPhase::Grants => {
                let patterns = result.child::<BTreeSet<String>, CallError>()??;
                self.phase = CopyPhase::Source(patterns);
                Ok(Next::Call(self.object(&self.source)))
            }
            CopyPhase::Source(patterns) => {
                let source = result.child::<Place, CallError>()??;
                self.phase = CopyPhase::Target(patterns, source);
                Ok(Next::Call(self.object(&self.target)))
            }
            CopyPhase::Target(patterns, source) => {
                let target = result.child::<Place, CallError>()??;
                let readable = Unmarked::new((&source.0, &source.1), self.version);
                self.phase = CopyPhase::Readable(patterns, source, target);
                Ok(Next::Call(IoCall::child(readable)))
            }
            CopyPhase::Readable(patterns, source, target) => {
                // The version checked is the version copied, so a newer marked one never slips in.
                let found = result.child::<Option<Ulid>, CallError>()??;
                let version = found.or(self.version);
                Ok(Next::Done(self.input(&patterns, source, target, version)))
            }
        }
    }
}

/// Whether a read stopped at a locked bucket key.
pub fn read_locked(error: &GetObjectError) -> bool {
    matches!(
        error,
        GetObjectError::ConversionError(ConversionError::BucketKey(BucketKeyError::Locked(_)))
    )
}

/// Whether a copy stopped at the locked key of its source.
pub fn copy_locked(error: &CopyObjectError) -> bool {
    matches!(error, CopyObjectError::Get(read) if read_locked(read))
}

/// Finds the key a locked source version waits for; `None` means it was unlocked meanwhile.
#[derive(Debug, PartialEq)]
pub struct LockedSource {
    place: (NodeId, GroupId),
    bucket: String,
    key: String,
    version: Option<Ulid>,
    described: bool,
}

impl LockedSource {
    pub fn new(
        node: NodeId,
        install: &PluginInstall,
        (bucket, key, version): (String, String, Option<Ulid>),
    ) -> Self {
        Self {
            place: (node, install.group_id),
            bucket,
            key,
            version,
            described: false,
        }
    }
}

impl Operation for LockedSource {
    type Output = Option<KeyWait>;
    type Error = KeyCheckError;

    fn first(&mut self) -> Result<Next<Option<KeyWait>>, KeyCheckError> {
        let head = HeadObjectOperation::new(HeadObjectInput {
            bucket: self.bucket.clone(),
            key: self.key.clone(),
            version_id: self.version,
        });
        Ok(Next::Call(IoCall::child(head)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<KeyWait>>, KeyCheckError> {
        if self.described {
            let wait = result.child::<Option<KeyWait>, KeyCheckError>()??;
            return Ok(Next::Done(wait));
        }
        let head = result.child::<HeadObjectResult, HeadObjectError>();
        let Some(version) = head
            .ok()
            .and_then(Result::ok)
            .and_then(|head| head.resolved_version_id)
        else {
            let unknown = "the locked source version could not be described";
            return Err(KeyCheckError(unknown.to_string()));
        };
        self.described = true;
        let version = VersionKey::new(self.bucket.clone(), self.key.clone(), version);
        let wait = VersionWaitOperation::new(self.place, version);
        Ok(Next::Call(IoCall::child(wait)))
    }
}

#[cfg(test)]
#[path = "copy_steps_tests.rs"]
mod tests;
