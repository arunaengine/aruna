//! Takes in what a visitor or guest hands an applet, an uploaded file or a submitted form, and
//! writes it as the applet's own account below the visitor's folder of the submission folder.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::applet::record::{Applet, Folder};
use aruna_core::applet::usage::caps;
use aruna_core::bucket_read::{GetBucketError, GetBucketOperation};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::git::{GitNode, StoredObject};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{BucketInfo, object_permission_path};
use aruna_ops_data::git::GitError;
use bytes::Bytes;

use super::release::{git_error, put_named};
use super::visits::CountVisitOperation;
use super::{AppletError, allowed, authorize_path};

/// Who hands something in: a visitor or guest by its pseudonym.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Visit {
    pub pseudonym: String,
    pub guest: bool,
}

/// The context the applet's account acts in, restricted to exactly the paths of its role.
pub fn applet_auth(applet: &Applet, realm_id: RealmId) -> Option<AuthContext> {
    let account = applet.account.filter(|_| applet.role_id.is_some())?;
    let restrictions = (applet.role_paths(realm_id, applet.host_node).into_iter())
        .map(|(pattern, permission)| PathRestriction {
            pattern,
            permission,
        })
        .collect();
    Some(AuthContext {
        user_id: account,
        realm_id,
        path_restrictions: Some(restrictions),
        session: None,
    })
}

/// The key one hand-in gets: below the visitor's folder, named by its time and its name.
pub fn intake_key(folder: &Folder, pseudonym: &str, now_ms: u64, name: &str) -> String {
    let prefix = folder.prefix.trim_end_matches('/');
    let name = name.rsplit('/').next().unwrap_or_default();
    match prefix {
        "" => format!("{pseudonym}/{now_ms:013}-{name}"),
        prefix => format!("{prefix}/{pseudonym}/{now_ms:013}-{name}"),
    }
}

#[derive(Debug, PartialEq)]
enum IntakeStep {
    Count,
    Bucket,
    Allowed(Box<BucketInfo>),
    Put,
}

/// Writes one hand-in of `visit` to a live applet's submission folder, after counting it
/// against the visitor's caps. The applet's account must hold WRITE there.
#[derive(Debug, PartialEq)]
pub struct IntakeOperation {
    node: GitNode,
    applet: Applet,
    visit: Visit,
    name: String,
    bytes: Option<Bytes>,
    auth: Option<AuthContext>,
    key: String,
    now_ms: u64,
    step: IntakeStep,
}

impl IntakeOperation {
    pub fn new(
        node: &GitNode,
        applet: &Applet,
        visit: Visit,
        (name, bytes): (&str, Bytes),
    ) -> Self {
        Self {
            node: node.clone(),
            applet: applet.clone(),
            visit,
            name: name.to_string(),
            bytes: Some(bytes),
            auth: None,
            key: String::new(),
            now_ms: 0,
            step: IntakeStep::Count,
        }
    }

    fn folder(&self) -> Result<&Folder, AppletError> {
        (self.applet.grants.submissions.as_ref()).ok_or(AppletError::Forbidden)
    }

    /// The most bytes one hand-in of this visitor may hold.
    fn max_bytes(&self) -> Option<u64> {
        match (&self.applet.guests, self.visit.guest) {
            (Some(policy), true) => Some(policy.file_bytes),
            (None, true) => Some(0),
            (_, false) => self.applet.grants.limits.upload_bytes,
        }
    }
}

impl Operation for IntakeOperation {
    type Output = StoredObject;
    type Error = AppletError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<StoredObject>, AppletError> {
        self.folder()?;
        let realm_id = self
            .node
            .ids
            .map(|(_, realm)| realm)
            .ok_or(IoError::Unfinished)?;
        self.auth = Some(applet_auth(&self.applet, realm_id).ok_or(AppletError::Forbidden)?);
        let size = self.bytes.as_ref().map_or(0, |bytes| bytes.len() as u64);
        if let Some(max) = self.max_bytes().filter(|max| size > *max) {
            return Err(AppletError::Limited(format!(
                "a hand-in holds at most {max} bytes"
            )));
        }
        let caps = caps(&self.applet, self.visit.guest);
        let count = CountVisitOperation::new(self.applet.applet_id, &self.visit.pseudonym, caps);
        Ok(Next::Call(IoCall::child(count)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<StoredObject>, AppletError> {
        match std::mem::replace(&mut self.step, IntakeStep::Put) {
            IntakeStep::Count => {
                result.child::<(), AppletError>()??;
                let folder = self.folder()?.clone();
                self.key = intake_key(&folder, &self.visit.pseudonym, self.now_ms, &self.name);
                self.step = IntakeStep::Bucket;
                let read = GetBucketOperation::new(folder.bucket.clone());
                Ok(Next::Call(IoCall::child(read)))
            }
            IntakeStep::Bucket => {
                let bucket = match result.child::<BucketInfo, GetBucketError>()? {
                    Ok(bucket) if bucket.group_id == self.applet.group_id => bucket,
                    Ok(_) | Err(GetBucketError::NotFound) => return Err(AppletError::Forbidden),
                    Err(error) => return Err(AppletError::Storage(error.to_string())),
                };
                let auth = self.auth.as_ref().ok_or(IoError::Unfinished)?;
                let folder = self.folder()?;
                let applet = &self.applet;
                let path = object_permission_path(
                    auth.realm_id,
                    applet.group_id,
                    applet.host_node,
                    &folder.bucket,
                    &self.key,
                );
                let check = authorize_path(auth, &path, Permission::WRITE);
                self.step = IntakeStep::Allowed(Box::new(bucket));
                Ok(Next::Call(check))
            }
            IntakeStep::Allowed(bucket) => {
                if !allowed(result)? {
                    return Err(AppletError::Forbidden);
                }
                let auth = self.auth.as_ref().ok_or(IoError::Unfinished)?;
                let bytes = self.bytes.take().ok_or(IoError::Unfinished)?;
                let folder = self.folder()?.bucket.clone();
                let target = (folder.as_str(), *bucket);
                let call = put_named(&self.node, auth, target, self.key.clone(), bytes);
                Ok(Next::Call(call))
            }
            IntakeStep::Put => result
                .child::<StoredObject, GitError>()?
                .map_err(git_error)
                .map(Next::Done),
        }
    }

    fn expected_error(error: &AppletError) -> bool {
        !matches!(error, AppletError::Storage(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_stay_inside() {
        let folder = Folder {
            bucket: "uploads".into(),
            prefix: "bakta/".into(),
        };
        let key = intake_key(&folder, "ab", 42, "../../etc/reads.fasta");
        assert_eq!(key, "bakta/ab/0000000000042-reads.fasta");
        let root = Folder {
            bucket: "uploads".into(),
            prefix: String::new(),
        };
        assert_eq!(
            intake_key(&root, "ab", 1, "a.json"),
            "ab/0000000000001-a.json"
        );
    }
}
