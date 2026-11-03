//! Lists the files one release pins, page by page, from its release crate: the exact version id
//! and size of every key, so a reader can browse and read the bucket as of the release.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::bucket_metadata::ReleasePointer;
use aruna_core::structs::storage::release::{
    CrateBytes, CrateRead, CrateReadError, ReleaseEffect, ReleaseEvent,
};
use aruna_core::structs::storage::replication::W3idIdentifier;
use aruna_core::types::Effects;
use serde_json::Value;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use super::{BucketScope, load_access};
use crate::driver::{DriverContext, drive};

/// The most entries one page lists.
pub const MAX_PAGE: usize = 1000;

#[derive(Debug, Error, PartialEq)]
pub enum ViewError {
    #[error("the release crate version does not exist")]
    Missing,
    #[error("the version is not a release")]
    NoRelease,
    #[error("the caller may not read the release crate")]
    Forbidden,
    #[error("the release crate cannot be read now: {0}")]
    Unavailable(String),
    #[error("the release crate is not valid JSON")]
    Malformed,
    #[error("unexpected event: {0}")]
    UnexpectedEvent(String),
}

/// One file a release pins, as its crate names it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PinnedFile {
    pub key: String,
    pub version_id: Ulid,
    pub size: u64,
    /// The exact id the crate names the version by.
    pub id: String,
    /// The content id, when the bytes had a known BLAKE3 at publication.
    pub content_id: Option<String>,
}

/// Which entries of the release one page lists.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FilePage {
    /// Only keys under this prefix.
    pub prefix: String,
    /// Groups keys below the next `/` after `prefix` into folders.
    pub folders: bool,
    /// The entry name the page starts after.
    pub after: Option<String>,
    pub limit: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReleaseView {
    pub label: String,
    /// The prefix the release pinned.
    pub prefix: String,
    pub files: Vec<PinnedFile>,
    /// Folder names ending with `/`, when `folders` was asked.
    pub folders: Vec<String>,
    /// The entry name the next page starts after; `None` on the last page.
    pub next: Option<String>,
}

#[derive(Debug, PartialEq)]
pub struct ViewConfig {
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub bucket: String,
    pub info: BucketInfo,
    pub auth: AuthContext,
    /// The release crate's key and version.
    pub key: String,
    pub version_id: Ulid,
    pub page: FilePage,
}

#[derive(Debug, PartialEq)]
enum ViewState {
    Init,
    Crate,
    Finish,
}

/// Reads one release crate version with the caller's identity, checks its release tag and
/// lists one page of the files it pins.
#[derive(Debug, PartialEq)]
pub struct ReleaseViewOperation {
    config: ViewConfig,
    state: ViewState,
    output: Option<Result<ReleaseView, ViewError>>,
}

impl ReleaseViewOperation {
    pub fn new(config: ViewConfig) -> Self {
        Self {
            config,
            state: ViewState::Init,
            output: None,
        }
    }

    fn finish(&mut self, output: Result<ReleaseView, ViewError>) -> Effects {
        self.state = ViewState::Finish;
        self.output = Some(output);
        smallvec![]
    }

    fn on_crate(&self, read: Result<CrateBytes, CrateReadError>) -> Result<ReleaseView, ViewError> {
        let read = match read {
            Ok(read) => read,
            Err(CrateReadError::Missing) => return Err(ViewError::Missing),
            Err(CrateReadError::Failed(reason)) => return Err(ViewError::Unavailable(reason)),
        };
        let pointer =
            ReleasePointer::of(&self.config.key, &read.metadata).ok_or(ViewError::NoRelease)?;
        let crate_json: Value =
            serde_json::from_slice(&read.bytes).map_err(|_| ViewError::Malformed)?;
        let (files, folders, next) = page(pinned_files(&crate_json), &self.config.page);
        Ok(ReleaseView {
            label: pointer.label,
            prefix: pointer.prefix,
            files,
            folders,
            next,
        })
    }
}

impl Operation for ReleaseViewOperation {
    type Output = ReleaseView;
    type Error = ViewError;

    fn start(&mut self) -> Effects {
        self.state = ViewState::Crate;
        let config = &self.config;
        let read = CrateRead {
            bucket: config.bucket.clone(),
            key: config.key.clone(),
            version_id: config.version_id,
            group_id: config.info.group_id,
            auth: config.auth.clone(),
            realm_id: config.realm_id,
            node_id: config.node_id,
        };
        let effect = ReleaseEffect::ReadCrate(Box::new(read));
        smallvec![Effect::Net(NetEffect::Release(Box::new(effect)))]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (&self.state, event) {
            (ViewState::Crate, Event::Net(NetEvent::Release(ReleaseEvent::CrateRead(read)))) => {
                let output = self.on_crate(read);
                self.finish(output)
            }
            (_, event) => self.finish(Err(ViewError::UnexpectedEvent(format!("{event:?}")))),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == ViewState::Finish
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .unwrap_or_else(|| Err(ViewError::UnexpectedEvent("not finished".into())))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// The files a release crate pins, in key order; entities without a key or version are left out.
pub fn pinned_files(crate_json: &Value) -> Vec<PinnedFile> {
    let graph = crate_json["@graph"].as_array().into_iter().flatten();
    let mut files: Vec<PinnedFile> = graph
        .filter(|entity| entity["@type"] == "File")
        .filter_map(|entity| {
            let id = entity["@id"].as_str()?;
            let version_id = match W3idIdentifier::parse(id).ok()? {
                W3idIdentifier::VersionedObject(arn) => arn.version,
                W3idIdentifier::Global(
                    aruna_core::structs::storage::global_id::GlobalId::Version {
                        version_id, ..
                    },
                ) => version_id,
                _ => return None,
            };
            Some(PinnedFile {
                key: entity["name"].as_str()?.to_string(),
                version_id,
                size: entity["contentSize"].as_str()?.parse().ok()?,
                id: id.to_string(),
                content_id: entity["identifier"].as_str().map(str::to_string),
            })
        })
        .collect();
    files.sort_by(|left, right| left.key.cmp(&right.key));
    files
}

/// One page of `files` under the page's prefix, folders grouped when asked, after `after`.
/// Keys are sorted, so the keys of one folder follow each other and names only grow.
fn page(files: Vec<PinnedFile>, page: &FilePage) -> (Vec<PinnedFile>, Vec<String>, Option<String>) {
    let limit = page.limit.clamp(1, MAX_PAGE);
    let after = page.after.as_deref().unwrap_or_default();
    let (mut listed, mut folders, mut last) = (Vec::new(), Vec::new(), None::<String>);
    for file in files
        .into_iter()
        .filter(|file| file.key.starts_with(&page.prefix))
    {
        let rest = &file.key[page.prefix.len()..];
        let folder = (page.folders)
            .then(|| rest.find('/'))
            .flatten()
            .map(|index| format!("{}{}", page.prefix, &rest[..=index]));
        let name = folder.clone().unwrap_or_else(|| file.key.clone());
        if name.as_str() <= after || last.as_ref() == Some(&name) {
            continue;
        }
        if listed.len() + folders.len() == limit {
            return (listed, folders, last);
        }
        last = Some(name);
        match folder {
            Some(folder) => folders.push(folder),
            None => listed.push(file),
        }
    }
    (listed, folders, None)
}

/// Lists one page of a release's files after checking the caller may read its crate.
pub async fn release_view(
    context: &DriverContext,
    scope: BucketScope<'_>,
    auth: &AuthContext,
    (key, version_id): (&str, Ulid),
    page: FilePage,
) -> Result<ReleaseView, ViewError> {
    let access = load_access(context, scope, auth)
        .await
        .map_err(|error| ViewError::Unavailable(error.to_string()))?;
    if !access.0.allows(Some(key), &Permission::READ) {
        return Err(ViewError::Forbidden);
    }
    let config = ViewConfig {
        realm_id: scope.realm_id,
        node_id: scope.node_id,
        bucket: scope.bucket.to_string(),
        info: scope.info.clone(),
        auth: auth.clone(),
        key: key.to_string(),
        version_id,
        page,
    };
    drive(ReleaseViewOperation::new(config), context).await
}

#[cfg(test)]
#[path = "view_tests.rs"]
mod tests;
