//! Authorizes native Git repositories and serves them from records on any document holder.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod access;
mod calls;
pub mod changes;
pub mod cleanup;
mod dataset;
pub mod download;
mod generate;
pub mod lfs;
pub mod linked;
pub mod location;
pub mod locks;
pub mod merge;
pub mod objects;
pub mod pending;
pub mod project;
pub mod publish;
pub mod push;
mod records;
pub mod snapshot;
pub mod state;
#[cfg(test)]
mod testing;
mod transport;
pub mod versions;

pub use self::access::{DocumentAccessOperation, RepositoryOperation, document, repository};
use self::changes::PropertyConflict;
pub use self::transport::{TransportOperation, transport};
use crate::auth::error::AuthorizeError;
use aruna_core::NodeId;
use aruna_core::structs::identity::realm::NodeRealm;
use aruna_core::structs::storage::routing::NodeRouting;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum GitError {
    #[error("repository not found")]
    NotFound,
    #[error("invalid Git or LFS request")]
    Invalid,
    #[error("repository state conflicts with the request")]
    Conflict,
    #[error(transparent)]
    Authorization(#[from] AuthorizeError),
    #[error("Git storage unavailable")]
    Unavailable,
    #[error("this node does not hold the document")]
    NotHolder,
    #[error("the document's Git history needs a checkpoint before more changes")]
    Full,
    #[error("{0} is locked by another user")]
    Locked(String),
    #[error("the branch moved since the version the request named")]
    Stale,
    #[error("the name already exists")]
    Exists,
    #[error("{0}")]
    Refused(String),
    #[error("the merge has conflicts")]
    MergeConflict(Box<MergeConflict>),
    #[error("only a push's own receive hook may record it")]
    NotHook,
    #[error("the branch does not exist")]
    BranchMissing,
}

impl From<aruna_core::calls::IoError> for GitError {
    fn from(_: aruna_core::calls::IoError) -> Self {
        Self::Unavailable
    }
}

/// Why a merge could not be completed; nothing was changed.
#[derive(Clone, Debug, PartialEq)]
pub struct MergeConflict {
    /// Files both sides changed that no metadata merge resolves.
    pub files: Vec<String>,
    pub properties: Vec<PropertyConflict>,
}

/// What the Git operations need to know about this node; the input reads it from its context.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct GitNode {
    pub ids: NodeRealm,
    pub routing: NodeRouting,
}

impl GitNode {
    pub(crate) fn node(&self) -> Option<NodeId> {
        self.ids.map(|(node_id, _)| node_id)
    }
}
