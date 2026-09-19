//! Owns the shared domain types, effects, events, keyspaces and the operation contract.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#![allow(clippy::result_large_err)]

pub mod admin_documents;
pub mod alpn;
pub mod audit;
pub mod auth;
pub mod compute;
pub mod credential_encryption;
pub mod document;
pub mod effects;
pub mod egress;
pub mod errors;
pub mod events;
pub mod handle;
pub mod id;
pub mod jobs;
pub mod join_request;
pub mod keys;
pub mod keyspaces;
pub mod metadata;
pub mod metrics;
pub mod onboarding;
pub mod operation;
pub mod permission_path;
pub mod reducer;
pub mod request_policy;
pub mod scheduling;
pub mod shutdown;
pub mod storage_entries;
pub mod stream;
pub mod structs;
pub mod structured_id;
pub mod task;
pub mod telemetry;
pub mod time;
pub mod trace_context;
pub mod types;
pub mod user;

#[cfg(test)]
mod tests;

pub use document::{
    DocumentApplyDecision, DocumentChange, DocumentChangeKind, DocumentEffect, DocumentEvent,
    DocumentEvictedDocument, DocumentNetEvent, DocumentSyncRevision, DocumentTarget,
};
pub use id::{DhtKeyId, NodeId, NodeIdExt, TopicId};
pub use keyspaces::*;
pub use metadata::*;
pub use onboarding::*;
pub use structured_id::{
    BucketId, ClockHealthError, IdEnvironment, JobId, MetaResourceId, PlacementHandle,
    StructuredId, StructuredIdGenerator, SystemEnvironment,
};
pub use task::{TaskEffect, TaskEvent, TaskKey};
pub use trace_context::DistributedTraceContext;
pub use user::id::UserId;
