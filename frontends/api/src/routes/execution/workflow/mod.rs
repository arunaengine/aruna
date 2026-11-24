//! Serves the workflow registry and the tool catalog: handlers check transport input, run the
//! registry operations and map their errors to responses.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod events;
pub mod registry;
pub mod run_wire;
pub mod runs;
pub mod tasks;
pub mod tools;
pub mod versions;
pub mod wire;

use std::sync::Arc;

use aruna_core::workflow::WorkflowError;
use aruna_ops_work::workflows::CatalogError;
use aruna_ops_work::workflows::import::{GitInput, ObjectsInput, SourceInput};
use ulid::Ulid;
use utoipa_axum::router::OpenApiRouter;

use crate::error::ServerError;
use crate::server::state::ServerState;
use wire::{GitSourceBody, WorkflowSourceBody};

/// Items of a page when the caller names no limit.
const DEFAULT_LIMIT: usize = 50;
/// The most items one page holds.
const MAX_LIMIT: usize = 500;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    registry::router()
        .merge(versions::router())
        .merge(tools::router())
        .merge(runs::router())
        .merge(tasks::router())
        .merge(events::router())
}

fn limit(asked: Option<usize>) -> usize {
    asked.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
}

fn ulid(text: &str, name: &str) -> Result<Ulid, ServerError> {
    Ulid::from_string(text)
        .map_err(|_| ServerError::BadRequestReason(format!("{name} is not a ULID")))
}

/// A version number of the path; one that is no number names no version.
fn version_number(text: &str) -> Result<u32, ServerError> {
    text.parse().map_err(|_| ServerError::NotFound)
}

fn catalog_error(error: CatalogError) -> ServerError {
    match error {
        CatalogError::NotFound => ServerError::NotFound,
        CatalogError::Forbidden => ServerError::Forbidden,
        CatalogError::Invalid(message) => ServerError::BadRequestReason(message),
        CatalogError::Unsupported(message) => ServerError::NotSupported(message),
        CatalogError::Conflict(message) => ServerError::Conflict(message),
        CatalogError::Coordinator(WorkflowError::Refused(refusal)) => {
            ServerError::BadRequestReason(refusal.to_string())
        }
        CatalogError::Coordinator(error) => {
            ServerError::ServiceUnavailableReason(error.to_string())
        }
        CatalogError::Storage(message) => ServerError::InternalError(message.into()),
    }
}

fn git_input(git: GitSourceBody) -> GitInput {
    GitInput {
        url: git.url,
        revision: git.revision,
        path: git.path,
        entrypoint: git.entrypoint,
    }
}

/// The domain source of a request body, with file contents decoded.
fn source_input(source: WorkflowSourceBody) -> Result<SourceInput, ServerError> {
    Ok(match source {
        WorkflowSourceBody::UweYaml(text) => SourceInput::UweYaml(text),
        WorkflowSourceBody::Wir(document) => SourceInput::Wir(document),
        WorkflowSourceBody::Upload(upload) => SourceInput::Upload {
            files: (upload.files.into_iter().map(wire::SourceFileBody::decoded))
                .collect::<Result<_, _>>()?,
            entrypoint: upload.entrypoint,
        },
        WorkflowSourceBody::Objects(objects) => {
            let node_id =
                match objects.node_id.as_deref() {
                    Some(node) => Some(node.parse().map_err(|_| {
                        ServerError::BadRequestReason("node_id is no node id".into())
                    })?),
                    None => None,
                };
            SourceInput::Objects(ObjectsInput {
                bucket: objects.bucket,
                prefix: objects.prefix,
                keys: objects.keys,
                node_id,
                entrypoint: objects.entrypoint,
            })
        }
        WorkflowSourceBody::Git(git) => SourceInput::Git(git_input(git)),
    })
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

#[cfg(test)]
#[path = "run_tests.rs"]
mod run_tests;
