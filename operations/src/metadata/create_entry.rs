//! Runs a metadata document create: id minting, Profile validation, the retried operation and
//! the projection and materialization wakeups.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::MetaResourceId;
use aruna_core::handle::Handle;
use aruna_core::metadata::MetadataError;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::types::GroupId;

use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::driver::{DriverContext, drive, drive_retry};
use crate::metadata::create_document::{
    CreateDocumentConfig, CreateDocumentError, CreateDocumentOperation, CreateDocumentPayload,
    CreateDocumentResult, mint_document_for,
};
use crate::metadata::profile::rules::not_profiled_status;
use crate::metadata::profile::validation::validate_submission;
use crate::metadata::projector::schedule_projection_drain;

pub async fn create_metadata_document(
    mut template: CreateDocumentOperation,
    context: Arc<DriverContext>,
) -> Result<CreateDocumentResult, CreateDocumentError> {
    // Only local nil-sentinel creates mint here; supplied and forwarded ids persist.
    if !template.forwarded && template.config.document_id.is_nil() {
        let document_id = mint_local_id(context.as_ref(), &template.config).await?;
        template.config.document_id = document_id.as_ulid();
    }
    template.route_profile_status = Some(match &template.config.payload {
        CreateDocumentPayload::RoCrate { jsonld } => {
            validate_submission(
                context.as_ref(),
                template.config.document_id,
                template.config.group_id,
                jsonld,
            )
            .await?
        }
        CreateDocumentPayload::Scaffold { .. } => not_profiled_status(template.config.document_id),
    });
    let seed = template.config.document_id.to_bytes();
    let created = drive_retry(|| template.fresh_copy(), context.as_ref(), &seed).await?;
    schedule_projection_drain(context.as_ref(), std::time::Duration::ZERO)
        .await
        .map_err(|error| MetadataError::Backend(error.to_string()))?;
    if let Some(task_handle) = context.task_handle.as_ref() {
        task_handle
            .send_effect(crate::metadata::repository::schedule_materialization())
            .await;
    }
    Ok(created)
}

/// Mints from a local held bucket and a pre-provisioned binding.
/// Missing holdership or bindings fail closed for routed handling.
pub(crate) async fn mint_local_id(
    context: &DriverContext,
    config: &CreateDocumentConfig,
) -> Result<MetaResourceId, CreateDocumentError> {
    let realm_config = drive(GetConfigOperation::new(config.actor.realm_id), context)
        .await
        .map_err(config_error)?;
    mint_document_for(
        &realm_config,
        &config.actor,
        config.group_id,
        &config.document_path,
        false,
    )
}

/// Mints a job-produced metadata document id with the blind placement.
pub async fn mint_job_document(
    context: &DriverContext,
    actor: &Actor,
    group_id: GroupId,
    document_path: &str,
) -> Result<MetaResourceId, CreateDocumentError> {
    let realm_config = drive(GetConfigOperation::new(actor.realm_id), context)
        .await
        .map_err(config_error)?;
    mint_document_for(&realm_config, actor, group_id, document_path, true)
}

/// Maps a realm config read failure into the create error; a missing config fails closed.
fn config_error(error: GetConfigError) -> CreateDocumentError {
    match error {
        GetConfigError::StorageError(error) => error.into(),
        GetConfigError::ConversionError(error) => error.into(),
        GetConfigError::DocumentNotFound => CreateDocumentError::PlacementBindingUnavailable(
            "realm config document missing".to_string(),
        ),
        other => CreateDocumentError::PlacementBindingUnavailable(format!(
            "unexpected storage event reading realm config: {other}"
        )),
    }
}
