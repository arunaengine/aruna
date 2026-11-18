//! Runs the routed metadata and persistent id operations the integration tests call.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::UserId;
use aruna_core::id::NodeId;
use aruna_core::metadata::AuthToken;
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::secondary_id::SecondaryIdentifier;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::structs::{MintPersistentSpec, PersistentIdMapping};
use aruna_operations::auth::request_policy::PolicyRequestExtras;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::forward::transport::MetadataWriteError;
use aruna_operations::jobs::submit::{SubmitJobError, SubmitJobResult};
use aruna_operations::metadata::api::MetadataApiError;
use aruna_operations::metadata::api::export_types::GetVisibleRequest;
use aruna_operations::metadata::forward::authorized_create::CreateAuthorizedError;
use aruna_operations::metadata::forward::authorized_create::CreateWithIdOperation;
use aruna_operations::metadata::forward::pinned::PinnedCreate;
use aruna_operations::metadata::forward::routed_read::RoutedReadOperation;
use aruna_operations::metadata::forward::routed_write::RouteDeleteOperation;
use aruna_operations::metadata::id_mapping::PersistentIdError;
use aruna_operations::metadata::persistent_id::MappingReadOperation;
use aruna_operations::metadata::persistent_id::forward::{
    PidRouteOperation, outcome_mapping, outcome_resolution, outcome_submission, submission_error,
};
use aruna_operations::metadata::{PersistentIdRequest, PersistentIdResolution};
use ulid::Ulid;

fn local_node(context: &DriverContext) -> Option<NodeId> {
    context.node_and_realm().map(|(node_id, _)| node_id)
}

pub async fn route_metadata_delete(
    context: &Arc<DriverContext>,
    actor: Actor,
    record: Option<&MetadataRegistryRecord>,
    document_id: Ulid,
    token: Option<AuthToken>,
) -> Result<(), MetadataWriteError> {
    let node = (actor, local_node(context));
    let delete = RouteDeleteOperation::new(node, record.cloned(), document_id, token);
    drive(delete, context).await
}

pub async fn get_metadata_routed(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    request: GetVisibleRequest,
    token: Option<AuthToken>,
) -> Result<MetadataRegistryRecord, MetadataApiError> {
    let read = RoutedReadOperation::new(local_node(context), realm_id, request, token);
    drive(read, context).await
}

pub async fn create_with_id(
    context: &Arc<DriverContext>,
    actor: Actor,
    (auth, extras): (&AuthContext, PolicyRequestExtras),
    create: &PinnedCreate,
) -> Result<MetadataRegistryRecord, CreateAuthorizedError> {
    let node = (actor, local_node(context));
    let create = CreateWithIdOperation::new(node, (auth.clone(), extras), create.clone());
    drive(create, context).await
}

pub async fn read_mapping(
    context: &DriverContext,
    document_id: Ulid,
) -> Result<Option<PersistentIdMapping>, PersistentIdError> {
    drive(MappingReadOperation::new(document_id), context).await
}

async fn pid_request(
    context: &Arc<DriverContext>,
    (realm_id, document_id): (RealmId, Ulid),
    request: PersistentIdRequest,
    caller: (Option<AuthToken>, Option<AuthContext>),
) -> Result<aruna_operations::metadata::PersistentIdOutcome, MetadataApiError> {
    let node = (realm_id, local_node(context));
    drive(
        PidRouteOperation::new(node, document_id, request, caller),
        context,
    )
    .await
}

pub async fn mint_pid_routed(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    document_id: Ulid,
    minted_by: UserId,
    minted_at_ms: u64,
    token: Option<AuthToken>,
) -> Result<(PersistentIdMapping, bool), MetadataApiError> {
    let request = PersistentIdRequest::Mint {
        minted_by,
        minted_at_ms,
    };
    let outcome = pid_request(context, (realm_id, document_id), request, (token, None)).await;
    outcome.and_then(outcome_mapping)
}

pub async fn withdraw_pid_routed(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    document_id: Ulid,
    withdrawn_by: UserId,
    reason: String,
    withdrawn_at_ms: u64,
    token: Option<AuthToken>,
) -> Result<PersistentIdMapping, MetadataApiError> {
    let request = PersistentIdRequest::Withdraw {
        withdrawn_by,
        reason,
        withdrawn_at_ms,
    };
    let outcome = pid_request(context, (realm_id, document_id), request, (token, None)).await;
    outcome
        .and_then(outcome_mapping)
        .map(|(mapping, _)| mapping)
}

pub async fn resolve_pid_routed(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    document_id: Ulid,
    pid: String,
) -> Result<PersistentIdResolution, MetadataApiError> {
    let request = PersistentIdRequest::Resolve { pid };
    let outcome = pid_request(context, (realm_id, document_id), request, (None, None)).await;
    outcome.and_then(outcome_resolution)
}

pub async fn submit_mint_pid(
    context: &Arc<DriverContext>,
    spec: MintPersistentSpec,
    local_node_id: NodeId,
    retention_ms: u64,
    token: Option<AuthToken>,
) -> Result<SubmitJobResult, SubmitJobError> {
    let realm_id = context.serving_realm(spec.minted_by.realm_id);
    let request = PersistentIdRequest::SubmitMint {
        minted_by: spec.minted_by,
        retention_ms,
    };
    let node = (realm_id, local_node(context));
    let route = PidRouteOperation::new(node, spec.document_id, request, (token, None))
        .with_owner(local_node_id);
    let outcome = drive(route, context).await.map_err(submission_error)?;
    outcome_submission(outcome)
}

pub async fn add_identifiers_routed(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    document_id: Ulid,
    identifiers: Vec<SecondaryIdentifier>,
    occurred_at_ms: u64,
    auth: AuthContext,
) -> Result<(PersistentIdMapping, bool), MetadataApiError> {
    let request = PersistentIdRequest::AddIdentifiers {
        identifiers,
        occurred_at_ms,
    };
    let outcome = pid_request(
        context,
        (realm_id, document_id),
        request,
        (None, Some(auth)),
    )
    .await;
    outcome.and_then(outcome_mapping)
}

#[allow(clippy::too_many_arguments)]
pub async fn route_metadata_update(
    context: &Arc<DriverContext>,
    actor: Actor,
    record: Option<&MetadataRegistryRecord>,
    document_id: Ulid,
    public: Option<bool>,
    mutation: aruna_operations::metadata::update_document::UpdateDocumentMutation,
    expected_revision: Option<Ulid>,
    token: Option<AuthToken>,
    commit_message: Option<String>,
) -> Result<MetadataRegistryRecord, MetadataWriteError> {
    use aruna_operations::metadata::forward::routed_update::{
        DocumentUpdate, RouteUpdateOperation,
    };
    let update = DocumentUpdate {
        public,
        mutation,
        expected_revision,
        commit_message,
    };
    let node = (actor, local_node(context));
    let route = RouteUpdateOperation::new(node, (record.cloned(), document_id), update, token);
    drive(route, context).await
}
