//! Runs the repository operations the way their callers do, for the Invenio driver tests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::repository::{
    RemoteState, RepositoryCredential, RepositoryDestination, RepositoryLink, RepositoryQuery,
};
use aruna_core::structs::execution::harvest::RepositoryConnectorKind;
use aruna_core::structs::execution::job::JobId;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::jobs::repository::connector::ConnectorViewOperation;
use aruna_operations::jobs::repository::invenio::query::{
    ResolveRecordOperation, SearchRecordsOperation,
};
use aruna_operations::jobs::repository::invenio::remote::RemoteStateOperation;
use aruna_operations::jobs::repository::link_queue::{
    DrainLinksOperation, OwnerHoldsOperation, PushRevisionOperation, StartPushOperation,
    revision_event,
};
use aruna_operations::jobs::repository::link_store::{ApplyChangeOperation, ListLinksOperation};
use aruna_operations::jobs::repository::links::{LinkChange, LinkError};
use aruna_operations::jobs::repository::pull::StartPullOperation;
use aruna_operations::jobs::repository::{NodeScope, RecordReference, TransferError, seal_token};
use serde_json::Value;
use ulid::Ulid;

pub(super) async fn change_link(
    context: &DriverContext,
    link: &RepositoryLink,
    change: LinkChange,
) -> Result<Option<RepositoryLink>, LinkError> {
    let operation = ApplyChangeOperation::new(NodeScope::of(context), link.clone(), change);
    drive(operation, context).await
}

pub(super) async fn list_links(
    context: &DriverContext,
    document_id: Ulid,
) -> Result<Vec<(RepositoryLink, bool)>, LinkError> {
    drive(ListLinksOperation::new(document_id), context).await
}

pub(super) async fn current_event(
    context: &DriverContext,
    document_id: Ulid,
) -> Result<Ulid, LinkError> {
    revision_event(drive(PushRevisionOperation::new(document_id), context).await?)
}

pub(super) async fn start_push(
    context: &DriverContext,
    link: &RepositoryLink,
    event_id: Ulid,
    publish: bool,
) -> Result<JobId, LinkError> {
    let operation =
        StartPushOperation::new(NodeScope::of(context), link.clone(), event_id, publish);
    drive(operation, context).await
}

pub(super) async fn start_pull(
    context: &DriverContext,
    link: &RepositoryLink,
) -> Result<JobId, LinkError> {
    drive(
        StartPullOperation::new(NodeScope::of(context), link.clone()),
        context,
    )
    .await
}

pub(super) async fn drain_links(context: &DriverContext) -> Result<Option<Duration>, LinkError> {
    drive(DrainLinksOperation::new(NodeScope::of(context)), context).await
}

pub(super) async fn owner_holds(context: &DriverContext, link: &RepositoryLink) -> bool {
    let holds = OwnerHoldsOperation::new(NodeScope::of(context), link);
    drive(holds, context).await.unwrap_or(true)
}

pub(super) async fn remote_state(
    _kind: RepositoryConnectorKind,
    context: &DriverContext,
    link: &RepositoryLink,
) -> Result<RemoteState, TransferError> {
    drive(
        RemoteStateOperation::new(NodeScope::of(context), link.clone()),
        context,
    )
    .await
}

pub(super) async fn seal_credential(
    context: &DriverContext,
    auth: &AuthContext,
    destination: &RepositoryDestination,
    token: &str,
) -> Result<RepositoryCredential, TransferError> {
    let connector = ConnectorViewOperation::new(destination.group_id, destination.connector_id);
    let view = drive(connector, context).await?;
    seal_token(context, auth.user_id, &view, None, token)
}

pub(super) async fn seal_link_token(
    context: &DriverContext,
    user: aruna_core::UserId,
    group_id: Ulid,
    connector_id: Ulid,
    link_id: Ulid,
    token: &str,
) -> Result<RepositoryCredential, TransferError> {
    let view = drive(ConnectorViewOperation::new(group_id, connector_id), context).await?;
    seal_token(context, user, &view, Some(link_id), token)
}

pub(super) async fn search(
    kind: RepositoryConnectorKind,
    context: &DriverContext,
    auth: &AuthContext,
    query: &RepositoryQuery,
    limit: u64,
) -> Result<Value, TransferError> {
    let operation = SearchRecordsOperation::new(
        kind,
        NodeScope::of(context),
        auth.clone(),
        query.clone(),
        limit,
    );
    drive(operation, context).await
}

pub(super) async fn resolve(
    kind: RepositoryConnectorKind,
    context: &DriverContext,
    auth: &AuthContext,
    group_id: Ulid,
    connector_id: Ulid,
    reference: &RecordReference,
    limit: u64,
) -> Result<String, TransferError> {
    let operation = ResolveRecordOperation::new(
        kind,
        NodeScope::of(context),
        auth.clone(),
        group_id,
        connector_id,
        reference.clone(),
        limit,
    );
    drive(operation, context).await
}
