use crate::auth::request_authorization::AuthorizeError;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::device::edit::DeviceEditError;
use crate::device::edit::accepts_edits;
use crate::device::edit::apply_local_edit;
use crate::driver::DriverContext;
use crate::metadata::api::GetVisibleMetadataDocumentRequest;
use crate::metadata::api::MetadataApiError;
use crate::metadata::api::ensure_record_readable;
use crate::metadata::api::get_visible_document;
use crate::metadata::api::load_live_record;
use crate::metadata::create_document::CreateMetadataDocumentConfig;
use crate::metadata::create_document::CreateMetadataDocumentError;
use crate::metadata::create_document::CreateMetadataDocumentOperation;
use crate::metadata::create_document::CreateMetadataDocumentPayload;
use crate::metadata::create_document::CreateMetadataDocumentResult;
use crate::metadata::create_document::create_metadata_document;
use crate::metadata::create_document::mint_forward_document;
use crate::metadata::create_document::mint_local_document;
use crate::metadata::create_document::resolve_metadata_id;
use crate::metadata::delete_document::DeleteMetadataDocumentOperation;
use crate::metadata::delete_document::delete_metadata_document;
use crate::metadata::profile_validation::current_validation_status;
use crate::metadata::profile_validation::revalidate_current;
use crate::metadata::protocol::MetadataAuthToken;
use crate::metadata::protocol::MetadataReadError;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::metadata::update_document::UpdateMetadataDocumentConfig;
use crate::metadata::update_document::UpdateMetadataDocumentError;
use crate::metadata::update_document::UpdateMetadataDocumentMutation;
use crate::metadata::update_document::UpdateMetadataDocumentOperation;
use crate::metadata::update_document::update_metadata_document;
use crate::notifications::watch::emit::emit_metadata_created;
use crate::placement::process_placements::load_realm_config;
use crate::placement::resolve_shard_holders;
use aruna_core::MetaResourceId;
use aruna_core::NodeId;
use aruna_core::metadata::MetadataBatch;
use aruna_core::metadata::MetadataBatchSource;
use aruna_core::metadata::MetadataError;
use aruna_core::structs::Actor;
use aruna_core::structs::AuthContext;
use aruna_core::structs::MetadataRegistryRecord;
use aruna_core::structs::RealmId;
use aruna_core::structs::SyncRefusal;
use aruna_core::types::GroupId;
use std::sync::Arc;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use super::authorize::authorize_create;
use super::authorize::authorize_forwarded_caller;
use super::authorize::authorize_write;
use super::authorize::forward_auth_error;
use super::pid::pid_authority_node;
use super::read::device_replica;
use super::replay::HeldRecordError;
use super::replay::create_record_matches;
use super::replay::forwarded_create_replay;
use super::replay::held_record;
use super::replay::routed_record_matches;
use super::replay::update_record_matches;
use super::routing::create_forward_holders;
use super::routing::distinct_holders;
use super::routing::holder_intersection;
use super::routing::holds_metadata_id;
use super::routing::is_user_origin;
use super::transport::MetadataWriteError;
use super::transport::RetryDisposition;
use super::transport::forward_to_holders;
use super::transport::forwarded_unavailable;
use super::transport::read_error;
use super::transport::reject;
use super::transport::retry_disposition;
use super::transport::unexpected_response;
use aruna_core::StructuredId;

/// Creates locally when the origin holds the bucket, otherwise at a holder.
/// Definitely unsent requests may try another holder; ambiguous delivery is
/// terminal so the create is not replayed.
pub async fn route_metadata_create(
    operation: CreateMetadataDocumentOperation,
    context: Arc<DriverContext>,
    auth_token: Option<MetadataAuthToken>,
) -> Result<CreateMetadataDocumentResult, MetadataWriteError> {
    let config = operation.config().clone();
    match create_metadata_document(operation, context.clone()).await {
        Err(CreateMetadataDocumentError::OriginHoldsNoBucket) => {}
        Ok(created) => return Ok(created),
        Err(error) => return Err(error.into()),
    }

    // Mint the forwarded id at the origin with the blind-hash bucket of the D8
    // subject, so every candidate holder stamps the same bucket (D3/D4).
    let realm_config = load_realm_config(&context, config.actor.realm_id)
        .await
        .ok_or_else(|| {
            MetadataWriteError::Undeliverable("realm placement config is unavailable".to_string())
        })?;
    let document_id = if config.document_id.is_nil() {
        mint_forward_document(
            &realm_config,
            &config.actor,
            config.group_id,
            &config.document_path,
        )?
    } else {
        MetaResourceId::from_bytes(config.document_id.to_bytes()).map_err(|error| {
            CreateMetadataDocumentError::PlacementBindingUnavailable(format!(
                "forwarded document id is not a structured id: {error}"
            ))
        })?
    };
    let (placement, holders) =
        create_forward_holders(&realm_config, &config, document_id.as_ulid()).ok_or_else(|| {
            MetadataWriteError::Undeliverable(
                "document id has no resolvable metadata placement".to_string(),
            )
        })?;
    let config_digest = realm_config
        .digest()
        .map_err(|error| MetadataWriteError::Undeliverable(error.to_string()))?;
    let response = forward_to_holders(
        &context,
        &holders,
        MetadataTransportMessage::ForwardCreateDocument {
            auth_token,
            config_digest,
            group_id: config.group_id,
            document_id: document_id.as_ulid(),
            document_path: config.document_path.clone(),
            public: config.public,
            payload: config.payload.clone(),
        },
        None,
        false,
    )
    .await?;
    match response {
        MetadataTransportMessage::ForwardedRecord { record }
            if create_record_matches(&config, document_id.as_ulid(), &placement, &record) =>
        {
            Ok(CreateMetadataDocumentResult {
                event_id: record.last_event_id,
                record: *record,
            })
        }
        MetadataTransportMessage::ForwardedRecord { .. } => Err(MetadataWriteError::Undeliverable(
            "holder returned a metadata create record for another document".to_string(),
        )),
        MetadataTransportMessage::ForwardedProfileValidation { findings } => Err(
            CreateMetadataDocumentError::MetadataError(MetadataError::ProfileValidation(findings))
                .into(),
        ),
        other => Err(unexpected_response(other)),
    }
}

/// Errors of the shared authorized create flow. Every variant maps onto the
/// same transport status the former REST-local sequence produced.
#[derive(Debug, Error)]
pub enum CreateMetadataAuthorizedError {
    #[error("metadata document path must not be empty")]
    EmptyPath,
    #[error("metadata create is forbidden")]
    Forbidden,
    #[error(transparent)]
    Api(#[from] MetadataApiError),
    #[error(transparent)]
    Create(#[from] CreateMetadataDocumentError),
    #[error(transparent)]
    Authorize(#[from] AuthorizeError),
    #[error(transparent)]
    Write(#[from] MetadataWriteError),
}

/// Shared metadata create used by REST and MCP: normalize the path, mint the document id, run
/// the non-user-origin permission checks, route the create to a holder and emit the post-commit
/// watch event. The caller owns bearer conversion and the transport status mapping.
#[allow(clippy::too_many_arguments)]
pub async fn create_metadata_authorized(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    local_node_id: NodeId,
    auth: &AuthContext,
    extras: PolicyRequestExtras,
    auth_token: Option<MetadataAuthToken>,
    group_id: GroupId,
    path: String,
    public: bool,
    payload: CreateMetadataDocumentPayload,
) -> Result<MetadataRegistryRecord, CreateMetadataAuthorizedError> {
    let path = MetadataRegistryRecord::normalize_document_path(&path);
    if path.is_empty() {
        return Err(CreateMetadataAuthorizedError::EmptyPath);
    }
    let user_origin = is_user_origin(context, realm_id, local_node_id).await?;
    let realm_config = load_realm_config(context, realm_id)
        .await
        .ok_or(MetadataApiError::ServiceUnavailable)?;
    let actor = Actor {
        node_id: local_node_id,
        user_id: auth.user_id,
        realm_id,
    };
    let document_id = if user_origin {
        mint_forward_document(&realm_config, &actor, group_id, &path)?.as_ulid()
    } else {
        match mint_local_document(&realm_config, &actor, group_id, &path) {
            Ok(document_id) => document_id.as_ulid(),
            Err(CreateMetadataDocumentError::OriginHoldsNoBucket) => {
                mint_forward_document(&realm_config, &actor, group_id, &path)?.as_ulid()
            }
            Err(error) => return Err(error.into()),
        }
    };
    if !user_origin {
        if auth.realm_id != realm_id {
            return Err(CreateMetadataAuthorizedError::Forbidden);
        }
        authorize_create(
            context,
            realm_id,
            auth,
            extras,
            group_id,
            &path,
            document_id,
        )
        .await?;
    }
    let created = route_metadata_create(
        CreateMetadataDocumentOperation::new_generated_id(CreateMetadataDocumentConfig {
            actor,
            group_id,
            document_id,
            document_path: path,
            public,
            payload,
        }),
        context.clone(),
        auth_token,
    )
    .await?;
    let event_id = created.event_id;
    let record = created.record;

    // Post-commit, best-effort resource-watch emission. A failed emission only
    // warns and never affects the already-successful create.
    emit_metadata_created(
        context,
        realm_id,
        auth.user_id,
        record.group_id,
        record.document_id,
        &record.document_path,
        event_id,
    )
    .await;

    Ok(record)
}

pub async fn route_metadata_update(
    context: &Arc<DriverContext>,
    actor: Actor,
    record: Option<&MetadataRegistryRecord>,
    document_id: Ulid,
    public: Option<bool>,
    mutation: UpdateMetadataDocumentMutation,
    auth_token: Option<MetadataAuthToken>,
) -> Result<MetadataRegistryRecord, MetadataWriteError> {
    let config = load_realm_config(context, actor.realm_id)
        .await
        .ok_or_else(|| {
            MetadataWriteError::Undeliverable("realm placement config is unavailable".to_string())
        })?;
    // On a device a selected document is edited locally and queued; a holder
    // sees the change set when the intake drain forwards it.
    if let Some(replica) = device_replica(context, &config, Some(actor.node_id), document_id).await
        && accepts_edits(&replica)
    {
        return apply_local_edit(context, actor.user_id, actor.node_id, &replica, mutation)
            .await
            .map_err(device_edit_error);
    }
    let placement = resolve_metadata_id(
        &config,
        actor.realm_id,
        record.map(|record| record.group_id),
        document_id,
    )
    .map_err(|error| MetadataWriteError::Undeliverable(error.to_string()))?;
    let config_digest = config
        .digest()
        .map_err(|error| MetadataWriteError::Undeliverable(error.to_string()))?;
    if record.is_some_and(|record| {
        !routed_record_matches(&config, actor.realm_id, document_id, &placement, record)
    }) {
        return Err(MetadataWriteError::Undeliverable(
            "local metadata registry record does not match the routed document".to_string(),
        ));
    }
    let local_node_id = actor.node_id;
    let current_holders = resolve_shard_holders(&config, &placement);
    let holders = record.map_or_else(
        || current_holders.clone(),
        |record| holder_intersection(&current_holders, &record.holder_node_ids),
    );
    if record.is_some() && holders.is_empty() {
        return Err(MetadataWriteError::Undeliverable(
            "metadata document has no active frozen holder with history capacity".to_string(),
        ));
    }
    let local_holds = holders.contains(&local_node_id);
    let mut local_capacity = false;
    if local_holds && let Some(record) = record {
        match update_metadata_document(
            UpdateMetadataDocumentOperation::new(UpdateMetadataDocumentConfig {
                actor: actor.clone(),
                group_id: record.group_id,
                document_id,
                public: public.unwrap_or(record.public),
                mutation: mutation.clone(),
            }),
            context.as_ref(),
        )
        .await
        {
            Ok(record) => return Ok(record),
            Err(UpdateMetadataDocumentError::RawLimit) => local_capacity = true,
            Err(error) => return Err(error.into()),
        }
    }
    let response = forward_to_holders(
        context,
        &holders,
        MetadataTransportMessage::ForwardUpdateDocument {
            auth_token,
            config_digest,
            document_id,
            public,
            mutation,
        },
        local_holds.then_some(local_node_id),
        local_capacity,
    )
    .await?;
    match response {
        MetadataTransportMessage::ForwardedRecord {
            record: response_record,
        } if routed_record_matches(
            &config,
            actor.realm_id,
            document_id,
            &placement,
            &response_record,
        ) && record.is_none_or(|record| update_record_matches(record, &response_record))
            && public.is_none_or(|public| response_record.public == public) =>
        {
            Ok(*response_record)
        }
        MetadataTransportMessage::ForwardedRecord { .. } => Err(MetadataWriteError::Undeliverable(
            "holder returned a metadata update record for another document".to_string(),
        )),
        MetadataTransportMessage::ForwardedUpdateInvalidInput { message } => Err(
            UpdateMetadataDocumentError::MetadataError(MetadataError::InvalidInput(message)).into(),
        ),
        MetadataTransportMessage::ForwardedProfileValidation { findings } => Err(
            UpdateMetadataDocumentError::MetadataError(MetadataError::ProfileValidation(findings))
                .into(),
        ),
        other => Err(unexpected_response(other)),
    }
}

/// Forwards one edit a device made on its replica to a holder.
/// The batch is idempotent by its dot, but an ambiguous delivery stops the
/// attempt: replaying would append a second event for a merge that changes nothing.
pub async fn apply_batch_routed(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    document_id: Ulid,
    batch: Box<MetadataBatch>,
    authored: MetadataBatchSource,
    auth_token: MetadataAuthToken,
) -> Result<MetadataRegistryRecord, MetadataWriteError> {
    let config = load_realm_config(context, realm_id).await.ok_or_else(|| {
        MetadataWriteError::Undeliverable("realm placement config is unavailable".to_string())
    })?;
    let config_digest = config
        .digest()
        .map_err(|error| MetadataWriteError::Undeliverable(error.to_string()))?;
    let placement = resolve_metadata_id(&config, realm_id, None, document_id)
        .map_err(|error| MetadataWriteError::Undeliverable(error.to_string()))?;
    let metadata = context.metadata_handle.as_ref().ok_or_else(|| {
        MetadataWriteError::Undeliverable("no metadata handle to forward with".to_string())
    })?;
    let message = MetadataTransportMessage::ForwardApplyBatch {
        auth_token,
        config_digest,
        document_id,
        batch,
        authored,
    };
    let mut detail = String::from("the document's bucket has no reachable holder");
    for holder in distinct_holders(&resolve_shard_holders(&config, &placement)) {
        match metadata
            .request_forwarded_write(holder, message.clone())
            .await
        {
            Ok(MetadataTransportMessage::ForwardedApplyBatch { result: Ok(record) }) => {
                return Ok(*record);
            }
            Ok(MetadataTransportMessage::ForwardedApplyBatch {
                result: Err(SyncRefusal::Unavailable),
            }) => detail = format!("{holder}: holder could not apply the device edit"),
            Ok(MetadataTransportMessage::ForwardedApplyBatch {
                result: Err(refusal),
            }) => return Err(batch_refusal(refusal)),
            Ok(other) => return Err(unexpected_response(other)),
            Err(error) => {
                warn!(holder = %holder, error = %error, "Failed to forward a device edit to holder");
                if retry_disposition(error.delivery()) == RetryDisposition::Stop {
                    return Err(MetadataWriteError::Undeliverable(format!(
                        "forward to holder `{holder}` may have applied the device edit before failing; refusing to replay it: {error}"
                    )));
                }
                detail = format!("{holder}: {error}");
            }
        }
    }
    Err(MetadataWriteError::Undeliverable(detail))
}

/// An offline edit's verdict, as the metadata routes report it.
pub(super) fn device_edit_error(error: DeviceEditError) -> MetadataWriteError {
    match error {
        DeviceEditError::Invalid(message) => {
            UpdateMetadataDocumentError::MetadataError(MetadataError::InvalidInput(message)).into()
        }
        DeviceEditError::NoReplica => MetadataWriteError::NotFound,
        other => MetadataWriteError::Undeliverable(other.to_string()),
    }
}

/// A holder's verdict on a device edit, as the drain classifies it.
pub(super) fn batch_refusal(refusal: SyncRefusal) -> MetadataWriteError {
    match refusal {
        SyncRefusal::Unauthorized => MetadataWriteError::Unauthorized,
        SyncRefusal::Forbidden => MetadataWriteError::Forbidden,
        SyncRefusal::NotFound => MetadataWriteError::NotFound,
        SyncRefusal::Invalid(message) => {
            UpdateMetadataDocumentError::MetadataError(MetadataError::InvalidInput(message)).into()
        }
        SyncRefusal::Unavailable => {
            MetadataWriteError::Undeliverable("no holder could apply the device edit".to_string())
        }
    }
}

pub async fn route_metadata_delete(
    context: &Arc<DriverContext>,
    actor: Actor,
    record: Option<&MetadataRegistryRecord>,
    document_id: Ulid,
    auth_token: Option<MetadataAuthToken>,
) -> Result<(), MetadataWriteError> {
    let config = load_realm_config(context, actor.realm_id)
        .await
        .ok_or_else(|| {
            MetadataWriteError::Undeliverable("realm placement config is unavailable".to_string())
        })?;
    let placement = resolve_metadata_id(
        &config,
        actor.realm_id,
        record.map(|record| record.group_id),
        document_id,
    )
    .map_err(|error| MetadataWriteError::Undeliverable(error.to_string()))?;
    let config_digest = config
        .digest()
        .map_err(|error| MetadataWriteError::Undeliverable(error.to_string()))?;
    if record.is_some_and(|record| {
        !routed_record_matches(&config, actor.realm_id, document_id, &placement, record)
    }) {
        return Err(MetadataWriteError::Undeliverable(
            "local metadata registry record does not match the routed document".to_string(),
        ));
    }
    let local_node_id = actor.node_id;
    let current_holders = resolve_shard_holders(&config, &placement);
    let holders = record.map_or_else(
        || current_holders.clone(),
        |record| holder_intersection(&current_holders, &record.holder_node_ids),
    );
    if record.is_some() && holders.is_empty() {
        return Err(MetadataWriteError::Undeliverable(
            "metadata document has no active frozen holder with history capacity".to_string(),
        ));
    }
    let authority = pid_authority_node(&config, actor.realm_id, document_id).ok_or_else(|| {
        MetadataWriteError::Undeliverable("persistent id authority is unavailable".to_string())
    })?;
    let local_holds = holders.contains(&local_node_id);
    if local_node_id == authority
        && local_holds
        && let Some(record) = record
    {
        delete_metadata_document(
            DeleteMetadataDocumentOperation::new(actor, record.group_id, document_id),
            context.as_ref(),
            document_id,
        )
        .await
        .map_err(MetadataWriteError::from)?;
        return Ok(());
    }
    let response = forward_to_holders(
        context,
        &holders,
        MetadataTransportMessage::ForwardDeleteDocument {
            auth_token,
            config_digest,
            document_id,
        },
        (local_holds && record.is_none()).then_some(local_node_id),
        false,
    )
    .await?;
    match response {
        MetadataTransportMessage::ForwardedDelete => Ok(()),
        other => Err(unexpected_response(other)),
    }
}

/// Applies a write forwarded by a non-holder under the caller's authority.
/// Re-runs the HTTP permission checks (a routing hop, not a trust bypass) and
/// gates on realm membership, not sync-eligibility (user nodes must forward).
pub(crate) async fn apply_forwarded_write(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> MetadataTransportMessage {
    // Debug poll frames reserve stack for every branch at once, so each awaiting
    // branch is boxed to keep only the active one on the stack.
    let Some(net_handle) = context.net_handle.as_ref() else {
        return forwarded_unavailable(&message);
    };
    let realm_id = *net_handle.realm_id();
    let Some(config) = load_realm_config(context, realm_id).await else {
        return forwarded_unavailable(&message);
    };
    let expected_digest = match &message {
        MetadataTransportMessage::ForwardCreateDocument { config_digest, .. }
        | MetadataTransportMessage::ForwardUpdateDocument { config_digest, .. }
        | MetadataTransportMessage::ForwardDeleteDocument { config_digest, .. }
        | MetadataTransportMessage::ForwardReadDocument { config_digest, .. }
        | MetadataTransportMessage::ForwardProfileValidationStatus { config_digest, .. } => {
            *config_digest
        }
        _ => return reject("unexpected forwarded metadata message"),
    };
    if config.digest().ok() != Some(expected_digest) {
        return forwarded_unavailable(&message);
    };

    if let MetadataTransportMessage::ForwardReadDocument {
        auth_token,
        document_id,
        ..
    } = &message
    {
        return Box::pin(async {
            let Some(metadata) = context.metadata_handle.as_ref() else {
                return reject("forwarded metadata read needs a metadata handle");
            };
            let result = match metadata
                .authorize_read_peer(peer, auth_token.clone(), false)
                .await
            {
                Ok(auth)
                    if holds_metadata_id(&config, realm_id, net_handle.node_id(), *document_id) =>
                {
                    get_visible_document(
                        context.as_ref(),
                        realm_id,
                        GetVisibleMetadataDocumentRequest {
                            document_id: *document_id,
                            auth,
                        },
                    )
                    .await
                    .map(Box::new)
                    .map_err(read_error)
                }
                Ok(_) => Err(MetadataReadError::Unavailable),
                Err(error) => Err(error),
            };
            MetadataTransportMessage::ForwardedRead { result }
        })
        .await;
    }

    if let MetadataTransportMessage::ForwardProfileValidationStatus {
        auth_token,
        document_id,
        revalidate,
        ..
    } = &message
    {
        return Box::pin(async {
            let Some(metadata) = context.metadata_handle.as_ref() else {
                return MetadataTransportMessage::ForwardedProfileValidationStatus {
                    result: Err(MetadataReadError::Unavailable),
                };
            };
            let result = match metadata
                .authorize_read_peer(peer, auth_token.clone(), false)
                .await
            {
                Ok(auth)
                    if holds_metadata_id(&config, realm_id, net_handle.node_id(), *document_id) =>
                {
                    let record = match load_live_record(context.as_ref(), *document_id).await {
                        Ok(record) => record,
                        Err(error) => {
                            return MetadataTransportMessage::ForwardedProfileValidationStatus {
                                result: Err(read_error(error)),
                            };
                        }
                    };
                    if let Err(error) = ensure_record_readable(
                        context.as_ref(),
                        realm_id,
                        auth.as_ref(),
                        &record,
                        None,
                    )
                    .await
                    {
                        return MetadataTransportMessage::ForwardedProfileValidationStatus {
                            result: Err(read_error(error)),
                        };
                    }
                    if *revalidate {
                        revalidate_current(context.as_ref(), &record).await
                    } else {
                        current_validation_status(context.as_ref(), &record).await
                    }
                    .map(Box::new)
                    .map_err(|_| MetadataReadError::Unavailable)
                }
                Ok(_) => Err(MetadataReadError::Unavailable),
                Err(error) => Err(error),
            };
            MetadataTransportMessage::ForwardedProfileValidationStatus { result }
        })
        .await;
    }

    let auth = match authorize_forwarded_caller(context, peer, realm_id, &message).await {
        Ok(auth) => auth,
        Err(error) => return forward_auth_error(error),
    };

    match message {
        MetadataTransportMessage::ForwardCreateDocument {
            group_id,
            document_id,
            document_path,
            public,
            payload,
            ..
        } => {
            Box::pin(async {
                let normalized_document_path =
                    MetadataRegistryRecord::normalize_document_path(&document_path);
                if normalized_document_path.is_empty() {
                    return reject("forwarded metadata create has an empty document path");
                }
                let path = MetadataRegistryRecord::permission_path_for(
                    &realm_id,
                    group_id,
                    &normalized_document_path,
                    document_id,
                );
                if let Err(error) = authorize_write(context, auth.clone(), path).await {
                    return forward_auth_error(error);
                }
                let create_config = CreateMetadataDocumentConfig {
                    actor: Actor {
                        node_id: net_handle.node_id(),
                        user_id: auth.user_id,
                        realm_id,
                    },
                    group_id,
                    document_id,
                    document_path,
                    public,
                    payload,
                };
                match forwarded_create_replay(context, &create_config).await {
                    Ok(Some(response)) => return response,
                    Ok(None) => {}
                    Err(error) => return reject(error),
                }
                let operation =
                    CreateMetadataDocumentOperation::new_forwarded(create_config.clone());
                match create_metadata_document(operation, context.clone()).await {
                    Ok(created) => MetadataTransportMessage::ForwardedRecord {
                        record: Box::new(created.record),
                    },
                    // Lost the race against a concurrent delivery of the same
                    // forward: the winner's record is the answer, not an error.
                    Err(CreateMetadataDocumentError::DocumentAlreadyExists) => {
                        match forwarded_create_replay(context, &create_config).await {
                            Ok(Some(response)) => response,
                            Ok(None) => reject(format!(
                                "forwarded metadata create for `{document_id}` raced a delete"
                            )),
                            Err(error) => reject(error),
                        }
                    }
                    Err(CreateMetadataDocumentError::MetadataError(
                        MetadataError::ProfileValidation(findings),
                    )) => MetadataTransportMessage::ForwardedProfileValidation { findings },
                    Err(error) => reject(format!("forwarded metadata create failed: {error}")),
                }
            })
            .await
        }
        MetadataTransportMessage::ForwardUpdateDocument {
            document_id,
            public,
            mutation,
            ..
        } => {
            Box::pin(async {
                let record =
                    match held_record(context, &config, net_handle.node_id(), document_id).await {
                        Ok(record) => record,
                        Err(HeldRecordError::NotFound) => {
                            return MetadataTransportMessage::ForwardedWriteNotFound;
                        }
                        Err(HeldRecordError::Unavailable(error)) => {
                            warn!(%document_id, %error, "Forwarded metadata update is unavailable");
                            return MetadataTransportMessage::ForwardedWriteUnavailable;
                        }
                    };
                if let Err(error) =
                    authorize_write(context, auth.clone(), record.permission_path.clone()).await
                {
                    return forward_auth_error(error);
                }
                let operation =
                    UpdateMetadataDocumentOperation::new(UpdateMetadataDocumentConfig {
                        actor: Actor {
                            node_id: net_handle.node_id(),
                            user_id: auth.user_id,
                            realm_id,
                        },
                        group_id: record.group_id,
                        document_id,
                        public: public.unwrap_or(record.public),
                        mutation,
                    });
                match update_metadata_document(operation, context.as_ref()).await {
                    Ok(record) => MetadataTransportMessage::ForwardedRecord {
                        record: Box::new(record),
                    },
                    Err(UpdateMetadataDocumentError::RawLimit) => {
                        MetadataTransportMessage::ForwardedMetadataHistoryCapacity
                    }
                    Err(UpdateMetadataDocumentError::MetadataError(
                        MetadataError::InvalidInput(message),
                    )) => MetadataTransportMessage::ForwardedUpdateInvalidInput { message },
                    Err(UpdateMetadataDocumentError::MetadataError(
                        MetadataError::ProfileValidation(findings),
                    )) => MetadataTransportMessage::ForwardedProfileValidation { findings },
                    Err(error) => reject(format!("forwarded metadata update failed: {error}")),
                }
            })
            .await
        }
        MetadataTransportMessage::ForwardDeleteDocument { document_id, .. } => {
            Box::pin(async {
                let record =
                    match held_record(context, &config, net_handle.node_id(), document_id).await {
                        Ok(record) => record,
                        Err(HeldRecordError::NotFound) => {
                            return MetadataTransportMessage::ForwardedWriteNotFound;
                        }
                        Err(HeldRecordError::Unavailable(error)) => {
                            warn!(%document_id, %error, "Forwarded metadata delete is unavailable");
                            return MetadataTransportMessage::ForwardedWriteUnavailable;
                        }
                    };
                if pid_authority_node(&config, realm_id, document_id) != Some(net_handle.node_id())
                {
                    return MetadataTransportMessage::ForwardedWriteUnavailable;
                }
                if let Err(error) =
                    authorize_write(context, auth.clone(), record.permission_path.clone()).await
                {
                    return forward_auth_error(error);
                }
                let operation = DeleteMetadataDocumentOperation::new(
                    Actor {
                        node_id: net_handle.node_id(),
                        user_id: auth.user_id,
                        realm_id,
                    },
                    record.group_id,
                    document_id,
                );
                match delete_metadata_document(operation, context.as_ref(), document_id).await {
                    Ok(()) => MetadataTransportMessage::ForwardedDelete,
                    Err(error) => reject(format!("forwarded metadata delete failed: {error}")),
                }
            })
            .await
        }
        other => reject(format!(
            "unexpected forwarded metadata message: {}",
            crate::metadata::handle::transport_message_kind(&other)
        )),
    }
}
