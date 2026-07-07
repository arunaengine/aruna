use std::sync::Arc;

use aruna_core::errors::AuthorizationError;
use aruna_core::metadata::MetadataError;
use aruna_core::structs::{Actor, AuthContext, MetadataRegistryRecord, Permission};

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentError, CreateMetadataDocumentOperation,
    CreateMetadataDocumentPayload, create_metadata_document as run_create_metadata_document,
};
use crate::delete_metadata_document::{
    DeleteMetadataDocumentError, DeleteMetadataDocumentOperation,
    delete_metadata_document as run_delete_metadata_document,
};
use crate::driver::{DriverContext, drive};
use crate::get_metadata_document::load_metadata_record_by_document;
use crate::routing::protocol::{
    HolderProxyResponse, MetadataCall, MetadataCreatePayload, MetadataMutation, MetadataReply,
    ProxiedReply,
};
use crate::update_metadata_document::{
    UpdateMetadataDocumentConfig, UpdateMetadataDocumentError, UpdateMetadataDocumentMutation,
    UpdateMetadataDocumentOperation, update_metadata_document as run_update_metadata_document,
};

fn ok(reply: MetadataReply) -> HolderProxyResponse {
    HolderProxyResponse::Ok(ProxiedReply::Metadata(Box::new(reply)))
}

/// Drives a routed metadata mutation on this holder. The write scope is
/// re-evaluated here from the validated bearer — never trusted from the origin —
/// and only then does the underlying create/delete/update operation run.
pub(crate) async fn serve_metadata_call(
    context: &DriverContext,
    call: MetadataCall,
    auth: Option<AuthContext>,
) -> HolderProxyResponse {
    let Some(auth) = auth else {
        return HolderProxyResponse::forbidden("metadata mutation requires a bearer token");
    };
    let Some(net_handle) = context.net_handle.as_ref() else {
        return HolderProxyResponse::internal("holder has no net handle");
    };
    let actor = Actor {
        node_id: net_handle.node_id(),
        user_id: auth.user_id,
        realm_id: auth.realm_id,
    };

    match call {
        MetadataCall::Create {
            group_id,
            document_id,
            document_path,
            public,
            payload,
        } => {
            let scope = format!("/{}/g/{group_id}/meta/**", auth.realm_id);
            if let Some(denied) = deny_without_write(context, &auth, scope).await {
                return denied;
            }
            match run_create_metadata_document(
                CreateMetadataDocumentOperation::new_for_generated_document_id(
                    CreateMetadataDocumentConfig {
                        actor,
                        group_id,
                        document_id,
                        document_path,
                        public,
                        payload: create_payload(payload),
                    },
                ),
                Arc::new(context.clone()),
            )
            .await
            {
                Ok(created) => ok(MetadataReply::Record(created.record)),
                Err(CreateMetadataDocumentError::MetadataError(error)) => {
                    reject_metadata_error(&error)
                }
                Err(CreateMetadataDocumentError::DocumentAlreadyExists) => {
                    HolderProxyResponse::conflict("document already exists")
                }
                Err(other) => HolderProxyResponse::internal(other.to_string()),
            }
        }
        MetadataCall::Delete { document_id } => {
            let Some(record) = load_writable_record(context, &auth, document_id).await else {
                return HolderProxyResponse::NotFound;
            };
            let record = match record {
                Ok(record) => record,
                Err(denied) => return denied,
            };
            match run_delete_metadata_document(
                DeleteMetadataDocumentOperation::new(actor, record.group_id, document_id),
                context,
                document_id,
            )
            .await
            {
                Ok(()) => ok(MetadataReply::Ack),
                Err(DeleteMetadataDocumentError::DocumentNotFound) => HolderProxyResponse::NotFound,
                Err(DeleteMetadataDocumentError::MetadataError(error)) => {
                    reject_metadata_error(&error)
                }
                Err(other) => HolderProxyResponse::internal(other.to_string()),
            }
        }
        MetadataCall::Update {
            document_id,
            public,
            mutation,
        } => {
            let Some(record) = load_writable_record(context, &auth, document_id).await else {
                return HolderProxyResponse::NotFound;
            };
            let record = match record {
                Ok(record) => record,
                Err(denied) => return denied,
            };
            match run_update_metadata_document(
                UpdateMetadataDocumentOperation::new(UpdateMetadataDocumentConfig {
                    actor,
                    group_id: record.group_id,
                    document_id,
                    public: public.unwrap_or(record.public),
                    mutation: update_mutation(mutation),
                }),
                context,
            )
            .await
            {
                Ok(record) => ok(MetadataReply::Record(record)),
                Err(UpdateMetadataDocumentError::DocumentNotFound) => HolderProxyResponse::NotFound,
                Err(UpdateMetadataDocumentError::MetadataError(error)) => {
                    reject_metadata_error(&error)
                }
                Err(other) => HolderProxyResponse::internal(other.to_string()),
            }
        }
    }
}

/// Loads the target registry record and re-checks write permission against its
/// permission path. `None` means the record is absent (→ NotFound); `Some(Err)`
/// carries an authorization or storage rejection.
async fn load_writable_record(
    context: &DriverContext,
    auth: &AuthContext,
    document_id: ulid::Ulid,
) -> Option<Result<MetadataRegistryRecord, HolderProxyResponse>> {
    let record = match load_metadata_record_by_document(context, document_id).await {
        Ok(Some(record)) => record,
        Ok(None) => return None,
        Err(error) => {
            return Some(Err(HolderProxyResponse::internal(format!(
                "metadata registry read failed: {error:?}"
            ))));
        }
    };
    if let Some(denied) = deny_without_write(context, auth, record.permission_path.clone()).await {
        return Some(Err(denied));
    }
    Some(Ok(record))
}

/// Returns `Some(rejection)` when the caller lacks WRITE on `path`.
async fn deny_without_write(
    context: &DriverContext,
    auth: &AuthContext,
    path: String,
) -> Option<HolderProxyResponse> {
    match drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: auth.clone(),
            path,
            required_permission: Permission::WRITE,
        }),
        context,
    )
    .await
    {
        Ok(true) => None,
        Ok(false) => Some(HolderProxyResponse::forbidden(
            "caller lacks write permission",
        )),
        Err(
            AuthorizationError::InvalidRealmId
            | AuthorizationError::InvalidGroupId
            | AuthorizationError::GroupNotFound
            | AuthorizationError::AuthDocNotFound,
        ) => Some(HolderProxyResponse::forbidden(
            "caller lacks write permission",
        )),
        Err(other) => Some(HolderProxyResponse::internal(other.to_string())),
    }
}

/// Maps a metadata backend error onto a typed rejection: invalid input is a 400,
/// a missing graph is a retryable 503, and anything else is a holder-side 500.
fn reject_metadata_error(error: &MetadataError) -> HolderProxyResponse {
    match error {
        MetadataError::InvalidInput(reason) => HolderProxyResponse::bad_request(reason.clone()),
        MetadataError::GraphNotFound => {
            HolderProxyResponse::unavailable("metadata graph not found")
        }
        other => HolderProxyResponse::internal(other.to_string()),
    }
}

fn create_payload(payload: MetadataCreatePayload) -> CreateMetadataDocumentPayload {
    match payload {
        MetadataCreatePayload::Scaffold {
            name,
            description,
            date_published,
            license,
        } => CreateMetadataDocumentPayload::Scaffold {
            name,
            description,
            date_published,
            license,
        },
        MetadataCreatePayload::RoCrate { jsonld } => {
            CreateMetadataDocumentPayload::RoCrate { jsonld }
        }
    }
}

fn update_mutation(mutation: MetadataMutation) -> UpdateMetadataDocumentMutation {
    match mutation {
        MetadataMutation::ReplaceRoCrate { jsonld } => {
            UpdateMetadataDocumentMutation::ReplaceRoCrate { jsonld }
        }
        MetadataMutation::UpsertDataEntity { jsonld } => {
            UpdateMetadataDocumentMutation::UpsertDataEntity { jsonld }
        }
        MetadataMutation::UpsertContextualEntity { jsonld } => {
            UpdateMetadataDocumentMutation::UpsertContextualEntity { jsonld }
        }
    }
}
