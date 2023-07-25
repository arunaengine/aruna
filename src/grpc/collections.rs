use crate::middlelayer::create_request_types::CreateRequest;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::delete_request_types::DeleteRequest;
use crate::middlelayer::update_request_types::{
    DataClassUpdate, DescriptionUpdate, KeyValueUpdate, NameUpdate,
};
use crate::utils::conversions::get_token_from_md;
use crate::utils::grpc_utils::IntoGenericInner;
use aruna_cache::notifications::NotificationCache;
use aruna_policy::ape::policy_evaluator::PolicyEvaluator;
use aruna_policy::ape::structs::{
    ApeResourcePermission, Context, PermissionLevels, ResourceContext,
};
use aruna_rust_api::api::storage::models::v2::generic_resource;
use aruna_rust_api::api::storage::services::v2::collection_service_server::CollectionService;
use aruna_rust_api::api::storage::services::v2::{
    CreateCollectionRequest, CreateCollectionResponse, DeleteCollectionRequest,
    DeleteCollectionResponse, GetCollectionRequest, GetCollectionResponse, GetCollectionsRequest,
    GetCollectionsResponse, SnapshotCollectionRequest, SnapshotCollectionResponse,
    UpdateCollectionDataClassRequest, UpdateCollectionDataClassResponse,
    UpdateCollectionDescriptionRequest, UpdateCollectionDescriptionResponse,
    UpdateCollectionKeyValuesRequest, UpdateCollectionKeyValuesResponse,
    UpdateCollectionNameRequest, UpdateCollectionNameResponse,
};
use diesel_ulid::DieselUlid;
use std::str::FromStr;
use std::sync::Arc;
use tonic::{Request, Response, Result};

crate::impl_grpc_server!(CollectionServiceImpl);

#[tonic::async_trait]
impl CollectionService for CollectionServiceImpl {
    async fn create_collection(
        &self,
        request: Request<CreateCollectionRequest>,
    ) -> Result<Response<CreateCollectionResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let request = CreateRequest::Collection(request.into_inner());

        let parent_ctx = tonic_invalid!(
            request
                .get_parent()
                .ok_or(tonic::Status::invalid_argument("Parent missing."))?
                .get_context(),
            "invalid parent"
        );

        let user_id = tonic_auth!(
            self.authorizer.check_context(&token, parent_ctx).await,
            "Unauthorized"
        )
        .ok_or(tonic::Status::invalid_argument("Missing user id"))?;

        let (generic_collection, shared_id, cache_res) = tonic_internal!(
            self.database_handler
                .create_resource(request, user_id)
                .await,
            "Internal database error"
        );

        tonic_internal!(
            self.cache.cache.process_api_resource_update(
                generic_collection.clone(),
                shared_id,
                cache_res,
            ),
            "Caching error"
        );

        let response = CreateCollectionResponse {
            collection: Some(generic_collection.into_inner()?),
        };

        return_with_log!(response);
    }

    async fn get_collection(
        &self,
        request: Request<GetCollectionRequest>,
    ) -> Result<Response<GetCollectionResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let request = request.into_inner();

        let collection_id = tonic_invalid!(
            DieselUlid::from_str(&request.collection_id),
            "ULID conversion error"
        );

        let ctx = Context::res_col(collection_id, PermissionLevels::READ, true);

        tonic_auth!(
            self.authorizer.check_context(&token, ctx).await,
            "Unauthorized"
        );

        let res = self
            .cache
            .get_resource(&aruna_cache::structs::Resource::Collection(collection_id))
            .ok_or_else(|| tonic::Status::not_found("Collection not found"))?;

        let response = GetCollectionResponse {
            collection: Some(res.into_inner()?),
        };

        return_with_log!(response);
    }

    async fn get_collections(
        &self,
        _request: Request<GetCollectionsRequest>,
    ) -> Result<Response<GetCollectionsResponse>> {
        todo!()
    }

    async fn delete_collection(
        &self,
        request: Request<DeleteCollectionRequest>,
    ) -> Result<Response<DeleteCollectionResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = DeleteRequest::Collection(request.into_inner());
        let id = tonic_invalid!(request.get_id(), "Invalid collection id.");

        let ctx = Context::res_col(id, PermissionLevels::WRITE, true);

        tonic_auth!(
            self.authorizer.check_context(&token, ctx).await,
            "Unauthorized."
        );

        let updates: Vec<(
            generic_resource::Resource,
            DieselUlid,
            aruna_cache::structs::Resource,
        )> = tonic_internal!(
            self.database_handler.delete_resource(request).await,
            "Internal database error"
        );

        for u in updates {
            tonic_internal!(
                self.cache.cache.process_api_resource_update(u.0, u.1, u.2),
                "Caching error"
            );
        }

        let response = DeleteCollectionResponse {};

        return_with_log!(response);
    }

    async fn update_collection_name(
        &self,
        request: Request<UpdateCollectionNameRequest>,
    ) -> Result<Response<UpdateCollectionNameResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = NameUpdate::Collection(request.into_inner());
        let collection_id = tonic_invalid!(request.get_id(), "Invalid collection id.");
        let ctx = Context::ResourceContext(ResourceContext::Collection(ApeResourcePermission {
            id: collection_id,
            level: PermissionLevels::WRITE, // append?
            allow_sa: true,
        }));

        let _user_id = tonic_auth!(
            self.authorizer.check_context(&token, ctx).await,
            "Unauthorized."
        )
        .ok_or(tonic::Status::invalid_argument("User id missing."))?;

        let collection = match tonic_internal!(
            self.database_handler.update_name(request).await,
            "Internal database error."
        ) {
            generic_resource::Resource::Collection(c) => Some(c),
            _ => return Err(tonic::Status::unknown("This should not happen.")),
        };

        Ok(tonic::Response::new(UpdateCollectionNameResponse {
            collection,
        }))
    }

    async fn update_collection_description(
        &self,
        request: Request<UpdateCollectionDescriptionRequest>,
    ) -> Result<Response<UpdateCollectionDescriptionResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = DescriptionUpdate::Collection(request.into_inner());
        let collection_id = tonic_invalid!(request.get_id(), "Invalid collection id.");
        let ctx = Context::ResourceContext(ResourceContext::Collection(ApeResourcePermission {
            id: collection_id,
            level: PermissionLevels::WRITE, // append?
            allow_sa: true,
        }));

        let _user_id = tonic_auth!(
            self.authorizer.check_context(&token, ctx).await,
            "Unauthorized."
        )
        .ok_or(tonic::Status::invalid_argument("User id missing."))?;

        let collection = match tonic_internal!(
            self.database_handler.update_description(request).await,
            "Internal database error."
        ) {
            generic_resource::Resource::Collection(c) => Some(c),
            _ => return Err(tonic::Status::unknown("This should not happen.")),
        };

        Ok(tonic::Response::new(UpdateCollectionDescriptionResponse {
            collection,
        }))
    }

    async fn update_collection_key_values(
        &self,
        request: Request<UpdateCollectionKeyValuesRequest>,
    ) -> Result<Response<UpdateCollectionKeyValuesResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = KeyValueUpdate::Collection(request.into_inner());
        let collection_id = tonic_invalid!(request.get_id(), "Invalid collection id.");
        let ctx = Context::ResourceContext(ResourceContext::Collection(ApeResourcePermission {
            id: collection_id,
            level: PermissionLevels::WRITE, // append?
            allow_sa: true,
        }));

        let _user_id = tonic_auth!(
            self.authorizer.check_context(&token, ctx).await,
            "Unauthorized."
        )
        .ok_or(tonic::Status::invalid_argument("User id missing."))?;

        let collection = match tonic_internal!(
            self.database_handler.update_keyvals(request).await,
            "Internal database error."
        ) {
            generic_resource::Resource::Collection(c) => Some(c),
            _ => return Err(tonic::Status::unknown("This should not happen.")),
        };

        Ok(tonic::Response::new(UpdateCollectionKeyValuesResponse {
            collection,
        }))
    }
    async fn update_collection_data_class(
        &self,
        request: Request<UpdateCollectionDataClassRequest>,
    ) -> Result<Response<UpdateCollectionDataClassResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = DataClassUpdate::Collection(request.into_inner());
        let collection_id = tonic_invalid!(request.get_id(), "Invalid collection id.");
        let ctx = Context::ResourceContext(ResourceContext::Collection(ApeResourcePermission {
            id: collection_id,
            level: PermissionLevels::WRITE, // append?
            allow_sa: true,
        }));

        let _user_id = tonic_auth!(
            self.authorizer.check_context(&token, ctx).await,
            "Unauthorized."
        )
        .ok_or(tonic::Status::invalid_argument("User id missing."))?;

        let collection = match tonic_internal!(
            self.database_handler.update_dataclass(request).await,
            "Internal database error."
        ) {
            generic_resource::Resource::Collection(c) => Some(c),
            _ => return Err(tonic::Status::unknown("This should not happen.")),
        };

        Ok(tonic::Response::new(UpdateCollectionDataClassResponse {
            collection,
        }))
    }
    async fn snapshot_collection(
        &self,
        _request: Request<SnapshotCollectionRequest>,
    ) -> Result<Response<SnapshotCollectionResponse>> {
        todo!()
    }
}