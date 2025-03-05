use crate::api::grpc::grpc_helpers::get_token;
use crate::models::requests::{
    AddGroupRequest, CreateRealmRequest, GetRealmRequest,
};
use crate::transactions::controller::Controller;
use aruna_rust_api::v3::aruna::api::v3::realm_service_server::RealmService;
use aruna_rust_api::v3::aruna::api::v3::{self as grpc};
use std::sync::Arc;

pub struct RealmServiceImpl {
    pub handler: Arc<Controller>,
}
impl RealmServiceImpl {
    pub fn new(handler: Arc<Controller>) -> Self {
        Self { handler }
    }
}

#[tonic::async_trait]
impl RealmService for RealmServiceImpl {
    async fn create_realm(
        &self,
        request: tonic::Request<grpc::CreateRealmRequest>,
    ) -> Result<tonic::Response<grpc::CreateRealmResponse>, tonic::Status> {
        let token = get_token(request.metadata());
        let controller = self.handler.clone();

        Ok(tonic::Response::new(
            controller
                .request(CreateRealmRequest::from(request.into_inner()), token)
                .await?
                .into(),
        ))
    }

    async fn get_realm(
        &self,
        request: tonic::Request<grpc::GetRealmRequest>,
    ) -> Result<tonic::Response<grpc::GetRealmResponse>, tonic::Status> {
        let token = get_token(request.metadata());
        let controller = self.handler.clone();

        Ok(tonic::Response::new(
            controller
                .request(GetRealmRequest::try_from(request.into_inner())?, token)
                .await?
                .into(),
        ))
    }
    async fn add_group(
        &self,
        request: tonic::Request<grpc::AddGroupRequest>,
    ) -> Result<tonic::Response<grpc::AddGroupResponse>, tonic::Status> {
        let token = get_token(request.metadata());
        let controller = self.handler.clone();

        Ok(tonic::Response::new(
            controller
                .request(AddGroupRequest::try_from(request.into_inner())?, token)
                .await?
                .into(),
        ))
    }

    async fn remove_group(
        &self,
        _request: tonic::Request<grpc::RemoveGroupRequest>,
    ) -> Result<tonic::Response<grpc::RemoveGroupResponse>, tonic::Status> {
        todo!()
    }

    async fn get_groups_from_realm(
        &self,
        request: tonic::Request<grpc::GetGroupsFromRealmRequest>, // TODO: Change to GetGroupsFromRealmRequest
    ) -> std::result::Result<tonic::Response<grpc::GetGroupsFromRealmResponse>, tonic::Status> {
        todo!()
        //let token = get_token(request.metadata());
        //let controller = self.handler.clone();

        //Ok(tonic::Response::new(
        //    controller
        //        .request(GetGroupsFromRealmRequest::try_from(request.into_inner())?, token)
        //        .await?
        //        .into(),
        //))
    }
}
