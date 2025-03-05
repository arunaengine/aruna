use crate::transactions::controller::Controller;
use aruna_rust_api::v3::aruna::api::v3 as grpc;
use aruna_rust_api::v3::aruna::api::v3::user_service_server::UserService;
use std::sync::Arc;

pub struct UserServiceImpl {
    pub handler: Arc<Controller>,
}
impl UserServiceImpl {
    pub fn new(handler: Arc<Controller>) -> Self {
        Self { handler }
    }
}

#[tonic::async_trait]
impl UserService for UserServiceImpl {
    async fn register_user(
        &self,
        _request: tonic::Request<grpc::RegisterUserRequest>,
    ) -> Result<tonic::Response<grpc::RegisterUserResponse>, tonic::Status> {
        todo!()
    }
    async fn create_token(
        &self,
        _request: tonic::Request<grpc::CreateTokenRequest>,
    ) -> Result<tonic::Response<grpc::CreateTokenResponse>, tonic::Status> {
        todo!()
    }
    async fn get_user(
        &self,
        _request: tonic::Request<grpc::GetUserRequest>,
    ) -> Result<tonic::Response<grpc::GetUserResponse>, tonic::Status> {
        todo!()
    }
    async fn delete_token(
        &self,
        _request: tonic::Request<grpc::DeleteTokenRequest>,
    ) -> Result<tonic::Response<grpc::DeleteTokenResponse>, tonic::Status> {
        todo!()
    }
    async fn get_tokens(
        &self,
        _request: tonic::Request<grpc::GetTokensRequest>,
    ) -> Result<tonic::Response<grpc::GetTokensResponse>, tonic::Status> {
        todo!()
    }
    async fn get_user_realms(
        &self,
        _request: tonic::Request<grpc::GetUserRealmsRequest>,
    ) -> Result<tonic::Response<grpc::GetUserRealmsResponse>, tonic::Status> {
        todo!()
    }
    async fn get_groups(
        &self,
        _request: tonic::Request<grpc::GetGroupsRequest>,
    ) -> Result<tonic::Response<grpc::GetGroupsResponse>, tonic::Status> {
        todo!()
    }
}
