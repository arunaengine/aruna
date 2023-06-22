use crate::{database::connection::Database, error::ArunaError};
use aruna_rust_api::api::bundler::services::v1::{CreateBundleRequest, CreateBundleResponse};

impl Database {
    pub fn create_bundle(
        &self,
        request: CreateBundleRequest,
    ) -> Result<CreateBundleResponse, ArunaError> {
        todo!()
    }

    pub fn delete_bundle(
        &self,
        request: CreateBundleRequest,
    ) -> Result<CreateBundleResponse, ArunaError> {
        todo!()
    }
}
