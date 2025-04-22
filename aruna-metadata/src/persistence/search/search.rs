use crate::{error::ArunaError, models::models::Resource, persistence::persistence::Authorize};
use roaring::RoaringBitmap;
use ulid::Ulid;

pub trait Search: Sync + Send + Sized {
    type SearchConfig;
    fn new(config: Self::SearchConfig) -> Result<Self, ArunaError>;
    fn search<A: Authorize>(
        &self,
        universe: RoaringBitmap,
        query: String,
    ) -> Result<Vec<String>, ArunaError>;
    fn add_resource(
        &self,
        idx: u32,
        resource: Resource,
    ) -> impl Future<Output = Result<(), ArunaError>>;
    #[allow(dead_code)]
    fn remove(&self, id: Ulid) -> Result<(), ArunaError>;
    fn purge(&self) -> impl std::future::Future<Output = Result<(), ArunaError>> + Send;
}
