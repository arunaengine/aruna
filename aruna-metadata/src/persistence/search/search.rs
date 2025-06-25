use crate::{
    error::ArunaMetadataError, models::models::Resource, persistence::authorization::Authorize,
};
use roaring::RoaringBitmap;
use ulid::Ulid;

pub trait Search: Sync + Send + Sized + std::fmt::Debug {
    type SearchConfig: Send;
    fn new(config: Self::SearchConfig) -> Result<Self, ArunaMetadataError>;
    fn search<A: Authorize>(
        &self,
        universe: RoaringBitmap,
        query: String,
    ) -> Result<Vec<Ulid>, ArunaMetadataError>;
    fn add_resource(
        &self,
        idx: u32,
        resource: Resource,
    ) -> impl Future<Output = Result<(), ArunaMetadataError>> + Send;
    fn remove(&self, id: Ulid) -> Result<(), ArunaMetadataError>;
}
