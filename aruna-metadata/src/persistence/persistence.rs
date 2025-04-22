use super::{search::search::Search, storage::store::Store};
use crate::{
    error::ArunaError,
    models::models::{Resource, User},
    network::network_trait::MetadataMessage,
};
use aruna_net::ProtocolHandler;
use ulid::Ulid;

#[async_trait::async_trait]
pub trait Persistor<St, Se>: Sized + Send + Sync + Authorize + ProtocolHandler
where
    for<'a> St: Store<'a>,
    Se: Search,
{
    type Context;
    async fn new(ctx: Self::Context) -> Result<Self, ArunaError>;
    async fn add_resource(
        &self,
        actor_id: &[u8; 32],
        user_id: &Ulid,
        resource: Resource,
    ) -> Result<Vec<u8>, ArunaError>;
    async fn get_resources(&self, id: Vec<Ulid>) -> Result<Vec<Resource>, ArunaError>;
    async fn update_resource(
        &self,
        actor_id: &[u8; 32],
        user_id: &Ulid,
        resource: Resource,
    ) -> Result<Vec<u8>, ArunaError>;
    async fn add_user(&self,
        actor_id: &[u8; 32],
        user: User) -> Result<Vec<u8>, ArunaError>;
    async fn get_user(&self, id: &Ulid) -> Result<Option<User>, ArunaError>;
    async fn search(&self, user: Option<Ulid>, query: String) -> Result<Vec<String>, ArunaError>;
    // TODO: Remove clear
    async fn clear(&self) -> Result<(), ArunaError>;
    async fn handle_message(&self, msg: MetadataMessage) -> Result<MetadataMessage, ArunaError>;
}

pub trait Authorize: Send + Sync {
    fn authorize(&self, user_id: &Ulid, resource_id: &Ulid) -> bool;
}
