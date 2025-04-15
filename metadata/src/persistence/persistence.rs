use super::{search::search::Search, storage::store::Store};
use crate::{
    error::ArunaError,
    models::models::{Resource, User},
};
use automerge::AutoCommit;
use autosurgeon::Doc;
use ulid::Ulid;

#[async_trait::async_trait]
pub trait Persistor<St, Se>: Sized + Send + Sync + Authorize
where
    for<'a> St: Store<'a>,
    Se: Search,
{
    type Context;
    async fn new(ctx: Self::Context) -> Result<Self, ArunaError>;
    async fn add_resource(&self, user_id: &Ulid, resource: Resource) -> Result<Vec<u8>, ArunaError>;
    async fn get_resources(&self, id: Vec<Ulid>) -> Result<Vec<Resource>, ArunaError>;
    async fn update_resource(&self, user_id: &Ulid, resource: Resource) -> Result<Vec<u8>, ArunaError>;
    async fn add_user(&self, user: User) -> Result<Vec<u8>, ArunaError>;
    async fn get_user(&self, id: &Ulid) -> Result<Option<User>, ArunaError>;
    async fn search(&self, user: Option<Ulid>, query: String) -> Result<Vec<String>, ArunaError>;
    // TODO: Remove clear
    async fn clear(&self) -> Result<(), ArunaError>;
}

pub trait Authorize: Send + Sync {
    fn authorize(&self, user_id: &Ulid, resource_id: &Ulid) -> bool;
}
