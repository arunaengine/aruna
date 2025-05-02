use crate::error::ArunaStorageError;
use std::borrow::Cow;

// pub mod tables {
//     pub const RESOURCE_DB_NAME: &str = "resources";
//     pub const RESOURCE_MAPPINGS_DB_NAME: &str = "resource_mappings";
//     pub const USER_DB_NAME: &str = "users";
//     pub const USER_MAPPINGS_DB_NAME: &str = "user_mappings";
//     pub const PUBLIC_MAPPINGS_DB_NAME: &str = "public_mappings";
// }

pub trait Store<'a>: Sync + Send + Sized + std::fmt::Debug {
    type StoreConfig: Send;
    type Txn;
    fn new(config: Self::StoreConfig) -> Result<Self, ArunaStorageError>;
    fn create_txn(&'a self, write: bool) -> Result<Self::Txn, ArunaStorageError>;
    fn put(
        &'a self,
        txn: &mut Self::Txn,
        dbname: &'static str,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), ArunaStorageError>;

    fn remove(
        &'a self,
        txn: &mut Self::Txn,
        dbname: &'static str,
        key: &[u8],
    ) -> Result<(), ArunaStorageError>;
    fn get<'b>(
        &'a self,
        txn: &'b Self::Txn,
        dbname: &'static str,
        key: &'b [u8],
    ) -> Result<Option<Cow<'b, [u8]>>, ArunaStorageError>
    where
        'a: 'b;
    fn commit(&self, txn: Self::Txn) -> Result<(), ArunaStorageError>;

    fn iter_db<'b>(
        &'a self,
        txn: &'b Self::Txn,
        dbname: &'static str,
    ) -> Result<Box<dyn Iterator<Item = (Cow<'b, [u8]>, Cow<'b, [u8]>)> + 'b>, ArunaStorageError>
    where
        'a: 'b;
}
