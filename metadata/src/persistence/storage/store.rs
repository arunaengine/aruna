use crate::error::ArunaError;
use std::borrow::Cow;

// pub trait Store: Sync + Send + Sized
// {
//     type StoreConfig;
//     type ReadTxn<'a>: Txn<'a>;
//     type WriteTxn<'a>: Txn<'a>;
//     fn new(config: Self::StoreConfig) -> Result<Self, ArunaError>;
//     fn create_resource<'a>(&self, resource: &Resource, txn: &mut Self::WriteTxn<'a>) -> Result<u32, ArunaError>;
//     fn get_resource<'a>(&self, id: &Ulid, txn: &Self::ReadTxn<'a>) -> Result<Option<Resource>, ArunaError>;
//     fn create_user<'a>(&self, user: &User, txn: &mut Self::WriteTxn<'a>) -> Result<(), ArunaError>;
//     fn get_user<'a>(&self, id: &Ulid, txn: &Self::ReadTxn<'a>) -> Result<Option<User>, ArunaError>;
//     fn add_user_universe<'a>(&self, user_id: &Ulid, resource_idx: u32, txn: &mut Self::WriteTxn<'a>) -> Result<(), ArunaError>;
//     fn get_user_universe<'a>(&self, user_id: &Ulid, txn: &Self::ReadTxn<'a>) -> Result<Option<RoaringBitmap>, ArunaError>;
//     fn get_public_universe<'a>(&self, txn: &Self::ReadTxn<'a>) -> Result<RoaringBitmap, ArunaError>;
//     fn get_read_txn<'a, 'b>(&'a self) -> Result<Self::ReadTxn<'b>, ArunaError> where 'a: 'b;
//     fn get_write_txn<'a, 'b>(&'a self) -> Result<Self::WriteTxn<'b>, ArunaError> where 'a: 'b;
// }

pub mod tables {
    pub const RESOURCE_DB_NAME: &str = "resources";
    pub const RESOURCE_MAPPINGS_DB_NAME: &str = "resource_mappings";
    pub const USER_DB_NAME: &str = "users";
    pub const USER_MAPPINGS_DB_NAME: &str = "user_mappings";
    pub const PUBLIC_MAPPINGS_DB_NAME: &str = "public_mappings";
}

pub trait Store<'a>: Sync + Send + Sized {
    type StoreConfig;
    type Txn;
    fn new(config: Self::StoreConfig) -> Result<Self, ArunaError>;
    fn create_txn(&'a self, write: bool) -> Result<Self::Txn, ArunaError>;
    fn put(
        &'a self,
        txn: &mut Self::Txn,
        dbname: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), ArunaError>;
    fn get<'b>(
        &'a self,
        txn: &'b Self::Txn,
        dbname: &'b str,
        key: &'b [u8],
    ) -> Result<Option<Cow<'b, [u8]>>, ArunaError>
    where
        'a: 'b;
    fn commit(&self, txn: Self::Txn) -> Result<(), ArunaError>;
    fn purge(&self) -> Result<(), ArunaError>;
}

// pub trait Store: Sync + Send + Sized {
//     type StoreConfig;
//     type Txn;
//     fn new(config: Self::StoreConfig) -> Result<Self, ArunaError>;
//     fn create_txn(&self, write: bool) -> Result<Self::Txn, ArunaError>;
//     fn put(
//         &self,
//         txn: &mut Self::Txn,
//         dbname: &str,
//         key: &[u8],
//         value: &[u8],
//     ) -> Result<(), ArunaError>;
//     fn get(
//         &self,
//         txn: &Self::Txn,
//         dbname: &str,
//         key: &[u8],
//     ) -> Result<Option<Cow<'_, [u8]>>, ArunaError>;
//     fn commit(&self, txn: Self::Txn) -> Result<(), ArunaError>;
// }
