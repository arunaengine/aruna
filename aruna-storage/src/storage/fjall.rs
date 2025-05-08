use crate::error::ArunaStorageError;
use std::{borrow::Cow, collections::HashMap};

use super::store::Store;
use fjall::{
    Config, PartitionCreateOptions, ReadTransaction, TransactionalKeyspace,
    TransactionalPartitionHandle, WriteTransaction,
};

pub struct FjallStore {
    keyspace: TransactionalKeyspace,
    //tables: FjallTables,
    tables: HashMap<&'static str, TransactionalPartitionHandle>,
}

impl std::fmt::Debug for FjallStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FjallStore").finish()
    }
}

pub struct FjallConfig {
    pub path: String,
    pub databases: Vec<&'static str>,
}
pub enum FjallTxn<'a> {
    Read(ReadTransaction),
    Write(WriteTransaction<'a>),
}

impl<'a> Store<'a> for FjallStore {
    type StoreConfig = FjallConfig;

    type Txn = FjallTxn<'a>;
    // type Txn = ();

    fn new(config: Self::StoreConfig) -> Result<Self, crate::error::ArunaStorageError> {
        let path = format!("{}/path", config.path);
        let keyspace = Config::new(path)
            .fsync_ms(Some(500))
            .cache_size(1024 * 1024 * 1024)
            .max_open_files(10)
            .open_transactional()?; // or open_transactional for transactional semantics

        let write_txn = keyspace.write_tx();
        let mut handles = HashMap::new();

        for db in config.databases {
            let handle = keyspace.open_partition(db, PartitionCreateOptions::default())?;
            handles.insert(db, handle);
        }

        write_txn.commit()?;

        Ok(Self {
            keyspace,
            tables: handles,
        })
    }

    fn create_txn(&'a self, write: bool) -> Result<Self::Txn, crate::error::ArunaStorageError> {
        if write {
            Ok(FjallTxn::Write(self.keyspace.write_tx()))
        } else {
            Ok(FjallTxn::Read(self.keyspace.read_tx()))
        }
    }

    fn put(
        &'a self,
        txn: &mut Self::Txn,
        dbname: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), crate::error::ArunaStorageError> {
        let wtxn = match txn {
            FjallTxn::Read(_) => {
                return Err(ArunaStorageError::DatabaseError(
                    "Only write txns permitted".to_string(),
                ));
            }
            FjallTxn::Write(w) => w,
        };
        match self.tables.get(dbname) {
            Some(handle) => {
                wtxn.insert(&handle, key, value);
            }
            None => {
                return Err(ArunaStorageError::DatabaseError(
                    "Database not found".to_string(),
                ));
            }
        }

        Ok(())
    }

    fn remove(
        &'a self,
        txn: &mut Self::Txn,
        dbname: &str,
        key: &[u8],
    ) -> Result<(), crate::error::ArunaStorageError> {
        let wtxn = match txn {
            FjallTxn::Read(_) => {
                return Err(ArunaStorageError::DatabaseError(
                    "Only write txns permitted".to_string(),
                ));
            }
            FjallTxn::Write(w) => w,
        };
        match self.tables.get(dbname) {
            Some(handle) => {
                wtxn.remove(handle, key);
            }
            None => {
                return Err(ArunaStorageError::DatabaseError(
                    "Database not found".to_string(),
                ));
            }
        }

        Ok(())
    }

    fn get<'b>(
        &'a self,
        txn: &'b Self::Txn,
        dbname: &'b str,
        key: &'b [u8],
    ) -> Result<Option<std::borrow::Cow<'b, [u8]>>, crate::error::ArunaStorageError>
    where
        'a: 'b,
    {
        match txn {
            FjallTxn::Read(r) => {
                match self.tables.get(dbname) {
                    Some(handle) => {
                        Ok(r.get(&handle, key)?.map(|v| {
                            // TODO: This sucks, needs improvement
                            let value = v.as_ref().to_vec();
                            Cow::from(value).to_owned()
                        }))
                    }
                    None => {
                        return Err(ArunaStorageError::DatabaseError(
                            "Database not found".to_string(),
                        ));
                    }
                }
            }
            FjallTxn::Write(w) => {
                match self.tables.get(dbname) {
                    Some(handle) => {
                        Ok(w.get(&handle, key)?.map(|v| {
                            // TODO: This sucks, needs improvement
                            let value = v.as_ref().to_vec();
                            Cow::from(value).to_owned()
                        }))
                    }
                    None => {
                        return Err(ArunaStorageError::DatabaseError(
                            "Database not found".to_string(),
                        ));
                    }
                }
            }
        }
    }

    fn commit(&self, txn: Self::Txn) -> Result<(), crate::error::ArunaStorageError> {
        match txn {
            FjallTxn::Read(r) => {
                drop(r);
                Ok(())
            }
            FjallTxn::Write(w) => Ok(w.commit()?),
        }
    }

    #[tracing::instrument(level = "trace", skip(self, txn))]
    fn iter_db<'b>(
        &'a self,
        txn: &'b Self::Txn,
        dbname: &'static str,
    ) -> Result<Box<dyn Iterator<Item = (Cow<'b, [u8]>, Cow<'b, [u8]>)> + 'b>, ArunaStorageError>
    where
        'a: 'b,
    {
        match txn {
            FjallTxn::Read(r) => match self.tables.get(dbname) {
                Some(handle) => Ok(Box::new(r.iter(&handle).filter_map(
                    |slices| -> Option<(Cow<'b, [u8]>, Cow<'b, [u8]>)> {
                        let (key, value) = slices.ok()?;
                        let key = Cow::from(key.as_ref().to_vec()).to_owned();
                        let value = Cow::from(value.as_ref().to_vec()).to_owned();
                        Some((key, value))
                    },
                ))),
                None => {
                    return Err(ArunaStorageError::DatabaseError(
                        "Database not found".to_string(),
                    ));
                }
            },
            FjallTxn::Write(w) => match self.tables.get(dbname) {
                Some(handle) => Ok(Box::new(w.iter(&handle).filter_map(
                    |slices| -> Option<(Cow<'b, [u8]>, Cow<'b, [u8]>)> {
                        let (key, value) = slices.ok()?;
                        let key = Cow::from(key.as_ref().to_vec()).to_owned();
                        let value = Cow::from(value.as_ref().to_vec()).to_owned();
                        Some((key, value))
                    },
                ))),
                None => {
                    return Err(ArunaStorageError::DatabaseError(
                        "Database not found".to_string(),
                    ));
                }
            },
        }
    }
}
