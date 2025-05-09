use std::{
    borrow::Cow,
    clone,
    collections::HashMap,
    fs,
    sync::{
        Arc,
        mpsc::{Receiver, Sender},
    },
};

use super::store::Store;
use crate::error::ArunaStorageError;
use redb::{Database, ReadTransaction, ReadableTable, TableDefinition, WriteTransaction};

// const RESOURCES: TableDefinition<&[u8], &[u8]> = TableDefinition::new(tables::RESOURCE_DB_NAME);
// const RESOURCE_MAPPINGS: TableDefinition<&[u8], &[u8]> =
//     TableDefinition::new(tables::RESOURCE_MAPPINGS_DB_NAME);
// const USERS: TableDefinition<&[u8], &[u8]> = TableDefinition::new(tables::USER_DB_NAME);
// const USER_MAPPINGS: TableDefinition<&[u8], &[u8]> =
//     TableDefinition::new(tables::USER_MAPPINGS_DB_NAME);
// const PUBLIC_MAPPINGS: TableDefinition<&[u8], &[u8]> =
//     TableDefinition::new(tables::PUBLIC_MAPPINGS_DB_NAME);

pub struct Redb {
    db: Arc<Database>,
    tables: HashMap<&'static str, TableDefinition<'static, &'static [u8], &'static [u8]>>,
}

impl std::fmt::Debug for Redb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let key: Vec<String> = self
            .tables
            .clone()
            .into_iter()
            .map(|k| k.0.to_string())
            .collect();
        f.debug_struct("Redb")
            .field("db", &self.db)
            .field("tables", &key)
            .finish()
    }
}
pub struct RedbConfig {
    pub path: String,
    pub databases: Vec<&'static str>,
}
pub enum RedbTxn {
    Write(WriteTransaction),
    Read(ReadTransaction),
}

impl<'a> Store<'a> for Redb {
    type StoreConfig = RedbConfig;

    type Txn = RedbTxn;

    fn new(config: Self::StoreConfig) -> Result<Self, crate::error::ArunaStorageError> {
        let path = format!("{}/store", config.path);
        println!("{path}");
        fs::create_dir_all(&path)?;
        let file = format!("/{path}/database.redb");

        let db = Arc::new(Database::create(file)?);
        let db_clone = db.clone();

        let mut tables = HashMap::new();
        let write_txn = db_clone.begin_write()?;
        {
            for key in config.databases {
                let table_definition = TableDefinition::new(key);
                tables.insert(key, table_definition);
                write_txn.open_table(table_definition);
            }
        }
        write_txn.commit()?;

        Ok(Self { db, tables })
    }

    fn create_txn(&'a self, write: bool) -> Result<Self::Txn, crate::error::ArunaStorageError> {
        Ok(if write {
            RedbTxn::Write(self.db.begin_write()?)
        } else {
            RedbTxn::Read(self.db.begin_read()?)
        })
    }

    fn put(
        &'a self,
        txn: &mut Self::Txn,
        dbname: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), crate::error::ArunaStorageError> {
        let RedbTxn::Write(write_txn) = txn else {
            return Err(ArunaStorageError::DatabaseError(
                "Only write txns allowed".to_string(),
            ));
        };
        let table = self
            .tables
            .get(dbname)
            .ok_or_else(|| {
                ArunaStorageError::DatabaseError("Table definition not found".to_string())
            })?
            .clone();

        let mut db = write_txn.open_table(table)?;
        db.insert(key, value)?;

        Ok(())
    }

    fn remove(
        &'a self,
        txn: &mut Self::Txn,
        dbname: &str,
        key: &[u8],
    ) -> Result<(), crate::error::ArunaStorageError> {
        let RedbTxn::Write(write_txn) = txn else {
            return Err(ArunaStorageError::DatabaseError(
                "Only write txns allowed".to_string(),
            ));
        };
        let table = self
            .tables
            .get(dbname)
            .ok_or_else(|| {
                ArunaStorageError::DatabaseError("Table definition not found".to_string())
            })?
            .clone();
        let mut db = write_txn.open_table(table)?;
        db.remove(key)?;
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
        let result = match txn {
            RedbTxn::Read(r) => {
                let table = self
                    .tables
                    .get(dbname)
                    .ok_or_else(|| {
                        ArunaStorageError::DatabaseError("Table definition not found".to_string())
                    })?
                    .clone();
                let db = r.open_table(table)?;
                db.get(key)?.map(|v| {
                    // TODO: This sucks, needs improvement
                    let value = v.value().to_vec();
                    Cow::from(value).to_owned()
                })
            }
            RedbTxn::Write(w) => {
                let table = self
                    .tables
                    .get(dbname)
                    .ok_or_else(|| {
                        ArunaStorageError::DatabaseError("Table definition not found".to_string())
                    })?
                    .clone();
                let db = w.open_table(table)?;
                db.get(key)?.map(|v| {
                    // TODO: This sucks, needs improvement
                    let value = v.value().to_vec();
                    Cow::from(value).to_owned()
                })
            }
        };

        Ok(result)
    }

    fn commit(&self, txn: Self::Txn) -> Result<(), crate::error::ArunaStorageError> {
        match txn {
            RedbTxn::Write(w) => w.commit()?,
            RedbTxn::Read(r) => r.close()?,
        };
        Ok(())
    }

    fn iter_db<'b>(
        &'a self,
        txn: &'b Self::Txn,
        dbname: &'static str,
    ) -> Result<Box<dyn Iterator<Item = (Cow<'b, [u8]>, Cow<'b, [u8]>)> + 'b>, ArunaStorageError>
    where
        'a: 'b,
    {
        match txn {
            RedbTxn::Read(txn) => {
                let table = self.tables.get(dbname).ok_or_else(|| {
                    ArunaStorageError::DatabaseError("Table definition not found".to_string())
                })?;
                let (sdx, rcv) = std::sync::mpsc::channel();
                std::thread::spawn(move || {
                    let db = txn.open_table(*table)?;
                    let iter =
                        db.iter()?
                            .filter_map(|kv| -> Option<(Cow<'b, [u8]>, Cow<'b, [u8]>)> {
                                let (key, value) = kv.ok()?;
                                let key = Cow::from(key.value().to_vec()).to_owned();
                                let value = Cow::from(value.value().to_vec()).to_owned();
                                Some((key, value))
                            });

                    for i in iter {
                        sdx.send(i);
                    }
                    Ok::<(), ArunaStorageError>(())
                });
                Ok(Box::new(rcv.into_iter()))
            }
            RedbTxn::Write(txn) => {
                let table = self
                    .tables
                    .get(dbname)
                    .ok_or_else(|| {
                        ArunaStorageError::DatabaseError("Table definition not found".to_string())
                    })?
                    .clone();
                let db = txn.open_table(table)?;
                let iter = db
                    .iter()?
                    .filter_map(|kv| -> Option<(Cow<'b, [u8]>, Cow<'b, [u8]>)> {
                        let (key, value) = kv.ok()?;
                        let key = Cow::from(key.value().to_vec()).to_owned();
                        let value = Cow::from(value.value().to_vec()).to_owned();
                        Some((key, value))
                    });
                Ok(Box::new(iter))
            }
        }
    }
}
