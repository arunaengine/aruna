use super::store::Store;
use crate::error::ArunaStorageError;
use heed::{Database, Env, EnvFlags, EnvOpenOptions, RoTxn, RwTxn, WithTls, types::Bytes};
use std::{borrow::Cow, fs};
use tracing::trace;

#[derive(Debug, Clone)]
pub struct LmdbStore {
    env: Env,
}

pub enum LmdbTxn<'a> {
    Read(RoTxn<'a, WithTls>),
    Write(RwTxn<'a>),
}

impl<'a> From<&'a LmdbTxn<'a>> for &RoTxn<'a> {
    fn from(value: &'a LmdbTxn<'a>) -> Self {
        match value {
            LmdbTxn::Read(ro_txn) => ro_txn,
            LmdbTxn::Write(rw_txn) => rw_txn,
        }
    }
}

impl<'a> LmdbTxn<'a> {
    pub fn commit(self) -> Result<(), ArunaStorageError> {
        match self {
            LmdbTxn::Read(ro_txn) => ro_txn.commit(),
            LmdbTxn::Write(rw_txn) => rw_txn.commit(),
        }
        .map_err(Into::into)
    }
}

pub struct LmdbConfig {
    pub path: String,
    pub databases: Vec<&'static str>,
}

impl<'a> Store<'a> for LmdbStore {
    type StoreConfig = LmdbConfig;
    type Txn = LmdbTxn<'a>;

    #[tracing::instrument(level = "trace", skip(config))]
    fn new(config: Self::StoreConfig) -> Result<Self, ArunaStorageError> {
        let path = format!("{}/store", config.path);
        fs::create_dir_all(&path)?;

        // SAFETY: This opens a memory mapped file that may introduce UB
        //         if handled incorrectly
        //         see: https://docs.rs/heed/latest/heed/struct.EnvOpenOptions.html#safety-1
        let mut env_options = EnvOpenOptions::new();
        unsafe { env_options.flags(EnvFlags::MAP_ASYNC | EnvFlags::WRITE_MAP) };
        env_options
            .map_size(30 * 1024 * 1024 * 1024) // 30 GB
            .max_readers(4098)
            .max_dbs(10);
        let env = unsafe { env_options.open(path) }?;

        // Init database
        let mut write_txn = env.write_txn()?;

        for dbname in config.databases {
            env.create_database::<Bytes, Bytes>(&mut write_txn, Some(dbname))?;
        }

        // Commit & return
        write_txn.commit()?;
        Ok(LmdbStore { env })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn create_txn(&'a self, write: bool) -> Result<LmdbTxn<'a>, ArunaStorageError> {
        trace!("Opening transaction");
        Ok(if write {
            LmdbTxn::Write(self.env.write_txn()?)
        } else {
            LmdbTxn::Read(self.env.read_txn()?)
        })
    }

    #[tracing::instrument(level = "trace", skip(self, txn, key, value))]
    fn put(
        &'a self,
        txn: &mut LmdbTxn<'a>,
        dbname: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), ArunaStorageError> {
        let txn = match txn {
            LmdbTxn::Read(_ro_txn) => {
                return Err(ArunaStorageError::DatabaseError(
                    "Read txn provided".to_string(),
                ));
            }
            LmdbTxn::Write(rw_txn) => rw_txn,
        };
        let db: Database<Bytes, Bytes> = self
            .env
            .open_database(txn, Some(dbname))?
            .ok_or_else(|| ArunaStorageError::DatabaseError("Database not found".to_string()))?;
        db.put(txn, key, value).map_err(Into::into)
    }

    #[tracing::instrument(level = "trace", skip(self, txn, key))]
    fn remove(
        &'a self,
        txn: &mut LmdbTxn<'a>,
        dbname: &str,
        key: &[u8],
    ) -> Result<(), ArunaStorageError> {
        let txn = match txn {
            LmdbTxn::Read(_ro_txn) => {
                return Err(ArunaStorageError::DatabaseError(
                    "Read txn provided".to_string(),
                ));
            }
            LmdbTxn::Write(rw_txn) => rw_txn,
        };
        let db: Database<Bytes, Bytes> = self
            .env
            .open_database(txn, Some(dbname))?
            .ok_or_else(|| ArunaStorageError::DatabaseError("Database not found".to_string()))?;
        db.delete(txn, key)?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, txn, key))]
    fn get<'b>(
        &'a self,
        txn: &'b LmdbTxn<'a>,
        dbname: &'b str,
        key: &'b [u8],
    ) -> Result<Option<Cow<'b, [u8]>>, ArunaStorageError>
    where
        'a: 'b,
    {
        let txn = txn.into();
        let db: Database<Bytes, Bytes> = self
            .env
            .open_database(txn, Some(dbname))?
            .ok_or_else(|| ArunaStorageError::DatabaseError("Database not found".to_string()))?;
        db.get(txn, key)
            .map(|r| r.map(Cow::from))
            .map_err(Into::into)
    }

    #[tracing::instrument(level = "trace", skip(self, txn))]
    fn commit(&self, txn: LmdbTxn<'_>) -> Result<(), ArunaStorageError> {
        trace!("Closing transaction");
        match txn {
            LmdbTxn::Read(ro_txn) => ro_txn.commit(),
            LmdbTxn::Write(rw_txn) => rw_txn.commit(),
        }
        .map_err(Into::into)
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
        let txn = txn.into();
        let db: Database<Bytes, Bytes> = self
            .env
            .open_database(txn, Some(dbname))?
            .ok_or_else(|| ArunaStorageError::DatabaseError("Database not found".to_string()))?;

        Ok(Box::new(db.iter(txn)?.filter_map(|iter| {
            // The trait signature for BytesDecode / BytesEncode contains an result return type
            // In our case its safe to skip since it will always return Ok
            let (key, value) = iter.ok()?;
            Some((Cow::from(key.to_vec()), Cow::from(value.to_vec())))
        })))
    }

    fn iter_over_prefix<'b>(
        &'a self,
        txn: &'b Self::Txn,
        dbname: &'static str,
        prefix: String,
    ) -> Result<Box<dyn Iterator<Item = (Cow<'b, [u8]>, Cow<'b, [u8]>)> + 'b>, ArunaStorageError>
    where
        'a: 'b,
    {
        let txn = txn.into();
        let db: Database<Bytes, Bytes> = self
            .env
            .open_database(txn, Some(dbname))?
            .ok_or_else(|| ArunaStorageError::DatabaseError("Database not found".to_string()))?;

        Ok(Box::new(db.prefix_iter(txn, prefix.as_bytes())?.filter_map(|iter| {
            // The trait signature for BytesDecode / BytesEncode contains an result return type
            // In our case its safe to skip since it will always return Ok
            let (key, value) = iter.ok()?;
            Some((Cow::from(key.to_vec()), Cow::from(value.to_vec())))
        })))
    }
}
