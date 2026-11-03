//! Streams Fjall keyspaces in the database snapshot format and reads such a stream back with checks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use blake3::Hasher;
use fjall::{Database, KeyspaceCreateOptions, Readable, Snapshot};
use std::collections::HashSet;
use std::io::{ErrorKind, Read, Write};
use std::path::PathBuf;
use std::time::Duration;
use thiserror::Error;

const SNAPSHOT_MAGIC: &[u8] = b"ARUNA_DB_SNAPSHOT";
const SNAPSHOT_VERSION: u16 = 1;
const RECORD_BEGIN_KEYSPACE: u8 = 1;
const RECORD_ENTRY: u8 = 2;
const RECORD_END_KEYSPACE: u8 = 3;
const RECORD_FOOTER: u8 = 4;
/// Tries and pause between tries to open snapshots of several databases at one instant.
const CUT_ATTEMPTS: u32 = 1_000;
const CUT_PAUSE: Duration = Duration::from_millis(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotStats {
    pub created_unix_seconds: u64,
    pub keyspace_count: u64,
    pub entry_count: u64,
}

#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Fjall(#[from] fjall::Error),
    #[error("snapshot path already exists: {0}")]
    SnapshotPathExists(PathBuf),
    #[error("target database path already exists: {0}")]
    TargetPathExists(PathBuf),
    #[error("target database parent directory does not exist: {0}")]
    TargetParentMissing(PathBuf),
    #[error("invalid snapshot magic")]
    InvalidMagic,
    #[error("unsupported snapshot version: {0}")]
    UnsupportedVersion(u16),
    #[error("invalid snapshot structure: {0}")]
    InvalidStructure(&'static str),
    #[error("duplicate keyspace in snapshot: {0}")]
    DuplicateKeyspace(String),
    #[error("checksum mismatch")]
    ChecksumMismatch,
    #[error("snapshot contains invalid UTF-8 keyspace name")]
    InvalidKeyspaceName,
    #[error("snapshot length does not fit in memory: {0}")]
    LengthOverflow(u64),
    #[error("the databases kept changing while their snapshots were opened")]
    BusyDatabases,
}

/// One record of a snapshot stream, in stream order.
#[derive(Debug, PartialEq, Eq)]
pub enum SnapshotRecord {
    Keyspace(String),
    Entry { key: Vec<u8>, value: Vec<u8> },
    EndKeyspace,
}

/// Writes a snapshot stream: keyspace sections of entries, then a footer with counts and a
/// BLAKE3 checksum over all sections.
pub struct SnapshotWriter<W: Write> {
    out: W,
    hasher: Hasher,
    created_unix_seconds: u64,
    keyspace_count: u64,
    entry_count: u64,
    open_entries: Option<u64>,
}

impl<W: Write> SnapshotWriter<W> {
    pub fn new(mut out: W, created_unix_seconds: u64) -> Result<Self, SnapshotError> {
        out.write_all(SNAPSHOT_MAGIC)?;
        out.write_all(&SNAPSHOT_VERSION.to_be_bytes())?;
        out.write_all(&created_unix_seconds.to_be_bytes())?;
        Ok(Self {
            out,
            hasher: Hasher::new(),
            created_unix_seconds,
            keyspace_count: 0,
            entry_count: 0,
            open_entries: None,
        })
    }

    pub fn begin_keyspace(&mut self, name: &str) -> Result<(), SnapshotError> {
        if self.open_entries.is_some() {
            return Err(SnapshotError::InvalidStructure(
                "began a keyspace before closing the previous keyspace",
            ));
        }
        let mut record = Vec::with_capacity(1 + 8 + name.len());
        record.push(RECORD_BEGIN_KEYSPACE);
        push_prefixed_bytes(&mut record, name.as_bytes())?;
        self.write_record(&record)?;
        self.open_entries = Some(0);
        self.keyspace_count += 1;
        Ok(())
    }

    pub fn entry(&mut self, key: &[u8], value: &[u8]) -> Result<(), SnapshotError> {
        if self.open_entries.is_none() {
            return Err(SnapshotError::InvalidStructure(
                "wrote an entry outside a keyspace section",
            ));
        }
        let mut record = Vec::with_capacity(1 + 16 + key.len() + value.len());
        record.push(RECORD_ENTRY);
        push_prefixed_bytes(&mut record, key)?;
        push_prefixed_bytes(&mut record, value)?;
        self.write_record(&record)?;
        self.open_entries = self.open_entries.map(|count| count + 1);
        self.entry_count += 1;
        Ok(())
    }

    pub fn end_keyspace(&mut self) -> Result<(), SnapshotError> {
        let Some(entry_count) = self.open_entries.take() else {
            return Err(SnapshotError::InvalidStructure(
                "ended a keyspace outside a keyspace section",
            ));
        };
        let mut record = Vec::with_capacity(9);
        record.push(RECORD_END_KEYSPACE);
        record.extend_from_slice(&entry_count.to_be_bytes());
        self.write_record(&record)
    }

    /// Writes every keyspace of `db` from one read snapshot, sorted by name and each name
    /// prefixed with `prefix`. The database may keep serving writes meanwhile.
    pub fn database(&mut self, db: &Database, prefix: &str) -> Result<(), SnapshotError> {
        self.database_at(db, &db.snapshot(), prefix)
    }

    /// Writes every keyspace of `db` as `snapshot` shows it, like [`Self::database`].
    pub fn database_at(
        &mut self,
        db: &Database,
        snapshot: &Snapshot,
        prefix: &str,
    ) -> Result<(), SnapshotError> {
        let mut names = db.list_keyspace_names();
        names.sort();
        for name in names {
            let keyspace = db.keyspace(&name, KeyspaceCreateOptions::default)?;
            self.begin_keyspace(&format!("{prefix}{}", &*name))?;
            for entry in snapshot.iter(&keyspace) {
                let (key, value) = entry.into_inner()?;
                self.entry(&key, &value)?;
            }
            self.end_keyspace()?;
        }
        Ok(())
    }

    /// Writes the footer and returns the flushed output with the stream totals.
    pub fn finish(mut self) -> Result<(W, SnapshotStats), SnapshotError> {
        if self.open_entries.is_some() {
            return Err(SnapshotError::InvalidStructure(
                "finished before closing the active keyspace",
            ));
        }
        let checksum = *self.hasher.finalize().as_bytes();
        self.out.write_all(&[RECORD_FOOTER])?;
        self.out.write_all(&self.keyspace_count.to_be_bytes())?;
        self.out.write_all(&self.entry_count.to_be_bytes())?;
        self.out.write_all(&checksum)?;
        self.out.flush()?;
        let stats = SnapshotStats {
            created_unix_seconds: self.created_unix_seconds,
            keyspace_count: self.keyspace_count,
            entry_count: self.entry_count,
        };
        Ok((self.out, stats))
    }

    fn write_record(&mut self, record: &[u8]) -> Result<(), SnapshotError> {
        self.out.write_all(record)?;
        self.hasher.update(record);
        Ok(())
    }
}

/// Opens one read snapshot per database, all showing the same instant. Fjall cannot pause
/// writers, so a try that a write crossed is dropped and repeated.
pub fn consistent_snapshots(databases: &[&Database]) -> Result<Vec<Snapshot>, SnapshotError> {
    for _ in 0..CUT_ATTEMPTS {
        let snapshots: Vec<Snapshot> = databases.iter().map(|db| db.snapshot()).collect();
        if same_instant(databases, &snapshots) {
            return Ok(snapshots);
        }
        std::thread::sleep(CUT_PAUSE);
    }
    Err(SnapshotError::BusyDatabases)
}

/// Fjall takes a write's sequence number and publishes it under one journal lock, so a
/// database whose next number is still its snapshot's has not started a write since.
fn same_instant(databases: &[&Database], snapshots: &[Snapshot]) -> bool {
    databases
        .iter()
        .zip(snapshots)
        .all(|(db, snapshot)| db.seqno() == snapshot.seqno())
}

/// Reads a snapshot stream record by record and rejects bad structure, mismatched counts, a
/// wrong checksum and trailing bytes.
pub struct SnapshotReader<R: Read> {
    input: R,
    hasher: Hasher,
    created_unix_seconds: u64,
    seen_keyspaces: HashSet<String>,
    open_entries: Option<u64>,
    keyspace_count: u64,
    entry_count: u64,
    finished: bool,
}

impl<R: Read> SnapshotReader<R> {
    pub fn new(mut input: R) -> Result<Self, SnapshotError> {
        let mut magic = vec![0_u8; SNAPSHOT_MAGIC.len()];
        input.read_exact(&mut magic)?;
        if magic.as_slice() != SNAPSHOT_MAGIC {
            return Err(SnapshotError::InvalidMagic);
        }
        let version = u16::from_be_bytes(read_array(&mut input)?);
        if version != SNAPSHOT_VERSION {
            return Err(SnapshotError::UnsupportedVersion(version));
        }
        let created_unix_seconds = u64::from_be_bytes(read_array(&mut input)?);
        Ok(Self {
            input,
            hasher: Hasher::new(),
            created_unix_seconds,
            seen_keyspaces: HashSet::new(),
            open_entries: None,
            keyspace_count: 0,
            entry_count: 0,
            finished: false,
        })
    }

    pub fn stats(&self) -> SnapshotStats {
        SnapshotStats {
            created_unix_seconds: self.created_unix_seconds,
            keyspace_count: self.keyspace_count,
            entry_count: self.entry_count,
        }
    }

    /// The next record, or `None` once the footer matched and no bytes follow it.
    pub fn next_record(&mut self) -> Result<Option<SnapshotRecord>, SnapshotError> {
        if self.finished {
            return Ok(None);
        }
        let tag = match read_array::<1>(&mut self.input) {
            Ok([tag]) => tag,
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => {
                return Err(SnapshotError::InvalidStructure("missing footer record"));
            }
            Err(error) => return Err(error.into()),
        };
        match tag {
            RECORD_BEGIN_KEYSPACE => self.read_begin(tag).map(Some),
            RECORD_ENTRY => {
                let Some(count) = self.open_entries else {
                    return Err(SnapshotError::InvalidStructure(
                        "encountered an entry outside a keyspace section",
                    ));
                };
                self.hasher.update(&[tag]);
                let key = self.read_hashed_bytes()?;
                let value = self.read_hashed_bytes()?;
                self.open_entries = Some(count + 1);
                self.entry_count += 1;
                Ok(Some(SnapshotRecord::Entry { key, value }))
            }
            RECORD_END_KEYSPACE => {
                let Some(count) = self.open_entries.take() else {
                    return Err(SnapshotError::InvalidStructure(
                        "encountered end-of-keyspace outside a keyspace section",
                    ));
                };
                self.hasher.update(&[tag]);
                let expected = self.read_hashed_u64()?;
                if count != expected {
                    return Err(SnapshotError::InvalidStructure(
                        "keyspace entry count did not match the section footer",
                    ));
                }
                Ok(Some(SnapshotRecord::EndKeyspace))
            }
            RECORD_FOOTER => {
                self.read_footer()?;
                self.finished = true;
                Ok(None)
            }
            _ => Err(SnapshotError::InvalidStructure("unknown record tag")),
        }
    }

    fn read_begin(&mut self, tag: u8) -> Result<SnapshotRecord, SnapshotError> {
        if self.open_entries.is_some() {
            return Err(SnapshotError::InvalidStructure(
                "encountered a new keyspace before closing the previous keyspace",
            ));
        }
        self.hasher.update(&[tag]);
        let name = String::from_utf8(self.read_hashed_bytes()?)
            .map_err(|_| SnapshotError::InvalidKeyspaceName)?;
        if !self.seen_keyspaces.insert(name.clone()) {
            return Err(SnapshotError::DuplicateKeyspace(name));
        }
        self.open_entries = Some(0);
        self.keyspace_count += 1;
        Ok(SnapshotRecord::Keyspace(name))
    }

    fn read_footer(&mut self) -> Result<(), SnapshotError> {
        if self.open_entries.is_some() {
            return Err(SnapshotError::InvalidStructure(
                "encountered footer before closing the active keyspace",
            ));
        }
        let keyspace_count = u64::from_be_bytes(read_array(&mut self.input)?);
        let entry_count = u64::from_be_bytes(read_array(&mut self.input)?);
        let checksum: [u8; 32] = read_array(&mut self.input)?;
        if self.keyspace_count != keyspace_count {
            return Err(SnapshotError::InvalidStructure(
                "keyspace count did not match the footer",
            ));
        }
        if self.entry_count != entry_count {
            return Err(SnapshotError::InvalidStructure(
                "entry count did not match the footer",
            ));
        }
        if *self.hasher.finalize().as_bytes() != checksum {
            return Err(SnapshotError::ChecksumMismatch);
        }
        let mut trailing = [0_u8; 1];
        match self.input.read(&mut trailing)? {
            0 => Ok(()),
            _ => Err(SnapshotError::InvalidStructure(
                "snapshot has trailing bytes after the footer",
            )),
        }
    }

    fn read_hashed_u64(&mut self) -> Result<u64, SnapshotError> {
        let bytes = read_array(&mut self.input)?;
        self.hasher.update(&bytes);
        Ok(u64::from_be_bytes(bytes))
    }

    fn read_hashed_bytes(&mut self) -> Result<Vec<u8>, SnapshotError> {
        let len = self.read_hashed_u64()?;
        let len = usize::try_from(len).map_err(|_| SnapshotError::LengthOverflow(len))?;
        let mut bytes = vec![0_u8; len];
        self.input.read_exact(&mut bytes)?;
        self.hasher.update(&bytes);
        Ok(bytes)
    }
}

fn push_prefixed_bytes(buffer: &mut Vec<u8>, bytes: &[u8]) -> Result<(), SnapshotError> {
    let len = u64::try_from(bytes.len()).map_err(|_| SnapshotError::LengthOverflow(u64::MAX))?;
    buffer.extend_from_slice(&len.to_be_bytes());
    buffer.extend_from_slice(bytes);
    Ok(())
}

fn read_array<const N: usize>(input: &mut impl Read) -> Result<[u8; N], std::io::Error> {
    let mut bytes = [0_u8; N];
    input.read_exact(&mut bytes)?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::{
        SnapshotError, SnapshotReader, SnapshotRecord, SnapshotWriter, consistent_snapshots,
        same_instant,
    };
    use fjall::{Database, KeyspaceCreateOptions};

    fn sample_stream() -> Vec<u8> {
        let mut writer = SnapshotWriter::new(Vec::new(), 7).unwrap();
        writer.begin_keyspace("a").unwrap();
        writer.entry(b"k1", b"v1").unwrap();
        writer.entry(b"k2", b"v2").unwrap();
        writer.end_keyspace().unwrap();
        writer.finish().unwrap().0
    }

    fn read_all(bytes: &[u8]) -> Result<Vec<SnapshotRecord>, SnapshotError> {
        let mut reader = SnapshotReader::new(bytes)?;
        let mut records = Vec::new();
        while let Some(record) = reader.next_record()? {
            records.push(record);
        }
        Ok(records)
    }

    #[test]
    fn rejects_truncated_stream() {
        let bytes = sample_stream();
        for len in [bytes.len() - 1, bytes.len() - 40, 40] {
            assert!(
                read_all(&bytes[..len]).is_err(),
                "cut at {len} bytes was accepted"
            );
        }
    }

    #[test]
    fn rejects_changed_byte() {
        let mut bytes = sample_stream();
        let position = bytes.windows(2).position(|pair| pair == b"v2").unwrap();
        bytes[position + 1] = b'3';
        assert!(matches!(
            read_all(&bytes),
            Err(SnapshotError::ChecksumMismatch)
        ));
    }

    #[test]
    fn prefixes_database_keyspaces() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::builder(dir.path()).open().unwrap();
        let keyspace = db
            .keyspace("items", KeyspaceCreateOptions::default)
            .unwrap();
        keyspace.insert("key", "value").unwrap();

        let mut writer = SnapshotWriter::new(Vec::new(), 1).unwrap();
        writer.database(&db, "main/").unwrap();
        let (bytes, stats) = writer.finish().unwrap();

        assert_eq!((stats.keyspace_count, stats.entry_count), (1, 1));
        assert_eq!(
            read_all(&bytes).unwrap(),
            vec![
                SnapshotRecord::Keyspace("main/items".to_string()),
                SnapshotRecord::Entry {
                    key: b"key".to_vec(),
                    value: b"value".to_vec(),
                },
                SnapshotRecord::EndKeyspace,
            ]
        );
    }

    #[test]
    fn rejects_crossed_cut() {
        let dir = tempfile::tempdir().unwrap();
        let first = Database::builder(dir.path().join("first")).open().unwrap();
        let second = Database::builder(dir.path().join("second")).open().unwrap();
        let keyspace = second
            .keyspace("items", KeyspaceCreateOptions::default)
            .unwrap();
        keyspace.insert("old", "value").unwrap();
        let databases = [&first, &second];

        let snapshots = consistent_snapshots(&databases).unwrap();
        assert!(same_instant(&databases, &snapshots));
        keyspace.insert("new", "value").unwrap();
        assert!(!same_instant(&databases, &snapshots));

        let mut writer = SnapshotWriter::new(Vec::new(), 1).unwrap();
        writer.database_at(&second, &snapshots[1], "").unwrap();
        assert_eq!(writer.finish().unwrap().1.entry_count, 1);
    }
}
