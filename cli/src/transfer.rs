//! Opens S3 sessions and moves files: multipart uploads that resume, downloads checked by BLAKE3.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::client::Api;
use crate::error::{CliError, s3_error};
use aruna_wire::info::InfoResponse;
use aruna_wire::session::{CLIENT_REFRESH_WINDOW, SessionTokenResponse};
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::{ByteStream, Length};
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use reqwest::Method;
use serde::Serialize;
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;
use tokio::task::JoinSet;

/// Any well-formed region is accepted; tools expect the AWS default.
pub const S3_REGION: &str = "us-east-1";
const MAX_PARTS: u64 = 10_000;
const PART_UPLOADS: usize = 4;
const PARTIAL_SUFFIX: &str = ".aruna-part";
const REFRESH_BEFORE: chrono::TimeDelta =
    chrono::TimeDelta::seconds(CLIENT_REFRESH_WINDOW.as_secs() as i64);

/// A short-lived S3 session of one group on one node, and the endpoint it is used at.
/// Restrictions are set only for a path-restricted token, which can neither refresh nor revoke.
#[derive(Clone, Debug)]
pub struct S3Session {
    pub token: SessionTokenResponse,
    pub endpoint: String,
}

/// Asks `api`'s node for an S3 session of `group_id`; it is valid on that node only.
/// `fallback` is the endpoint used when the node publishes none.
pub async fn open_session(
    api: &Api,
    group_id: &str,
    fallback: Option<&str>,
) -> Result<S3Session, CliError> {
    match request_session(api, group_id, fallback).await {
        Err(CliError::Api(error)) if error.status == 403 => Err(refused(group_id)),
        other => other,
    }
}

fn refused(group_id: &str) -> CliError {
    CliError::routing(format!(
        "the node refused an S3 session for group {group_id}: your roles grant no \
         access to the group's data on this node, or the node does not know the group yet"
    ))
}

async fn request_session(
    api: &Api,
    group_id: &str,
    fallback: Option<&str>,
) -> Result<S3Session, CliError> {
    let body = serde_json::json!({ "group_id": group_id });
    let token: SessionTokenResponse = api.send(Method::POST, "access/s3/sessions", &body).await?;
    let mut session = S3Session {
        token,
        endpoint: String::new(),
    };
    let issuer = &session.token.issuer_node;
    session.endpoint = match issuer.s3_endpoint.clone() {
        Some(endpoint) => endpoint,
        None => match node_endpoint(api, fallback, &issuer.node_id).await {
            Ok(endpoint) => endpoint,
            Err(error) => {
                revoke_session(api, &session).await;
                return Err(error);
            }
        },
    };
    Ok(session)
}

/// The S3 endpoint `api`'s node publishes, else `fallback`.
pub(crate) async fn node_endpoint(
    api: &Api,
    fallback: Option<&str>,
    node_id: &str,
) -> Result<String, CliError> {
    let info: InfoResponse = api.get("system/info", &[]).await?;
    info.services
        .interfaces
        .s3
        .url
        .or_else(|| fallback.map(str::to_string))
        .ok_or_else(|| {
            CliError::routing(format!(
                "node {node_id} publishes no S3 endpoint; pass --s3-endpoint"
            ))
        })
}

/// Revokes a session; a failure only warns, as the session ends at its expiry anyway.
/// A path-restricted token may not revoke, so its session is left to expire.
pub async fn revoke_session(api: &Api, session: &S3Session) {
    let token = &session.token;
    if !token.restrictions.is_empty() {
        return;
    }
    let path = format!("access/s3/sessions/{}", token.access_key_id);
    if let Err(error) = api.delete(&path).await {
        eprintln!(
            "warning: the S3 session {} was not revoked: {error}",
            token.access_key_id
        );
    }
}

fn client_config(endpoint: &str) -> aws_sdk_s3::config::Builder {
    aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(S3_REGION))
        .endpoint_url(endpoint)
        .force_path_style(true)
}

pub fn s3_client(session: &S3Session) -> aws_sdk_s3::Client {
    let token = &session.token;
    let credentials = Credentials::new(
        &token.access_key_id,
        &token.secret_access_key,
        Some(token.session_token.clone()),
        None,
        "aruna-cli",
    );
    let config = client_config(&session.endpoint)
        .credentials_provider(credentials)
        .build();
    aws_sdk_s3::Client::from_conf(config)
}

/// One group's S3 client on one node: a session that is refreshed before it expires and revoked
/// at the end, or unsigned reads of public objects for a caller the node gives no session.
pub struct S3Access {
    api: Api,
    session: Option<S3Session>,
    client: aws_sdk_s3::Client,
    endpoint: String,
}

impl S3Access {
    /// Opens a session; with `read_only`, a refused session falls back to unsigned reads.
    pub async fn open(
        api: &Api,
        group_id: &str,
        fallback: Option<&str>,
        read_only: bool,
    ) -> Result<Self, CliError> {
        let session = match request_session(api, group_id, fallback).await {
            Err(CliError::Api(error)) if error.status == 403 && read_only => None,
            Err(CliError::Api(error)) if error.status == 403 => return Err(refused(group_id)),
            other => Some(other?),
        };
        let (client, endpoint) = match &session {
            Some(session) => (s3_client(session), session.endpoint.clone()),
            None => {
                let node_id = api.node_id.clone().unwrap_or_default();
                let endpoint = node_endpoint(api, fallback, &node_id).await?;
                let config = client_config(&endpoint).allow_no_auth().build();
                (aws_sdk_s3::Client::from_conf(config), endpoint)
            }
        };
        Ok(Self {
            api: api.clone(),
            session,
            client,
            endpoint,
        })
    }

    /// The client, after refreshing a session that is about to expire.
    pub async fn client(&mut self) -> Result<aws_sdk_s3::Client, CliError> {
        let Some(session) = &mut self.session else {
            return Ok(self.client.clone());
        };
        let now = chrono::Utc::now();
        if !expires_within(&session.token.expires_at, now, REFRESH_BEFORE) {
            return Ok(self.client.clone());
        }
        if !session.token.restrictions.is_empty() {
            return Err(cannot_refresh(&session.token.expires_at));
        }
        let path = format!("access/s3/sessions/{}/refresh", session.token.access_key_id);
        match self
            .api
            .send::<SessionTokenResponse>(Method::POST, &path, &serde_json::json!({}))
            .await
        {
            Ok(fresh) => {
                session.token = fresh;
                self.client = s3_client(session);
            }
            // The node may see the refresh window later; the old pair works until expiry.
            Err(_)
                if !expires_within(&session.token.expires_at, now, chrono::TimeDelta::zero()) => {}
            Err(error) => return Err(error),
        }
        Ok(self.client.clone())
    }

    /// Revokes the session, so it does not hold one of the few places a group has per node.
    pub async fn revoke(self) {
        if let Some(session) = &self.session {
            revoke_session(&self.api, session).await;
        }
    }
}

fn cannot_refresh(expires_at: &str) -> CliError {
    CliError::usage(format!(
        "the S3 session expires at {expires_at} and a path-restricted token cannot refresh it; \
         rerun to continue, or sign in with an unrestricted token for longer transfers"
    ))
}

/// Whether an RFC 3339 expiry is less than `margin` away; an unreadable one never expires.
fn expires_within(
    expires_at: &str,
    now: chrono::DateTime<chrono::Utc>,
    margin: chrono::TimeDelta,
) -> bool {
    chrono::DateTime::parse_from_rfc3339(expires_at)
        .is_ok_and(|expiry| expiry.with_timezone(&chrono::Utc) - now < margin)
}

/// One byte range of a multipart upload; numbers start at 1.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Part {
    pub number: i32,
    pub offset: u64,
    pub length: u64,
}

/// A part the node already holds for an unfinished upload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredPart {
    pub number: i32,
    pub size: u64,
    pub etag: String,
}

/// Splits `size` bytes into parts of `part_size`, grown so at most 10 000 parts are needed.
pub fn split_parts(size: u64, part_size: u64) -> Vec<Part> {
    let part_size = part_size.max(size.div_ceil(MAX_PARTS)).max(1);
    (0..size.div_ceil(part_size))
        .map(|index| {
            let offset = index * part_size;
            Part {
                number: index as i32 + 1,
                offset,
                length: part_size.min(size - offset),
            }
        })
        .collect()
}

/// For each part, the stored ETag it can reuse: same number, size and MD5.
pub fn reusable_parts(
    parts: &[Part],
    md5s: &[String],
    stored: &[StoredPart],
) -> Vec<Option<String>> {
    let by_number: std::collections::HashMap<i32, &StoredPart> =
        stored.iter().map(|held| (held.number, held)).collect();
    parts
        .iter()
        .zip(md5s)
        .map(|(part, md5)| {
            (by_number.get(&part.number))
                .filter(|held| held.size == part.length)
                .filter(|held| held.etag.trim_matches('"').eq_ignore_ascii_case(md5))
                .map(|held| held.etag.clone())
        })
        .collect()
}

/// The BLAKE3 of the whole file and the MD5 of each part, in one pass.
fn digest_file(path: &Path, parts: &[Part]) -> std::io::Result<(String, Vec<String>)> {
    let mut file = std::fs::File::open(path)?;
    let mut whole = blake3::Hasher::new();
    let mut md5s = Vec::with_capacity(parts.len());
    let mut buffer = vec![0u8; 1 << 20];
    for part in parts {
        let mut context = md5::Context::new();
        let mut left = part.length;
        while left > 0 {
            let want = left.min(buffer.len() as u64) as usize;
            file.read_exact(&mut buffer[..want])?;
            whole.update(&buffer[..want]);
            context.consume(&buffer[..want]);
            left -= want as u64;
        }
        md5s.push(format!("{:x}", context.finalize()));
    }
    Ok((whole.finalize().to_hex().to_string(), md5s))
}

/// What an upload sent and how much of it an earlier attempt had already stored.
#[derive(Clone, Debug, Serialize)]
pub struct Uploaded {
    pub size: u64,
    pub blake3: String,
    pub reused_parts: usize,
}

/// Uploads a file; above `part_size` as a multipart upload. The upload id is kept in `records`,
/// so only a rerun of the same transfer continues it, and never two runs at once.
pub async fn upload(
    access: &mut S3Access,
    (bucket, key): (&str, &str),
    path: &Path,
    part_size: u64,
    records: &Path,
) -> Result<Uploaded, CliError> {
    let size = tokio::fs::metadata(path).await?.len();
    if size <= part_size {
        // One read is hashed and sent, so the reported digest and size are of the sent bytes.
        let bytes = tokio::fs::read(path).await?;
        let (bytes, blake3) = tokio::task::spawn_blocking(move || {
            let blake3 = blake3::hash(&bytes).to_hex().to_string();
            (bytes, blake3)
        })
        .await
        .map_err(|error| CliError::Io(std::io::Error::other(error)))?;
        let size = bytes.len() as u64;
        let body = ByteStream::from(bytes);
        access
            .client()
            .await?
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(body)
            .send()
            .await
            .map_err(s3_error)?;
        return Ok(Uploaded {
            size,
            blake3,
            reused_parts: 0,
        });
    }
    let parts = split_parts(size, part_size);
    let (blake3, md5s) = {
        let (path, parts) = (path.to_path_buf(), parts.clone());
        tokio::task::spawn_blocking(move || digest_file(&path, &parts))
            .await
            .map_err(|error| CliError::Io(std::io::Error::other(error)))??
    };
    let named = (access.endpoint.as_str(), bucket, key, blake3.as_str());
    let (mut record, kept) = claim_record(upload_record(records, named, &parts)).await?;
    // Hashing and the wait for the record can outlast the session, so it refreshes only now.
    let client = access.client().await?;
    let resumed = match kept {
        Some(upload_id) => (stored_parts(&client, bucket, key, &upload_id).await?)
            .map(|stored| (upload_id, stored)),
        None => None,
    };
    let (upload_id, stored) = match resumed {
        Some(resumed) => resumed,
        None => {
            let created = client
                .create_multipart_upload()
                .bucket(bucket)
                .key(key)
                .send()
                .await
                .map_err(s3_error)?;
            let upload_id = created
                .upload_id
                .ok_or_else(|| CliError::S3("the node returned no upload id".into()))?;
            if let Err(error) = keep_id(&mut record, &upload_id) {
                // Without its record no run could resume or find the new upload, so it goes.
                let abort = client.abort_multipart_upload().bucket(bucket).key(key);
                if let Err(failed) = abort.upload_id(&upload_id).send().await {
                    eprintln!("warning: upload {upload_id} was not aborted: {}", s3_error(failed));
                }
                return Err(error.into());
            }
            (upload_id, Vec::new())
        }
    };
    let reuse = reusable_parts(&parts, &md5s, &stored);
    let reused_parts = reuse.iter().filter(|etag| etag.is_some()).count();
    let mut etags: Vec<Option<String>> = reuse;
    let mut tasks = JoinSet::new();
    let pending: Vec<Part> = parts
        .iter()
        .filter(|part| etags[part.number as usize - 1].is_none())
        .copied()
        .collect();
    for part in pending {
        if tasks.len() >= PART_UPLOADS
            && let Some(done) = tasks.join_next().await
        {
            let (number, etag) = joined(done)?;
            check_part(&md5s, (number, &etag), path)?;
            etags[number as usize - 1] = Some(etag);
        }
        let client = access.client().await?;
        let (bucket, key) = (bucket.to_string(), key.to_string());
        let (upload_id, path) = (upload_id.clone(), path.to_path_buf());
        tasks
            .spawn(async move { send_part(&client, &bucket, &key, &upload_id, &path, part).await });
    }
    while let Some(done) = tasks.join_next().await {
        let (number, etag) = joined(done)?;
        check_part(&md5s, (number, &etag), path)?;
        etags[number as usize - 1] = Some(etag);
    }
    let completed: Vec<CompletedPart> = parts
        .iter()
        .zip(etags)
        .map(|(part, etag)| {
            CompletedPart::builder()
                .part_number(part.number)
                .set_e_tag(etag)
                .build()
        })
        .collect();
    access
        .client()
        .await?
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed))
                .build(),
        )
        .send()
        .await
        .map_err(s3_error)?;
    clear_record(&mut record, key);
    Ok(Uploaded {
        size,
        blake3,
        reused_parts,
    })
}

/// The folder of upload records below the config folder.
pub(crate) const UPLOAD_RECORDS: &str = "uploads";

/// Records `upload_id` as the provider upload of copying `path` to `bucket` and `key` at
/// `endpoint` in parts of `part_size` bytes, as an interrupted run leaves it.
#[cfg(feature = "test-hooks")]
pub(crate) async fn keep_upload(
    config_dir: &Path,
    (endpoint, bucket, key): (&str, &str, &str),
    (path, part_size): (&Path, u64),
    upload_id: &str,
) -> Result<(), CliError> {
    let parts = split_parts(std::fs::metadata(path)?.len(), part_size);
    let (blake3, _) = digest_file(path, &parts)?;
    let records = config_dir.join(UPLOAD_RECORDS);
    let record = upload_record(&records, (endpoint, bucket, key, &blake3), &parts);
    let (mut record, _) = claim_record(record).await?;
    keep_id(&mut record, upload_id)?;
    Ok(())
}

/// The record file of one transfer: its endpoint, bucket, key, content and part layout name it.
fn upload_record(records: &Path, named: (&str, &str, &str, &str), parts: &[Part]) -> PathBuf {
    let (endpoint, bucket, key, blake3) = named;
    let mut hasher = blake3::Hasher::new();
    for field in [endpoint, bucket, key, blake3] {
        hasher.update(&(field.len() as u64).to_le_bytes());
        hasher.update(field.as_bytes());
    }
    for part in parts {
        hasher.update(&part.offset.to_le_bytes());
        hasher.update(&part.length.to_le_bytes());
    }
    records.join(format!("{}.upload", hasher.finalize().to_hex()))
}

/// Opens and locks the record at `path`, waiting while another run holds it, and answers the
/// upload id an earlier run of the same transfer kept there. The lock ends with the file.
async fn claim_record(path: PathBuf) -> Result<(std::fs::File, Option<String>), CliError> {
    let claimed = tokio::task::spawn_blocking(move || -> std::io::Result<_> {
        if let Some(folder) = path.parent() {
            std::fs::create_dir_all(folder)?;
        }
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        file.lock()?;
        let mut kept = String::new();
        file.read_to_string(&mut kept)?;
        let kept = kept.trim().to_string();
        Ok((file, (!kept.is_empty()).then_some(kept)))
    });
    Ok(claimed
        .await
        .map_err(|error| CliError::Io(std::io::Error::other(error)))??)
}

/// Marks a claimed record finished. The emptied file stays, so a run waiting on its lock and
/// every later run lock this same file.
fn finish_record(record: &mut std::fs::File) -> std::io::Result<()> {
    keep_id(record, "")
}

/// Finishes the record of a completed upload, best effort: the object exists, and a kept id
/// only makes a later run of the same file meet `NoSuchUpload` and start afresh.
fn clear_record(record: &mut std::fs::File, key: &str) {
    if let Err(error) = finish_record(record) {
        eprintln!("warning: the upload record of {key} was not cleared: {error}");
    }
}

/// Replaces the upload id in a claimed record; an empty id marks the transfer as finished.
fn keep_id(record: &mut std::fs::File, upload_id: &str) -> std::io::Result<()> {
    record.set_len(0)?;
    record.rewind()?;
    record.write_all(upload_id.as_bytes())?;
    record.sync_all()
}

/// A part's ETag is the MD5 of the bytes the node stored; another MD5 than the hashed one means
/// the file changed while it was sent, so the hashed digest would not describe the upload.
fn check_part(md5s: &[String], (number, etag): (i32, &str), path: &Path) -> Result<(), CliError> {
    let expected = (number as usize)
        .checked_sub(1)
        .and_then(|index| md5s.get(index));
    let etag = etag.trim_matches('"');
    match expected {
        Some(expected) if expected.eq_ignore_ascii_case(etag) => Ok(()),
        expected => Err(CliError::Checksum {
            path: path.display().to_string(),
            expected: format!(
                "part {number} with MD5 {}",
                expected.map_or("", String::as_str)
            ),
            actual: etag.to_string(),
        }),
    }
}

fn joined(
    done: Result<Result<(i32, String), CliError>, tokio::task::JoinError>,
) -> Result<(i32, String), CliError> {
    done.map_err(|error| CliError::Io(std::io::Error::other(error)))?
}

async fn send_part(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    path: &Path,
    part: Part,
) -> Result<(i32, String), CliError> {
    let body = ByteStream::read_from()
        .path(path)
        .offset(part.offset)
        .length(Length::Exact(part.length))
        .build()
        .await
        .map_err(|error| CliError::S3(error.to_string()))?;
    let answer = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(part.number)
        .body(body)
        .send()
        .await
        .map_err(s3_error)?;
    let etag = answer
        .e_tag
        .ok_or_else(|| CliError::S3(format!("part {} returned no ETag", part.number)))?;
    Ok((part.number, etag))
}

/// The parts the node holds for `upload_id`, or `None` once the node no longer knows the upload.
async fn stored_parts(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> Result<Option<Vec<StoredPart>>, CliError> {
    let mut stored = Vec::new();
    let mut marker = None;
    loop {
        let listed = client
            .list_parts()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .set_part_number_marker(marker)
            .send()
            .await;
        let page = match listed {
            Ok(page) => page,
            Err(error)
                if error.as_service_error().and_then(|error| error.code())
                    == Some("NoSuchUpload") =>
            {
                return Ok(None);
            }
            Err(error) => return Err(s3_error(error)),
        };
        for part in page.parts.unwrap_or_default() {
            if let (Some(number), Some(size), Some(etag)) =
                (part.part_number, part.size, part.e_tag)
            {
                stored.push(StoredPart {
                    number,
                    size: size.max(0) as u64,
                    etag,
                });
            }
        }
        match (page.is_truncated, page.next_part_number_marker) {
            (Some(true), Some(next)) => marker = Some(next),
            _ => return Ok(Some(stored)),
        }
    }
}

/// What a download wrote.
#[derive(Clone, Debug, Serialize)]
pub struct Downloaded {
    pub size: u64,
    pub blake3: String,
}

/// A new file `.<name>.<random>.aruna-part` beside `dest`, removed on drop unless persisted.
fn scratch_file(dest: &Path) -> std::io::Result<tempfile::NamedTempFile> {
    let folder = match dest.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    let mut prefix = std::ffi::OsString::from(".");
    prefix.push(dest.file_name().unwrap_or_default());
    prefix.push(".");
    tempfile::Builder::new()
        .prefix(&prefix)
        .suffix(PARTIAL_SUFFIX)
        .tempfile_in(folder)
}

/// Downloads into a scratch file of its own, checks size and BLAKE3, then moves it to `dest`.
pub async fn download(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    dest: &Path,
    size: Option<u64>,
    blake3: Option<&str>,
) -> Result<Downloaded, CliError> {
    if let Some(parent) = dest
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        tokio::fs::create_dir_all(parent).await?;
    }
    let answer = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(s3_error)?;
    let scratch = scratch_file(dest)?;
    let mut file = tokio::fs::File::from_std(scratch.as_file().try_clone()?);
    let mut hasher = blake3::Hasher::new();
    let mut body = answer.body;
    let mut written = 0u64;
    while let Some(chunk) = body
        .try_next()
        .await
        .map_err(|error| CliError::S3(format!("download of {key} broke off: {error}")))?
    {
        hasher.update(&chunk);
        file.write_all(&chunk).await?;
        written += chunk.len() as u64;
    }
    file.flush().await?;
    drop(file);
    let actual = hasher.finalize().to_hex().to_string();
    let wrong_size = size.is_some_and(|size| size != written);
    let wrong_digest = blake3.is_some_and(|expected| !expected.eq_ignore_ascii_case(&actual));
    if wrong_size || wrong_digest {
        return Err(CliError::Checksum {
            path: format!("{bucket}/{key}"),
            expected: blake3.unwrap_or("(size only)").to_string(),
            actual: format!("{actual} ({written} bytes)"),
        });
    }
    scratch.persist(dest).map_err(|error| error.error)?;
    Ok(Downloaded {
        size: written,
        blake3: actual,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_wire::session::{
        SessionGroupResponse, SessionIssuerResponse, SessionRestrictionResponse,
    };

    #[test]
    fn splits_into_parts() {
        let parts = split_parts(25, 10);
        assert_eq!(parts.len(), 3);
        assert_eq!(
            parts[2],
            Part {
                number: 3,
                offset: 20,
                length: 5
            }
        );
        // Parts grow so a huge file never needs more than 10 000.
        let parts = split_parts(MAX_PARTS * 100 + 1, 1);
        assert!(parts.len() as u64 <= MAX_PARTS);
        assert_eq!(
            parts.iter().map(|part| part.length).sum::<u64>(),
            MAX_PARTS * 100 + 1
        );
    }

    #[test]
    fn changed_part_refused() {
        // A part whose stored MD5 differs from the hashed one fails the upload.
        let md5s = vec!["aa".to_string(), "bb".to_string()];
        let path = Path::new("data.bin");
        assert!(check_part(&md5s, (2, "\"BB\""), path).is_ok());
        assert!(matches!(
            check_part(&md5s, (1, "\"cc\""), path),
            Err(CliError::Checksum { .. })
        ));
        assert!(check_part(&md5s, (3, "\"aa\""), path).is_err());
    }

    #[test]
    fn transfers_keep_apart() {
        // Another file for the same key is another transfer, with an upload of its own.
        let records = Path::new("records");
        let parts = split_parts(25, 10);
        let named = |blake3| ("http://node", "bucket", "key", blake3);
        let first = upload_record(records, named("aa"), &parts);
        assert_eq!(first, upload_record(records, named("aa"), &parts));
        assert_ne!(first, upload_record(records, named("bb"), &parts));
        let other = split_parts(25, 5);
        assert_ne!(first, upload_record(records, named("aa"), &other));
    }

    #[tokio::test]
    async fn record_keeps_id() {
        // A rerun of the same transfer reads the id the first run kept; a finished one none.
        let folder = tempfile::tempdir().unwrap();
        let path = folder.path().join("nested").join("record.upload");
        let (mut record, kept) = claim_record(path.clone()).await.unwrap();
        assert_eq!(kept, None);
        keep_id(&mut record, "upload-1").unwrap();
        drop(record);
        let (mut record, kept) = claim_record(path.clone()).await.unwrap();
        assert_eq!(kept.as_deref(), Some("upload-1"));
        keep_id(&mut record, "").unwrap();
        drop(record);
        assert_eq!(claim_record(path).await.unwrap().1, None);
    }

    #[test]
    fn clearing_best_effort() {
        // A record that cannot be written leaves the completed upload a success.
        let folder = tempfile::tempdir().unwrap();
        let path = folder.path().join("record.upload");
        std::fs::write(&path, "upload-1").unwrap();
        let mut read_only = std::fs::File::open(&path).unwrap();
        clear_record(&mut read_only, "key");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "upload-1");
    }

    #[tokio::test]
    async fn finished_record_stays() {
        // A finished transfer keeps its emptied record, so a waiting run locks the same file.
        let folder = tempfile::tempdir().unwrap();
        let path = folder.path().join("record.upload");
        let (mut record, _) = claim_record(path.clone()).await.unwrap();
        keep_id(&mut record, "upload-1").unwrap();
        finish_record(&mut record).unwrap();
        assert!(path.exists(), "the record stays while its lock is held");
        drop(record);
        assert!(path.exists());
        assert_eq!(claim_record(path).await.unwrap().1, None);
    }

    #[test]
    fn reuses_matching_parts() {
        let parts = split_parts(25, 10);
        let md5s = vec!["aa".to_string(), "bb".to_string(), "cc".to_string()];
        let stored = vec![
            StoredPart {
                number: 1,
                size: 10,
                etag: "\"AA\"".into(),
            },
            StoredPart {
                number: 2,
                size: 10,
                etag: "\"ff\"".into(),
            },
            StoredPart {
                number: 3,
                size: 4,
                etag: "\"cc\"".into(),
            },
        ];
        let reuse = reusable_parts(&parts, &md5s, &stored);
        assert_eq!(reuse, vec![Some("\"AA\"".to_string()), None, None]);
    }

    #[test]
    fn digests_match_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.bin");
        let bytes: Vec<u8> = (0..3000u32).map(|value| value as u8).collect();
        std::fs::write(&path, &bytes).unwrap();
        let parts = split_parts(bytes.len() as u64, 1024);
        let (whole, md5s) = digest_file(&path, &parts).unwrap();
        assert_eq!(whole, blake3::hash(&bytes).to_hex().to_string());
        assert_eq!(md5s[1], format!("{:x}", md5::compute(&bytes[1024..2048])));
        assert_eq!(md5s.len(), 3);
    }

    #[test]
    fn refreshes_near_expiry() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-11-03T12:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert!(expires_within("2026-11-03T12:03:00Z", now, REFRESH_BEFORE));
        assert!(expires_within("2026-11-03T11:59:00Z", now, REFRESH_BEFORE));
        assert!(!expires_within("2026-11-03T12:30:00Z", now, REFRESH_BEFORE));
        assert!(!expires_within("not a time", now, REFRESH_BEFORE));
    }

    #[tokio::test]
    async fn refuses_restricted_refresh() {
        let soon = chrono::Utc::now() + chrono::TimeDelta::minutes(1);
        let token = SessionTokenResponse {
            access_key_id: "key".into(),
            secret_access_key: "secret".into(),
            session_token: "token".into(),
            expires_at: soon.to_rfc3339(),
            group: SessionGroupResponse { id: "g".into() },
            restrictions: vec![SessionRestrictionResponse {
                pattern: "/r/g/*".into(),
                permission: "READ".into(),
            }],
            issuer_node: SessionIssuerResponse {
                node_id: "node".into(),
                s3_endpoint: None,
            },
        };
        let session = S3Session {
            token,
            endpoint: "http://127.0.0.1:1".into(),
        };
        // The refusal comes before any request, so the unreachable API is never called.
        let mut access = S3Access {
            api: Api::new("http://127.0.0.1:1", None).unwrap(),
            client: s3_client(&session),
            session: Some(session),
            endpoint: "http://127.0.0.1:1".into(),
        };
        let error = access.client().await.unwrap_err();
        assert!(error.to_string().contains("cannot refresh"), "{error}");
    }

    #[test]
    fn keeps_foreign_partials() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("data.csv");
        let foreign = dir.path().join("data.csv.aruna-part");
        std::fs::write(&foreign, b"not ours").unwrap();
        let first = scratch_file(&dest).unwrap();
        let second = scratch_file(&dest).unwrap();
        assert_ne!(first.path(), second.path());
        assert_ne!(first.path(), foreign.as_path());
        assert!(first.path().to_string_lossy().ends_with(PARTIAL_SUFFIX));
        let scratch = first.path().to_path_buf();
        drop(first);
        assert!(!scratch.exists());
        assert_eq!(std::fs::read(&foreign).unwrap(), b"not ours");
    }
}
