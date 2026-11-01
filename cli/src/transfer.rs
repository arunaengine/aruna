//! Opens S3 sessions and moves files: multipart uploads that resume, downloads checked by BLAKE3.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::client::Api;
use crate::error::{CliError, s3_error};
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::primitives::{ByteStream, Length};
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use std::io::Read;
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;
use tokio::task::JoinSet;

/// Any well-formed region is accepted; tools expect the AWS default.
pub const S3_REGION: &str = "us-east-1";
const MAX_PARTS: u64 = 10_000;
const PART_UPLOADS: usize = 4;
const PARTIAL_SUFFIX: &str = ".aruna-part";

/// A short-lived S3 session of one group on one node.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct S3Session {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
    pub expires_at: String,
    pub issuer_node: Issuer,
    #[serde(default)]
    pub endpoint: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Issuer {
    pub node_id: String,
    pub s3_endpoint: Option<String>,
}

#[derive(Deserialize)]
struct NodeInfo {
    services: Services,
}

#[derive(Deserialize)]
struct Services {
    interfaces: Interfaces,
}

#[derive(Deserialize)]
struct Interfaces {
    s3: Interface,
}

#[derive(Deserialize)]
struct Interface {
    url: Option<String>,
}

/// Asks `api`'s node for an S3 session of `group_id`; it is valid on that node only.
/// `fallback` is the endpoint used when the node publishes none.
pub async fn open_session(
    api: &Api,
    group_id: &str,
    fallback: Option<&str>,
) -> Result<S3Session, CliError> {
    let body = serde_json::json!({ "group_id": group_id });
    let mut session: S3Session = match api.send(Method::POST, "access/s3/sessions", &body).await {
        Err(CliError::Api(error)) if error.status == 403 => {
            return Err(CliError::routing(format!(
                "the node refused an S3 session for group {group_id}: your roles grant no \
                 access to the group's data on this node, or the node does not know the group yet"
            )));
        }
        other => other?,
    };
    session.endpoint = match session.issuer_node.s3_endpoint.clone() {
        Some(endpoint) => endpoint,
        None => {
            let info: NodeInfo = api.get("system/info", &[]).await?;
            let published = info.services.interfaces.s3.url;
            published
                .or_else(|| fallback.map(str::to_string))
                .ok_or_else(|| {
                    CliError::routing(format!(
                        "node {} publishes no S3 endpoint; pass --s3-endpoint",
                        session.issuer_node.node_id
                    ))
                })?
        }
    };
    Ok(session)
}

pub fn s3_client(session: &S3Session) -> aws_sdk_s3::Client {
    let credentials = Credentials::new(
        &session.access_key_id,
        &session.secret_access_key,
        Some(session.session_token.clone()),
        None,
        "aruna-cli",
    );
    let config = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(S3_REGION))
        .credentials_provider(credentials)
        .endpoint_url(&session.endpoint)
        .force_path_style(true)
        .build();
    aws_sdk_s3::Client::from_conf(config)
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
    parts
        .iter()
        .zip(md5s)
        .map(|(part, md5)| {
            stored
                .iter()
                .find(|held| held.number == part.number && held.size == part.length)
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

pub async fn hash_file(path: &Path) -> Result<String, CliError> {
    let path = path.to_path_buf();
    let digest = tokio::task::spawn_blocking(move || -> std::io::Result<String> {
        let mut hasher = blake3::Hasher::new();
        hasher.update_reader(std::fs::File::open(path)?)?;
        Ok(hasher.finalize().to_hex().to_string())
    })
    .await
    .map_err(|error| CliError::Io(std::io::Error::other(error)))??;
    Ok(digest)
}

/// What an upload sent and how much of it an earlier attempt had already stored.
#[derive(Clone, Debug, Serialize)]
pub struct Uploaded {
    pub size: u64,
    pub blake3: String,
    pub reused_parts: usize,
}

/// Uploads a file; above `part_size` as a multipart upload that continues an unfinished one.
pub async fn upload(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    path: &Path,
    part_size: u64,
) -> Result<Uploaded, CliError> {
    let size = tokio::fs::metadata(path).await?.len();
    if size <= part_size {
        let blake3 = hash_file(path).await?;
        let body = ByteStream::from_path(path)
            .await
            .map_err(|error| CliError::S3(error.to_string()))?;
        client
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
    let (upload_id, stored) = match unfinished_upload(client, bucket, key).await? {
        Some(upload_id) => {
            let stored = stored_parts(client, bucket, key, &upload_id).await?;
            (upload_id, stored)
        }
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
            etags[number as usize - 1] = Some(etag);
        }
        let (client, bucket, key) = (client.clone(), bucket.to_string(), key.to_string());
        let (upload_id, path) = (upload_id.clone(), path.to_path_buf());
        tasks
            .spawn(async move { send_part(&client, &bucket, &key, &upload_id, &path, part).await });
    }
    while let Some(done) = tasks.join_next().await {
        let (number, etag) = joined(done)?;
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
    client
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
    Ok(Uploaded {
        size,
        blake3,
        reused_parts,
    })
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

/// The newest unfinished multipart upload of exactly `key`, which a rerun continues.
async fn unfinished_upload(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
) -> Result<Option<String>, CliError> {
    let listed = client
        .list_multipart_uploads()
        .bucket(bucket)
        .prefix(key)
        .send()
        .await
        .map_err(s3_error)?;
    let mut uploads: Vec<_> = listed
        .uploads
        .unwrap_or_default()
        .into_iter()
        .filter(|upload| upload.key.as_deref() == Some(key))
        .collect();
    uploads.sort_by_key(|upload| upload.initiated);
    Ok(uploads.pop().and_then(|upload| upload.upload_id))
}

async fn stored_parts(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> Result<Vec<StoredPart>, CliError> {
    let mut stored = Vec::new();
    let mut marker = None;
    loop {
        let page = client
            .list_parts()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .set_part_number_marker(marker)
            .send()
            .await
            .map_err(s3_error)?;
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
            _ => return Ok(stored),
        }
    }
}

/// What a download wrote and how many bytes an earlier attempt had left.
#[derive(Clone, Debug, Serialize)]
pub struct Downloaded {
    pub size: u64,
    pub blake3: String,
    pub resumed_bytes: u64,
}

/// The file a download writes before it is checked and renamed into place.
pub fn partial_path(dest: &Path) -> PathBuf {
    let mut name = dest.file_name().unwrap_or_default().to_os_string();
    name.push(PARTIAL_SUFFIX);
    dest.with_file_name(name)
}

/// Downloads into a partial file, continues one a former run left, and checks size and BLAKE3.
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
    let partial = partial_path(dest);
    let held = match tokio::fs::metadata(&partial).await {
        Ok(meta) => meta.len(),
        Err(_) => 0,
    };
    // Only a partial file shorter than the known size can be continued.
    let resumed_bytes = match size {
        Some(size) if blake3.is_some() && held > 0 && held < size => held,
        _ => 0,
    };
    let mut hasher = blake3::Hasher::new();
    if resumed_bytes > 0 {
        let (path, mut prefix) = (partial.clone(), blake3::Hasher::new());
        hasher = tokio::task::spawn_blocking(move || -> std::io::Result<blake3::Hasher> {
            prefix.update_reader(std::fs::File::open(path)?)?;
            Ok(prefix)
        })
        .await
        .map_err(|error| CliError::Io(std::io::Error::other(error)))??;
    }
    let mut request = client.get_object().bucket(bucket).key(key);
    if resumed_bytes > 0 {
        request = request.range(format!("bytes={resumed_bytes}-"));
    }
    let answer = request.send().await.map_err(s3_error)?;
    let mut file = match resumed_bytes {
        0 => tokio::fs::File::create(&partial).await?,
        _ => {
            tokio::fs::OpenOptions::new()
                .append(true)
                .open(&partial)
                .await?
        }
    };
    let mut body = answer.body;
    let mut written = resumed_bytes;
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
        tokio::fs::remove_file(&partial).await?;
        return Err(CliError::Checksum {
            path: format!("{bucket}/{key}"),
            expected: blake3.unwrap_or("(size only)").to_string(),
            actual: format!("{actual} ({written} bytes)"),
        });
    }
    tokio::fs::rename(&partial, dest).await?;
    Ok(Downloaded {
        size: written,
        blake3: actual,
        resumed_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn names_partial_file() {
        let path = partial_path(Path::new("out/data.csv"));
        assert_eq!(path, Path::new("out/data.csv.aruna-part"));
    }
}
