//! Executes Git's native HTTP backend in node-local bare repositories.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::git::{GitEffect, GitEvent, MAX_GIT_BYTES};
use bytes::Bytes;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::{Mutex, Semaphore};

#[derive(Debug)]
pub struct GitStore {
    root: PathBuf,
    helper: PathBuf,
    locks: [Mutex<()>; 64],
    slots: Semaphore,
}

impl GitStore {
    pub fn new(root: PathBuf, helper: PathBuf) -> Self {
        Self {
            root,
            helper,
            locks: std::array::from_fn(|_| Mutex::new(())),
            slots: Semaphore::new(2),
        }
    }

    pub async fn execute(
        &self,
        effect: GitEffect,
        actor: aruna_core::UserId,
    ) -> std::io::Result<GitEvent> {
        let _slot = self.slots.try_acquire().map_err(std::io::Error::other)?;
        let id = match &effect {
            GitEffect::Initialize(id) | GitEffect::Refs(id) | GitEffect::Imported(id) => *id,
            GitEffect::Generate { snapshot, .. } | GitEffect::Edit { snapshot, .. } => {
                snapshot.document_id
            }
            GitEffect::Import { document_id, .. }
            | GitEffect::Resolve { document_id, .. }
            | GitEffect::MergeBase { document_id, .. }
            | GitEffect::Log { document_id, .. }
            | GitEffect::Diff { document_id, .. }
            | GitEffect::Merge { document_id, .. }
            | GitEffect::MergeMetadata { document_id, .. }
            | GitEffect::Ancestry { document_id, .. }
            | GitEffect::SetRefs { document_id, .. }
            | GitEffect::Pack { document_id, .. }
            | GitEffect::Export { document_id, .. } => *document_id,
            GitEffect::Http(request) => request.repository.document_id,
        };
        let _lock = self.locks[id.to_bytes()[15] as usize % 64].lock().await;
        let repository = self.root.join(format!("{id}.git"));
        match effect {
            GitEffect::Generate { snapshot, refs } => Ok(
                match crate::arc::generate(&repository, snapshot, &refs).await? {
                    Ok((aruna, main)) => GitEvent::Generated { aruna, main },
                    Err(error) => GitEvent::GenerateFailed(error),
                },
            ),
            GitEffect::Resolve { revision, .. } => Ok(GitEvent::Resolved(
                crate::repo::resolve(&repository, &revision).await,
            )),
            GitEffect::MergeBase { first, second, .. } => Ok(GitEvent::Resolved(
                crate::repo::merge_base(&repository, &first, &second).await,
            )),
            GitEffect::Log {
                revision,
                skip,
                limit,
                ..
            } => crate::repo::log(&repository, &revision, skip, limit)
                .await
                .map(GitEvent::Log),
            GitEffect::Diff { from, to, .. } => {
                crate::repo::diff(&repository, from.as_deref(), &to)
                    .await
                    .map(GitEvent::Diff)
            }
            GitEffect::Edit {
                head,
                snapshot,
                message,
            } => crate::arc::edit(&repository, &head, snapshot, message)
                .await
                .map(GitEvent::Edited),
            GitEffect::Merge {
                target,
                source,
                message,
                ..
            } => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(std::io::Error::other)?;
                let occurred_at_ms =
                    u64::try_from(now.as_millis()).map_err(std::io::Error::other)?;
                crate::arc::merge(&repository, id, &target, &source, message, occurred_at_ms)
                    .await
                    .map(GitEvent::Merged)
            }
            GitEffect::MergeMetadata {
                old, new, graph, ..
            } => crate::arc::merge_metadata(&repository, old.as_deref(), &new, &graph)
                .await
                .map(GitEvent::MetadataMerged),
            GitEffect::Imported(_) => crate::repo::imported(&repository)
                .await
                .map(GitEvent::Imported),
            GitEffect::Import { digest, pack, .. } => {
                crate::repo::import(&repository, &digest, pack)
                    .await
                    .map(|_| GitEvent::Initialized)
            }
            GitEffect::Refs(_) => crate::repo::refs(&repository).await.map(GitEvent::Refs),
            GitEffect::Ancestry { pairs, .. } => Ok(GitEvent::Ancestry(
                crate::repo::ancestry(&repository, &pairs).await,
            )),
            GitEffect::SetRefs {
                expected, target, ..
            } => crate::repo::set_refs(&repository, &expected, &target)
                .await
                .map(|_| GitEvent::Refs(target)),
            GitEffect::Pack {
                include, exclude, ..
            } => crate::repo::pack(&repository, &include, &exclude)
                .await
                .map(GitEvent::Packed),
            GitEffect::Export { revision, .. } => {
                let result = crate::arc::export(&repository, &revision).await?;
                Ok(GitEvent::Exported(
                    serde_json::to_vec(&result)
                        .map_err(std::io::Error::other)?
                        .into(),
                ))
            }
            GitEffect::Initialize(_) => {
                tokio::fs::create_dir_all(&self.root).await?;
                let root = tokio::fs::canonicalize(&self.root).await?;
                command(
                    &root,
                    &[
                        "init",
                        "--bare",
                        "--object-format=sha1",
                        "--initial-branch=main",
                        &format!("{id}.git"),
                    ],
                )
                .await?;
                tokio::fs::create_dir_all(repository.join("hooks")).await?;
                let hooks = tokio::fs::canonicalize(repository.join("hooks")).await?;
                let hook = hooks.join("pre-receive");
                #[cfg(unix)]
                {
                    if tokio::fs::symlink_metadata(&hook).await.is_ok() {
                        if tokio::fs::read_link(&hook).await? != self.helper {
                            return Err(std::io::Error::other(
                                "repository has a different receive hook",
                            ));
                        }
                    } else {
                        tokio::fs::symlink(&self.helper, &hook).await?;
                    }
                }
                #[cfg(not(unix))]
                return Err(std::io::Error::other("native Git hosting requires Unix"));
                for (key, value) in [
                    (
                        "core.hooksPath",
                        hooks
                            .to_str()
                            .ok_or_else(|| std::io::Error::other("invalid hook path"))?,
                    ),
                    ("http.receivepack", "true"),
                    ("http.getanyfile", "false"),
                    ("receive.fsckObjects", "true"),
                    ("receive.denyNonFastForwards", "true"),
                    ("core.logAllRefUpdates", "true"),
                    ("core.fsync", "committed"),
                ] {
                    command(&repository, &["config", key, value]).await?;
                }
                tokio::fs::write(repository.join("git-daemon-export-ok"), []).await?;
                Ok(GitEvent::Initialized)
            }
            GitEffect::Http(request) => {
                let mut process = Command::new("git");
                process.arg("http-backend");
                for (key, _) in
                    std::env::vars_os().filter(|(key, _)| key.to_string_lossy().starts_with("GIT_"))
                {
                    process.env_remove(key);
                }
                process
                    .env(
                        "GIT_PROJECT_ROOT",
                        tokio::fs::canonicalize(&self.root).await?,
                    )
                    .env("PATH_INFO", format!("/{id}.git/{}", request.action))
                    .env("REQUEST_METHOD", request.method)
                    .env("QUERY_STRING", request.query)
                    .env("CONTENT_TYPE", request.content_type)
                    .env("HTTP_CONTENT_ENCODING", request.content_encoding)
                    .env("CONTENT_LENGTH", request.body.len().to_string())
                    .env("GIT_PROTOCOL", request.protocol)
                    .env("REMOTE_USER", actor.to_string())
                    .env("ARUNA_GIT_HELPER", &self.helper)
                    .env("ARUNA_GIT_TOKEN", request.token)
                    .env("ARUNA_GIT_LFS_URL", request.lfs_url)
                    .env("ARUNA_GIT_METADATA_URL", request.metadata_url)
                    .env(
                        "ARUNA_GIT_ARC",
                        if request.repository.arc { "1" } else { "0" },
                    );
                let output = exchange(process, request.body, true).await?;
                let boundary = output
                    .windows(4)
                    .position(|value| value == b"\r\n\r\n")
                    .ok_or_else(|| std::io::Error::other("invalid Git HTTP response"))?;
                let text =
                    std::str::from_utf8(&output[..boundary]).map_err(std::io::Error::other)?;
                let mut status = 200;
                let mut headers = Vec::new();
                for line in text.lines() {
                    let (key, value) = line
                        .split_once(':')
                        .ok_or_else(|| std::io::Error::other("invalid Git header"))?;
                    if key.eq_ignore_ascii_case("status") {
                        status = value
                            .trim()
                            .split(' ')
                            .next()
                            .unwrap_or("")
                            .parse()
                            .map_err(std::io::Error::other)?;
                    } else {
                        headers.push((key.to_string(), value.trim().to_string()));
                    }
                }
                Ok(GitEvent::Response {
                    status,
                    headers,
                    body: output.slice(boundary + 4..),
                })
            }
        }
    }
}

pub async fn command(directory: &Path, args: &[&str]) -> std::io::Result<Bytes> {
    let mut command = Command::new("git");
    command.current_dir(directory).args(args);
    exchange(command, Bytes::new(), false).await
}

pub async fn lfs_exchange(url: &str, token: &str, body: Vec<u8>) -> std::io::Result<Bytes> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(std::io::Error::other)?
        .post(url)
        .bearer_auth(token)
        .header("Content-Type", "application/vnd.git-lfs+json")
        .body(body)
        .send()
        .await
        .map_err(std::io::Error::other)?
        .error_for_status()
        .map_err(std::io::Error::other)?
        .bytes()
        .await
        .map_err(std::io::Error::other)
}

/// Calls the node's own metadata API as the pushing user: `GET` without a body, otherwise
/// `PUT` for JSON or `POST` for other content.
pub async fn metadata_request(
    url: &str,
    token: &str,
    body: Option<(&str, Vec<u8>)>,
) -> std::io::Result<Bytes> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(300))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(std::io::Error::other)?;
    let request = match body {
        Some(("application/json", body)) => client
            .put(url)
            .header("Content-Type", "application/json")
            .body(body),
        Some((content_type, body)) => client
            .post(url)
            .header("Content-Type", content_type)
            .body(body),
        None => client.get(url),
    };
    let response = request
        .bearer_auth(token)
        .send()
        .await
        .map_err(std::io::Error::other)?;
    let status = response.status();
    let bytes = response.bytes().await.map_err(std::io::Error::other)?;
    if !status.is_success() {
        let detail = String::from_utf8_lossy(&bytes[..bytes.len().min(1000)]).into_owned();
        return Err(std::io::Error::other(format!(
            "metadata update refused with {status}: {detail}"
        )));
    }
    Ok(bytes)
}

#[cfg(unix)]
struct ProcessGroup(Option<rustix::process::Pid>);

#[cfg(unix)]
impl Drop for ProcessGroup {
    fn drop(&mut self) {
        if let Some(pid) = self.0 {
            let _ = rustix::process::kill_process_group(pid, rustix::process::Signal::KILL);
        }
    }
}

pub(crate) async fn exchange(
    mut process: Command,
    body: Bytes,
    grouped: bool,
) -> std::io::Result<Bytes> {
    #[cfg(unix)]
    if grouped {
        process.process_group(0);
    }
    let mut child = process
        .kill_on_drop(true)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    #[cfg(unix)]
    let _group = ProcessGroup(if grouped {
        child
            .id()
            .and_then(|pid| i32::try_from(pid).ok())
            .and_then(rustix::process::Pid::from_raw)
    } else {
        None
    });
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| std::io::Error::other("Git stdin missing"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| std::io::Error::other("Git stdout missing"))?;
    tokio::time::timeout(Duration::from_secs(300), async {
        let send = async move {
            stdin.write_all(&body).await?;
            stdin.shutdown().await?;
            drop(stdin);
            Ok::<_, std::io::Error>(())
        };
        let receive = async {
            let mut bytes = Vec::new();
            stdout
                .take(MAX_GIT_BYTES as u64 + 1)
                .read_to_end(&mut bytes)
                .await?;
            if bytes.len() > MAX_GIT_BYTES {
                return Err(std::io::Error::other("Git response exceeds limit"));
            }
            Ok::<_, std::io::Error>(bytes)
        };
        let (_, bytes) = tokio::try_join!(send, receive)?;
        if !child.wait().await?.success() {
            return Err(std::io::Error::other("Git backend failed"));
        }
        Ok(Bytes::from(bytes))
    })
    .await
    .map_err(std::io::Error::other)?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stdin_closes() {
        let mut process = Command::new("git");
        process.args(["hash-object", "--stdin"]);
        let result = exchange(process, Bytes::from_static(b"test content\n"), false)
            .await
            .expect("EOF-dependent Git command completes");
        assert_eq!(
            result.as_ref(),
            b"d670460b4b4aece5915caf5c68d12f560a9fe3e4\n"
        );
    }
}
