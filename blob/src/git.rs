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
            GitEffect::Initialize(id) => *id,
            GitEffect::Http(request) => request.repository.document_id,
        };
        let _lock = self.locks[id.to_bytes()[15] as usize % 64].lock().await;
        let repository = self.root.join(format!("{id}.git"));
        match effect {
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

async fn exchange(mut process: Command, body: Bytes, grouped: bool) -> std::io::Result<Bytes> {
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
        let send = async {
            stdin.write_all(&body).await?;
            stdin.shutdown().await
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
