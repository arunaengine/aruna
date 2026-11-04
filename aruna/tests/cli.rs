//! Runs aruna-cli in process against a node: login, buckets, copies, sync, and RO-Crate metadata.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::shared::{
    self, SeedNode, TestResult, add_user, create_bearer_token, create_group_http,
    create_s3_credentials, s3_client, spawn_complete_seed,
};
use aws_sdk_s3::primitives::ByteStream;
use serde_json::Value;
use std::path::Path;
use std::sync::Once;
use std::time::Duration;

static MOCK_KEYRING: Once = Once::new();

/// Runs one command line with a private config folder and returns its standard output.
async fn cli(config: &Path, args: &[&str]) -> TestResult<String> {
    // An in-memory keyring keyed by API URL, so tests stay apart without an OS keyring.
    MOCK_KEYRING.call_once(|| {
        if let Ok(store) = keyring_core::mock::Store::new() {
            keyring_core::set_default_store(store);
        }
    });
    let config = config.to_string_lossy().to_string();
    let mut line = vec!["aruna-cli", "--config-dir", &config];
    line.extend_from_slice(args);
    let mut out = Vec::new();
    // Names the command but not its arguments, which can hold the token.
    let command = args.iter().find(|arg| !arg.starts_with('-')).copied();
    aruna_cli::run_args(line, &mut out).await.map_err(|error| {
        std::io::Error::other(format!("{} failed: {error}", command.unwrap_or("")))
    })?;
    Ok(String::from_utf8(out)?)
}

async fn cli_json(config: &Path, args: &[&str]) -> TestResult<Value> {
    let mut line = vec!["--json"];
    line.extend_from_slice(args);
    Ok(serde_json::from_str(&cli(config, &line).await?)?)
}

/// A node, a group, its admin token and a signed-in CLI profile in `config`.
async fn signed_in(config: &Path, group: &str) -> TestResult<(SeedNode, String, String)> {
    let seed = spawn_complete_seed().await?;
    let token = create_bearer_token(
        seed.context.as_ref(),
        seed.user_id,
        seed.realm_id,
        seed.capabilities.clone(),
    )
    .await?;
    add_user(seed.context.as_ref(), seed.user_id).await?;
    let group = create_group_http(&seed.base_url, &token, group).await?;
    let login = cli_json(
        config,
        &["--api", &seed.base_url, "login", "--token", &token],
    )
    .await?;
    assert!(
        login["session_id"].is_string(),
        "login made no CLI session: {login}"
    );
    Ok((seed, group.group_id, token))
}

/// The harness S3 server, which the node does not publish.
fn s3_endpoint(seed: &SeedNode) -> TestResult<String> {
    let endpoint = seed.s3.as_ref().ok_or("the node runs no S3 server")?;
    Ok(endpoint.endpoint_url.clone())
}

/// Bytes that differ per position, so a misplaced part changes the digest.
fn pattern(size: usize, seed: u8) -> Vec<u8> {
    (0..size)
        .map(|index| (index as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

#[tokio::test]
async fn data_round_trip() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let config = dir.path().join("config");
    let (seed, group, token) = signed_in(&config, "cli-data").await?;
    let s3 = s3_endpoint(&seed)?;
    let result = async {
        let whoami = cli_json(&config, &["whoami"]).await?;
        assert!(whoami["user"]["user_id"].is_string(), "{whoami}");
        cli(
            &config,
            &[
                "--s3-endpoint",
                &s3,
                "buckets",
                "create",
                "cli-data",
                "--group",
                &group,
            ],
        )
        .await?;
        let buckets = cli_json(&config, &["buckets", "ls"]).await?;
        assert!(
            buckets
                .as_array()
                .into_iter()
                .flatten()
                .any(|hit| hit["bucket"] == "cli-data"),
            "bucket missing from listing: {buckets}"
        );

        // 11 MiB with 5 MiB parts takes the multipart path with three parts.
        let big = pattern(11 * 1024 * 1024, 7);
        // An earlier attempt stored part 1 only; cp must continue that upload.
        let credentials = create_s3_credentials(&seed.base_url, &token, &group).await?;
        let endpoint = seed.s3.as_ref().ok_or("the node runs no S3 server")?;
        let client = s3_client(endpoint, &credentials);
        let started = client
            .create_multipart_upload()
            .bucket("cli-data")
            .key("up/big.bin")
            .send()
            .await?;
        client
            .upload_part()
            .bucket("cli-data")
            .key("up/big.bin")
            .upload_id(started.upload_id().unwrap_or_default())
            .part_number(1)
            .body(ByteStream::from(big[..5 * 1024 * 1024].to_vec()))
            .send()
            .await?;
        let local = dir.path().join("big.bin");
        std::fs::write(&local, &big)?;
        let local_text = local.to_string_lossy().to_string();
        let up = cli_json(
            &config,
            &[
                "--s3-endpoint",
                &s3,
                "cp",
                &local_text,
                "cli-data/up/",
                "--part-size",
                "5",
            ],
        )
        .await?;
        assert_eq!(up["key"], "up/big.bin");
        assert_eq!(up["verified"], true, "upload not checked by BLAKE3: {up}");
        assert_eq!(up["resumed"], 1, "the stored part was not reused: {up}");

        let listing = cli_json(&config, &["ls", "cli-data/up"]).await?;
        let keys: Vec<&str> = listing["objects"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|entry| entry["key"].as_str())
            .collect();
        assert_eq!(keys, vec!["up/big.bin"]);

        let back = dir.path().join("back.bin");
        let back_text = back.to_string_lossy().to_string();
        let down = cli_json(
            &config,
            &[
                "--s3-endpoint",
                &s3,
                "cp",
                "cli-data/up/big.bin",
                &back_text,
            ],
        )
        .await?;
        assert_eq!(down["verified"], true, "{down}");
        assert_eq!(std::fs::read(&back)?, big);

        let tree = dir.path().join("tree");
        std::fs::create_dir_all(tree.join("nested"))?;
        std::fs::write(tree.join("a.txt"), b"first")?;
        std::fs::write(tree.join("nested/b.txt"), b"second")?;
        let tree_text = tree.to_string_lossy().to_string();
        let sync_up = ["--s3-endpoint", &s3, "sync", &tree_text, "cli-data/tree"];
        let planned = cli_json(&config, &[&sync_up[..], &["--dry-run"]].concat()).await?;
        assert_eq!(
            planned["plan"]["transfers"].as_array().map(Vec::len),
            Some(2)
        );
        cli_json(&config, &sync_up).await?;
        let again = cli_json(&config, &[&sync_up[..], &["--dry-run"]].concat()).await?;
        assert_eq!(
            again["plan"]["transfers"].as_array().map(Vec::len),
            Some(0),
            "{again}"
        );
        assert_eq!(again["plan"]["same"], 2, "{again}");

        // Same size, new content: only BLAKE3 can tell.
        std::fs::write(tree.join("a.txt"), b"FIRST")?;
        let changed = cli_json(&config, &[&sync_up[..], &["--dry-run"]].concat()).await?;
        assert_eq!(changed["plan"]["transfers"][0]["path"], "a.txt");
        assert_eq!(changed["plan"]["transfers"][0]["reason"], "checksum");
        cli_json(&config, &sync_up).await?;

        let copy = dir.path().join("copy");
        let copy_text = copy.to_string_lossy().to_string();
        cli_json(
            &config,
            &["--s3-endpoint", &s3, "sync", "cli-data/tree", &copy_text],
        )
        .await?;
        assert_eq!(std::fs::read(copy.join("a.txt"))?, b"FIRST");
        assert_eq!(std::fs::read(copy.join("nested/b.txt"))?, b"second");

        cli(&config, &["logout"]).await?;
        assert!(
            cli(&config, &["whoami"]).await.is_err(),
            "whoami works after logout"
        );
        Ok(())
    }
    .await;
    seed.shutdown().await;
    result
}

#[tokio::test]
async fn revokes_unusable_session() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let config = dir.path().join("config");
    let (seed, group, token) = signed_in(&config, "cli-revoke").await?;
    let result = async {
        // Without --s3-endpoint the session has no endpoint, so the CLI must revoke it.
        let error = cli(&config, &["s3", "env", "--group", &group])
            .await
            .err()
            .ok_or("s3 env worked without an S3 endpoint")?;
        assert!(error.to_string().contains("s3 failed"), "{error}");
        let listed: Value = reqwest::Client::new()
            .get(format!("{}/api/v1/access/s3/sessions", seed.base_url))
            .bearer_auth(&token)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(listed["sessions"], serde_json::json!([]), "{listed}");
        Ok(())
    }
    .await;
    seed.shutdown().await;
    result
}

#[tokio::test]
async fn metadata_round_trip() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let config = dir.path().join("config");
    let (seed, group, _) = signed_in(&config, "cli-meta").await?;
    let result = async {
        let rocrate = serde_json::json!({
            "@context": "https://w3id.org/ro/crate/1.2/context",
            "@graph": [
                {"@id": "ro-crate-metadata.json", "@type": "CreativeWork",
                 "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"}, "about": {"@id": "./"}},
                {"@id": "./", "@type": "Dataset", "name": "Quokka field notes",
                 "description": "Counts of quokkas from the zyxwquokka survey",
                 "datePublished": "2026-11-01",
                 "license": {"@id": "https://creativecommons.org/licenses/by/4.0/"}}
            ]
        });
        let folder = dir.path().join("crate");
        std::fs::create_dir_all(&folder)?;
        std::fs::write(folder.join("ro-crate-metadata.json"), rocrate.to_string())?;
        let folder_text = folder.to_string_lossy().to_string();

        let verdict = cli_json(
            &config,
            &["meta", "validate", &folder_text, "--group", &group],
        )
        .await?;
        assert_eq!(verdict["accepted"], true, "{verdict}");
        let created = cli_json(
            &config,
            &["meta", "create", &folder_text, "--group", &group],
        )
        .await?;
        let id = created["document_id"]
            .as_str()
            .ok_or_else(|| std::io::Error::other(format!("no document id: {created}")))?
            .to_string();
        assert_eq!(created["document_path"], "datasets/quokka-field-notes");

        shared::wait_until(
            "the new crate in search",
            shared::WAIT_CAP,
            Duration::from_millis(100),
            || async {
                cli_json(&config, &["meta", "search", "zyxwquokka"])
                    .await
                    .is_ok_and(|page| {
                        page["hits"]
                            .as_array()
                            .into_iter()
                            .flatten()
                            .any(|hit| hit["document_id"] == id.as_str())
                    })
            },
        )
        .await?;

        let output = dir.path().join("export.json");
        let output_text = output.to_string_lossy().to_string();
        cli(&config, &["meta", "export", &id, "-o", &output_text]).await?;
        let exported: Value = serde_json::from_slice(&std::fs::read(&output)?)?;
        let names: Vec<&str> = exported["@graph"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|entity| entity["name"].as_str())
            .collect();
        assert!(names.contains(&"Quokka field notes"), "{exported}");

        let listed = cli_json(&config, &["meta", "ls", "--group", &group]).await?;
        assert!(
            listed["documents"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|document| document["document_id"] == id.as_str()),
            "{listed}"
        );
        Ok(())
    }
    .await;
    seed.shutdown().await;
    result
}
