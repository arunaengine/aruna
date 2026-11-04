//! Lists and creates buckets, lists objects, searches the realm, shares paths and prints S3 access.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{Context, resolve_group};
use crate::args::{BucketsCommand, S3Command, ShareArgs, ShareMode};
use crate::client::Api;
use crate::error::{CliError, s3_error};
use crate::output::human_size;
use crate::remote::{self, BucketHit, BucketPage, RemotePath};
use crate::transfer::{self, S3_REGION};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub async fn buckets(context: &mut Context<'_>, command: BucketsCommand) -> Result<(), CliError> {
    let api = context.api()?;
    match command {
        BucketsCommand::Ls { group } => {
            let mut query = vec![("q", String::new()), ("limit", "50".to_string())];
            if let Some(group) = group {
                query.push(("group_id", resolve_group(&api, &group).await?));
            }
            let mut hits: Vec<BucketHit> = Vec::new();
            let mut failed = 0;
            loop {
                let page: BucketPage = api.get("search/buckets", &query).await?;
                failed = failed.max(page.nodes_failed);
                hits.extend(page.hits);
                match page.next_cursor {
                    Some(cursor) => {
                        query.retain(|(name, _)| *name != "cursor");
                        query.push(("cursor", cursor));
                    }
                    None => break,
                }
            }
            if failed > 0 {
                eprintln!("warning: {failed} nodes did not answer; their buckets are missing");
            }
            let rows = hits
                .iter()
                .map(|hit| {
                    vec![
                        hit.bucket.clone(),
                        hit.group_name
                            .clone()
                            .unwrap_or_else(|| hit.group_id.clone()),
                        hit.node_id.clone(),
                        match &hit.set_id {
                            Some(_) => "distributed".to_string(),
                            None => String::new(),
                        },
                        hit.created_at.clone(),
                    ]
                })
                .collect();
            context
                .out
                .listing(&hits, &["BUCKET", "GROUP", "NODE", "KIND", "CREATED"], rows)
        }
        BucketsCommand::Create { name, group } => {
            let group_id = resolve_group(&api, &group).await?;
            let node_api = match &context.node {
                Some(node) => api.for_node(node).await?,
                None => api,
            };
            let session =
                transfer::open_session(&node_api, &group_id, context.s3_endpoint.as_deref())
                    .await?;
            let created = transfer::s3_client(&session)
                .create_bucket()
                .bucket(&name)
                .send()
                .await
                .map_err(s3_error);
            transfer::revoke_session(&node_api, &session).await;
            created?;
            let node = session.issuer_node.node_id;
            match context.out.json {
                true => context.out.json(&serde_json::json!({
                    "bucket": name, "group_id": group_id, "node_id": node,
                })),
                false => context
                    .out
                    .line(format!("Created bucket {name} on node {node}.")),
            }
        }
    }
}

#[derive(Serialize)]
struct Listing<'a> {
    bucket: &'a str,
    node_id: &'a str,
    prefixes: Vec<String>,
    objects: Vec<remote::ObjectEntry>,
}

pub async fn list(context: &mut Context<'_>, path: &str, recursive: bool) -> Result<(), CliError> {
    let api = context.api()?;
    let target = RemotePath::parse(path)?;
    let (placement, node_api) =
        remote::locate(&api, &target.bucket, context.node.as_deref()).await?;
    // A key without a slash may still be a folder, so it is listed as a prefix.
    let prefix = match target.key.is_empty() || target.key.ends_with('/') {
        true => target.key.clone(),
        false => format!("{}/", target.key),
    };
    let mut listing = Listing {
        bucket: &target.bucket,
        node_id: &placement.node_id,
        prefixes: Vec::new(),
        objects: Vec::new(),
    };
    let mut cursor = None;
    loop {
        let page =
            remote::list_page(&node_api, &target.bucket, &prefix, !recursive, cursor).await?;
        listing.prefixes.extend(page.prefixes);
        listing.objects.extend(page.objects);
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    if listing.objects.is_empty() && listing.prefixes.is_empty() && !target.key.is_empty() {
        // The path may name one object rather than a folder.
        if let Some(entry) = remote::find_object(&node_api, &target.bucket, &target.key).await? {
            listing.objects.push(entry);
        }
    }
    let mut rows: Vec<Vec<String>> = listing
        .prefixes
        .iter()
        .map(|prefix| vec!["PRE".into(), String::new(), String::new(), prefix.clone()])
        .collect();
    rows.extend(listing.objects.iter().map(|entry| {
        vec![
            entry.size.map(human_size).unwrap_or_default(),
            entry.last_modified.clone().unwrap_or_default(),
            entry
                .blake3()
                .map(|digest| digest[..12.min(digest.len())].to_string())
                .unwrap_or_default(),
            entry.key.clone(),
        ]
    }));
    context
        .out
        .listing(&listing, &["SIZE", "MODIFIED", "BLAKE3", "KEY"], rows)
}

pub async fn search(
    context: &mut Context<'_>,
    query: &str,
    types: Option<String>,
    limit: usize,
) -> Result<(), CliError> {
    let api = context.api()?;
    let mut params = vec![("q", query.to_string()), ("limit", limit.to_string())];
    if let Some(types) = types {
        params.push(("types", types));
    }
    let answer: Value = api.get("search", &params).await?;
    if context.out.json {
        return context.out.json(&answer);
    }
    let field = |value: &Value, name: &str| value[name].as_str().unwrap_or_default().to_string();
    let mut rows = Vec::new();
    let sections = [
        ("documents", "dataset", "title", "document_id"),
        ("buckets", "bucket", "bucket", "node_id"),
        ("groups", "group", "display_name", "group_id"),
        ("users", "user", "name", "user_id"),
    ];
    for (section, kind, name, id) in sections {
        let part = &answer[section];
        for hit in part["hits"].as_array().into_iter().flatten() {
            rows.push(vec![kind.to_string(), field(hit, name), field(hit, id)]);
        }
        if let Some(failed) = part["nodes_failed"].as_u64().filter(|failed| *failed > 0) {
            eprintln!("warning: {failed} nodes did not answer for {section}; hits may be missing");
        }
    }
    context.out.listing(&answer, &["TYPE", "NAME", "ID"], rows)
}

pub async fn s3_env(context: &mut Context<'_>, command: S3Command) -> Result<(), CliError> {
    let S3Command::Env { group, show_secret } = command;
    let api = context.api()?;
    let group_id = resolve_group(&api, &group).await?;
    let node_api = match &context.node {
        Some(node) => api.for_node(node).await?,
        None => api,
    };
    let session =
        transfer::open_session(&node_api, &group_id, context.s3_endpoint.as_deref()).await?;
    if show_secret {
        eprintln!(
            "warning: anyone who sees these values can use your access until {}",
            session.expires_at
        );
    }
    if context.out.json {
        let mut value = serde_json::json!({
            "endpoint": session.endpoint,
            "region": S3_REGION,
            "access_key_id": session.access_key_id,
            "expires_at": session.expires_at,
            "node_id": session.issuer_node.node_id,
            "group_id": group_id,
        });
        if show_secret {
            value["secret_access_key"] = session.secret_access_key.clone().into();
            value["session_token"] = session.session_token.clone().into();
        }
        return context.out.json(&value);
    }
    let lines = [
        format!(
            "# S3 session of group {group_id} on node {}",
            session.issuer_node.node_id
        ),
        format!(
            "# valid until {}; path-style addressing",
            session.expires_at
        ),
        format!("export AWS_ENDPOINT_URL={}", session.endpoint),
        format!("export AWS_DEFAULT_REGION={S3_REGION}"),
        format!("export AWS_ACCESS_KEY_ID={}", session.access_key_id),
    ];
    for line in lines {
        context.out.line(line)?;
    }
    match show_secret {
        true => {
            context.out.line(format!(
                "export AWS_SECRET_ACCESS_KEY={}",
                session.secret_access_key
            ))?;
            context.out.line(format!(
                "export AWS_SESSION_TOKEN={}",
                session.session_token
            ))?;
        }
        false => context
            .out
            .line("# secret key and session token hidden; add --show-secret")?,
    }
    // rclone reads the AWS variables when env_auth is on.
    for line in [
        "export RCLONE_CONFIG_ARUNA_TYPE=s3".to_string(),
        "export RCLONE_CONFIG_ARUNA_PROVIDER=Other".to_string(),
        "export RCLONE_CONFIG_ARUNA_ENV_AUTH=true".to_string(),
        format!("export RCLONE_CONFIG_ARUNA_ENDPOINT={}", session.endpoint),
    ] {
        context.out.line(line)?;
    }
    Ok(())
}

#[derive(Debug, Deserialize, Serialize)]
struct ShareDetails {
    share: Value,
    token: String,
}

pub async fn share(context: &mut Context<'_>, args: ShareArgs) -> Result<(), CliError> {
    let profile = context.profile()?;
    let api = context.api()?;
    let target = RemotePath::parse(&args.path)?;
    let (placement, node_api) =
        remote::locate(&api, &target.bucket, context.node.as_deref()).await?;
    if placement.set_id.is_some() && args.mode != ShareMode::Read {
        return Err(CliError::routing(format!(
            "bucket {} is distributed; upload links are not supported for it yet",
            target.bucket
        )));
    }
    let share_target = if target.key.is_empty() {
        serde_json::json!({ "kind": "bucket", "bucket": target.bucket })
    } else if args.folder || target.key.ends_with('/') {
        serde_json::json!({ "kind": "folder", "bucket": target.bucket, "prefix": target.folder() })
    } else {
        serde_json::json!({ "kind": "object", "bucket": target.bucket, "key": target.key })
    };
    let mode = match args.mode {
        ShareMode::Read => "read",
        ShareMode::ReadUpload => "read_upload",
        ShareMode::Upload => "upload",
    };
    let mut body = serde_json::json!({ "target": share_target, "mode": mode, "label": args.label });
    if let Some(expires) = &args.expires {
        body["expires_at"] = parse_expiry(expires, chrono::Utc::now())?.into();
    }
    if let Some(password) = &args.password {
        body["password"] = password.clone().into();
    }
    let created: ShareDetails = share_request(&node_api, &body).await?;
    let link = format!(
        "{}/s/{}#{}",
        profile.website.trim_end_matches('/'),
        placement.node_id,
        created.token
    );
    match context.out.json {
        true => context
            .out
            .json(&serde_json::json!({ "share": created.share, "url": link })),
        false => context.out.line(link),
    }
}

async fn share_request(api: &Api, body: &Value) -> Result<ShareDetails, CliError> {
    api.send(Method::POST, "shares", body).await
}

/// An RFC 3339 time, or a lifetime such as `7d`, `12h` or `30m` from `now`.
pub fn parse_expiry(raw: &str, now: chrono::DateTime<chrono::Utc>) -> Result<String, CliError> {
    let invalid = || {
        CliError::usage(format!(
            "{raw} is neither a lifetime like 7d nor an RFC 3339 time"
        ))
    };
    if let Ok(time) = chrono::DateTime::parse_from_rfc3339(raw) {
        return Ok(time
            .with_timezone(&chrono::Utc)
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true));
    }
    let (number, unit) = raw.split_at(raw.len().saturating_sub(1));
    let amount: i64 = number.parse().map_err(|_| invalid())?;
    let span = match unit {
        "d" => chrono::Duration::try_days(amount),
        "h" => chrono::Duration::try_hours(amount),
        "m" => chrono::Duration::try_minutes(amount),
        _ => None,
    }
    .filter(|span| *span > chrono::Duration::zero())
    .ok_or_else(invalid)?;
    let time = now.checked_add_signed(span).ok_or_else(invalid)?;
    Ok(time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_share_expiry() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-11-01T10:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(parse_expiry("7d", now).unwrap(), "2026-11-08T10:00:00Z");
        assert_eq!(parse_expiry("30m", now).unwrap(), "2026-11-01T10:30:00Z");
        assert_eq!(
            parse_expiry("2026-12-01T00:00:00+01:00", now).unwrap(),
            "2026-11-30T23:00:00Z"
        );
        for bad in ["", "7", "0d", "-1h", "7w", "soon"] {
            assert!(parse_expiry(bad, now).is_err(), "{bad}");
        }
    }
}
