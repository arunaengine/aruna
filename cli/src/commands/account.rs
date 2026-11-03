//! Signs in with a token, creates the CLI's own API session, signs out and shows the identity.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::Context;
use crate::args::LoginArgs;
use crate::client::Api;
use crate::config::Profile;
use crate::error::CliError;
use base64::Engine;
use reqwest::Method;
use serde::{Deserialize, Serialize};
use std::io::BufRead;

#[derive(Debug, Deserialize)]
struct NodeInfo {
    node: NodeStatus,
}

#[derive(Debug, Deserialize)]
struct NodeStatus {
    realm_id: String,
    peer_id: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct UserInfo {
    user: User,
    #[serde(default)]
    groups: Vec<UserGroup>,
}

#[derive(Debug, Deserialize, Serialize)]
struct User {
    user_id: String,
    name: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct UserGroup {
    group_id: String,
    display_name: String,
    #[serde(default)]
    roles: Vec<Role>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Role {
    name: String,
}

#[derive(Debug, Deserialize)]
struct CreatedSession {
    session_id: String,
    token: String,
    expires_at: String,
}

#[derive(Debug, Serialize)]
struct LoginSummary<'a> {
    user_id: &'a str,
    user_name: &'a str,
    node_id: Option<&'a str>,
    realm_id: Option<&'a str>,
    session_id: Option<&'a str>,
    expires_at: Option<&'a str>,
}

pub async fn login(context: &mut Context<'_>, args: LoginArgs) -> Result<(), CliError> {
    let previous = context.config.load_profile()?;
    let base = context
        .api_flag
        .clone()
        .or_else(|| previous.as_ref().map(|profile| profile.api.clone()))
        .ok_or_else(|| CliError::usage("pass the node's address with --api <url>"))?;
    let given = match args.token {
        Some(token) => token,
        None => read_token()?,
    };
    let given = given.trim().to_string();
    if given.is_empty() {
        return Err(CliError::usage("the token is empty"));
    }
    let api = Api::new(&base, Some(given.clone()))?;
    let info: NodeInfo = api.get("system/info", &[]).await?;
    let me: UserInfo = api.get("access/users/me", &[]).await?;
    let label = format!("cli@{}", host_name());
    let body = serde_json::json!({ "kind": "api", "label": label });
    // A session of its own lets the portal list the CLI and logout revoke it.
    let (token, session_id, expires_at) = match api
        .send::<CreatedSession>(Method::POST, "access/sessions", &body)
        .await
    {
        Ok(created) => (
            created.token,
            Some(created.session_id),
            Some(created.expires_at),
        ),
        Err(CliError::Api(error)) if matches!(error.status, 403 | 409) => {
            eprintln!("note: kept the given token, as the node made no CLI session: {error}");
            let expiry = token_expiry(&given);
            (given, None, expiry)
        }
        Err(error) => return Err(error),
    };
    if let Some(previous) = previous {
        revoke_previous(&context.config, &previous).await;
        context.config.forget(&previous)?;
    }
    if let Err(error) = context.config.save_token(api.base(), &token) {
        if let Some(session_id) = &session_id {
            let _ = api.delete(&format!("access/sessions/{session_id}")).await;
        }
        return Err(error);
    }
    let website = match args.website {
        Some(website) => website.trim_end_matches('/').to_string(),
        None => origin(api.base()),
    };
    let profile = Profile {
        api: api.base().to_string(),
        website,
        node_id: info.node.peer_id,
        realm_id: Some(info.node.realm_id),
        user_id: Some(me.user.user_id.clone()),
        user_name: Some(me.user.name.clone()),
        session_id,
        expires_at,
    };
    context.config.save_profile(&profile)?;
    let summary = LoginSummary {
        user_id: &me.user.user_id,
        user_name: &me.user.name,
        node_id: profile.node_id.as_deref(),
        realm_id: profile.realm_id.as_deref(),
        session_id: profile.session_id.as_deref(),
        expires_at: profile.expires_at.as_deref(),
    };
    if context.out.json {
        return context.out.json(&summary);
    }
    context.out.line(format!(
        "Signed in as {} ({}) on {}.",
        me.user.name, me.user.user_id, profile.api
    ))?;
    context.out.line(format!(
        "Session {} expires at {}; the token is in the OS keyring.",
        profile.session_id.as_deref().unwrap_or("(given token)"),
        profile.expires_at.as_deref().unwrap_or("an unknown time"),
    ))
}

pub async fn logout(context: &mut Context<'_>) -> Result<(), CliError> {
    let Some(profile) = context.config.load_profile()? else {
        return context.out.line("Not signed in.");
    };
    let revoked = revoke_previous(&context.config, &profile).await;
    context.config.forget(&profile)?;
    match revoked {
        true => context.out.line("Signed out; the CLI session is revoked."),
        false => context.out.line("Signed out; the token is forgotten."),
    }
}

pub async fn whoami(context: &mut Context<'_>) -> Result<(), CliError> {
    let profile = context.profile()?;
    let api = context.api()?;
    let me: UserInfo = api.get("access/users/me", &[]).await?;
    if context.out.json {
        let value = serde_json::json!({
            "user": me.user,
            "groups": me.groups,
            "api": profile.api,
            "node_id": profile.node_id,
            "realm_id": profile.realm_id,
            "session_id": profile.session_id,
            "expires_at": profile.expires_at,
        });
        return context.out.json(&value);
    }
    let unknown = || "-".to_string();
    let rows = vec![
        vec![
            "User".into(),
            format!("{} ({})", me.user.name, me.user.user_id),
        ],
        vec!["API".into(), profile.api.clone()],
        vec![
            "Node".into(),
            profile.node_id.clone().unwrap_or_else(unknown),
        ],
        vec![
            "Realm".into(),
            profile.realm_id.clone().unwrap_or_else(unknown),
        ],
        vec![
            "Session".into(),
            profile.session_id.clone().unwrap_or_else(unknown),
        ],
        vec![
            "Expires".into(),
            profile.expires_at.clone().unwrap_or_else(unknown),
        ],
    ];
    context.out.table(&["", ""], &rows)?;
    for group in &me.groups {
        let roles: Vec<&str> = group.roles.iter().map(|role| role.name.as_str()).collect();
        context.out.line(format!(
            "Group {} ({}): {}",
            group.display_name,
            group.group_id,
            roles.join(", ")
        ))?;
    }
    Ok(())
}

/// Revokes the stored CLI session; failures only warn, as the token is forgotten anyway.
async fn revoke_previous(config: &crate::config::Config, profile: &Profile) -> bool {
    let Some(session_id) = &profile.session_id else {
        return false;
    };
    let revoked = async {
        let api = Api::new(&profile.api, Some(config.load_token(profile)?))?;
        api.delete(&format!("access/sessions/{session_id}")).await
    }
    .await;
    match revoked {
        Ok(()) => true,
        Err(error) => {
            eprintln!("warning: the old CLI session was not revoked: {error}");
            false
        }
    }
}

fn read_token() -> Result<String, CliError> {
    eprintln!("Paste a token from the portal (Settings, API sessions) and press Enter:");
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line)?;
    Ok(line)
}

fn host_name() -> String {
    hostname::get()
        .ok()
        .and_then(|name| name.into_string().ok())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "unknown-host".to_string())
}

/// The scheme, host and port of the API base, where a node serves its portal.
pub fn origin(base: &str) -> String {
    url::Url::parse(base)
        .map(|url| url.origin().ascii_serialization())
        .unwrap_or_else(|_| base.to_string())
}

/// The expiry claim of a JWT as RFC 3339, read without checking the signature.
fn token_expiry(token: &str) -> Option<String> {
    let payload = token.split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload.trim_end_matches('='))
        .ok()?;
    let claims: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    let seconds = claims.get("exp")?.as_i64()?;
    let time = chrono::DateTime::<chrono::Utc>::from_timestamp(seconds, 0)?;
    Some(time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_token_expiry() {
        let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let payload = engine.encode(br#"{"sub":"u","exp":1767225600}"#);
        let token = format!("header.{payload}.signature");
        assert_eq!(
            token_expiry(&token).as_deref(),
            Some("2026-01-01T00:00:00Z")
        );
        assert_eq!(token_expiry("opaque-token"), None);
    }

    #[test]
    fn derives_website_origin() {
        assert_eq!(
            origin("https://node.example:8443/api/v1/"),
            "https://node.example:8443"
        );
    }
}
