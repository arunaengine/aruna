//! Shows the realm, its nodes, its groups and a group's members.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{Context, resolve_group};
use crate::args::{GroupsCommand, RealmCommand};
use crate::error::CliError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize, Serialize)]
struct RealmInfo {
    realm_id: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    oidc_providers: Vec<Provider>,
    #[serde(default)]
    management_urls: Vec<String>,
    #[serde(default)]
    nodes: Vec<RealmNode>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Provider {
    id: String,
    issuer: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct RealmNode {
    node_id: String,
    kind: Value,
    #[serde(default)]
    present: bool,
    connection_status: Value,
    display_name: Option<String>,
    place: Option<String>,
    info: Option<NodeDocument>,
}

#[derive(Debug, Deserialize, Serialize)]
struct NodeDocument {
    urls: NodeUrls,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct NodeUrls {
    api: Option<String>,
    s3: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct GroupList {
    groups: Vec<Group>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Group {
    group_id: String,
    display_name: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct MemberList {
    members: Vec<Member>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Member {
    user_id: String,
    name: Option<String>,
    #[serde(default)]
    roles: Vec<MemberRole>,
}

#[derive(Debug, Deserialize, Serialize)]
struct MemberRole {
    name: String,
}

pub async fn realm(context: &mut Context<'_>, command: RealmCommand) -> Result<(), CliError> {
    let api = context.api()?;
    let info: RealmInfo = api.get("system/realm", &[]).await?;
    match command {
        RealmCommand::Info => {
            if context.out.json {
                return context.out.json(&info);
            }
            let providers: Vec<&str> = info
                .oidc_providers
                .iter()
                .map(|provider| provider.issuer.as_str())
                .collect();
            let rows = vec![
                vec!["Realm".to_string(), info.realm_id.clone()],
                vec!["Description".to_string(), info.description.clone()],
                vec!["Nodes".to_string(), info.nodes.len().to_string()],
                vec!["Sign-in".to_string(), providers.join(", ")],
                vec!["Management".to_string(), info.management_urls.join(", ")],
            ];
            context.out.table(&["", ""], &rows)
        }
        RealmCommand::Nodes => {
            let rows = info
                .nodes
                .iter()
                .map(|node| {
                    let urls = node.info.as_ref().map(|info| &info.urls);
                    vec![
                        node.node_id.clone(),
                        node.display_name.clone().unwrap_or_default(),
                        text(&node.kind),
                        match node.present {
                            true => text(&node.connection_status),
                            false => "absent".to_string(),
                        },
                        node.place.clone().unwrap_or_default(),
                        urls.and_then(|urls| urls.api.clone()).unwrap_or_default(),
                        urls.and_then(|urls| urls.s3.clone()).unwrap_or_default(),
                    ]
                })
                .collect();
            context.out.listing(
                &info.nodes,
                &["NODE", "NAME", "KIND", "STATUS", "PLACE", "API", "S3"],
                rows,
            )
        }
    }
}

pub async fn groups(
    context: &mut Context<'_>,
    command: Option<GroupsCommand>,
) -> Result<(), CliError> {
    let api = context.api()?;
    match command {
        None => {
            let list: GroupList = api
                .get("access/groups", &[("limit", "1000".to_string())])
                .await?;
            let rows = list
                .groups
                .iter()
                .map(|group| vec![group.group_id.clone(), group.display_name.clone()])
                .collect();
            context.out.listing(&list.groups, &["GROUP", "NAME"], rows)
        }
        Some(GroupsCommand::Members { group }) => {
            let group_id = resolve_group(&api, &group).await?;
            let list: MemberList = api
                .get(&format!("access/groups/{group_id}/members"), &[])
                .await?;
            let rows = list
                .members
                .iter()
                .map(|member| {
                    let roles: Vec<&str> =
                        member.roles.iter().map(|role| role.name.as_str()).collect();
                    vec![
                        member.user_id.clone(),
                        member.name.clone().unwrap_or_default(),
                        roles.join(", "),
                    ]
                })
                .collect();
            context
                .out
                .listing(&list.members, &["USER", "NAME", "ROLES"], rows)
        }
    }
}

/// A JSON string as plain text, any other value in its JSON form.
fn text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}
