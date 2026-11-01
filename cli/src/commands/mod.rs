//! Dispatches parsed commands and holds what they share: profile, API and output.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod account;
mod copy;
mod data;
mod jobs;
mod meta;
mod realm;

use crate::args::{Cli, Command};
use crate::client::Api;
use crate::config::{Config, Profile};
use crate::error::CliError;
use crate::output::Output;
use serde::Deserialize;
use std::io::Write;

/// The state one command runs with.
pub struct Context<'a> {
    pub config: Config,
    pub api_flag: Option<String>,
    pub node: Option<String>,
    pub s3_endpoint: Option<String>,
    pub out: Output<'a>,
}

impl Context<'_> {
    /// The saved profile, or for scripts one made from --api while ARUNA_TOKEN is set.
    pub fn profile(&self) -> Result<Profile, CliError> {
        if let Some(profile) = self.config.load_profile()? {
            return Ok(profile);
        }
        match (&self.api_flag, std::env::var_os("ARUNA_TOKEN")) {
            (Some(api), Some(_)) => Ok(Profile {
                api: api.clone(),
                website: account::origin(api),
                ..Profile::default()
            }),
            _ => Err(CliError::NotSignedIn),
        }
    }

    /// The signed-in node's API with the stored token.
    pub fn api(&self) -> Result<Api, CliError> {
        let profile = self.profile()?;
        let token = self.config.load_token(&profile)?;
        let mut api = Api::new(
            self.api_flag.as_deref().unwrap_or(&profile.api),
            Some(token),
        )?;
        // Another --api base is some node the profile does not know by id.
        if self.api_flag.is_none() {
            api.node_id = profile.node_id;
        }
        Ok(api)
    }
}

pub async fn dispatch(cli: Cli, out: &mut dyn Write) -> Result<(), CliError> {
    let mut context = Context {
        config: Config::new(cli.config_dir, cli.token_store)?,
        api_flag: cli.api,
        node: cli.node,
        s3_endpoint: cli.s3_endpoint,
        out: Output {
            out,
            json: cli.json,
        },
    };
    let context = &mut context;
    match cli.command {
        Command::Login(args) => account::login(context, args).await,
        Command::Logout => account::logout(context).await,
        Command::Whoami => account::whoami(context).await,
        Command::Realm(command) => realm::realm(context, command).await,
        Command::Groups { command } => realm::groups(context, command).await,
        Command::Buckets(command) => data::buckets(context, command).await,
        Command::Ls { path, recursive } => data::list(context, &path, recursive).await,
        Command::Search {
            query,
            types,
            limit,
        } => data::search(context, &query, types, limit).await,
        Command::Cp(args) => copy::copy(context, args).await,
        Command::Sync(args) => copy::sync(context, args).await,
        Command::S3(command) => data::s3_env(context, command).await,
        Command::Jobs(command) => jobs::jobs(context, command).await,
        Command::Meta(command) => meta::meta(context, command).await,
        Command::Share(args) => data::share(context, args).await,
    }
}

#[derive(Debug, Deserialize)]
struct GroupList {
    groups: Vec<GroupEntry>,
}

#[derive(Debug, Deserialize)]
struct GroupEntry {
    group_id: String,
    display_name: String,
}

/// A group id as given, or the id of the one group with this display name.
pub async fn resolve_group(api: &Api, raw: &str) -> Result<String, CliError> {
    if is_ulid(raw) {
        return Ok(raw.to_ascii_uppercase());
    }
    let list: GroupList = api
        .get("access/groups", &[("limit", "1000".to_string())])
        .await?;
    let matches: Vec<&GroupEntry> = list
        .groups
        .iter()
        .filter(|group| group.display_name.eq_ignore_ascii_case(raw))
        .collect();
    match matches.as_slice() {
        [group] => Ok(group.group_id.clone()),
        [] => Err(CliError::usage(format!("no group is named {raw}"))),
        _ => Err(CliError::usage(format!(
            "several groups are named {raw}; pass the group id"
        ))),
    }
}

/// Whether the text has the form of a ULID: 26 Crockford base32 characters.
pub fn is_ulid(raw: &str) -> bool {
    raw.len() == 26
        && raw.bytes().all(|byte| {
            byte.is_ascii_digit() || (byte.is_ascii_alphabetic() && !b"iIlLoOuU".contains(&byte))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_ulids() {
        assert!(is_ulid("01JABCDEF0123456789ABCDEFG"));
        assert!(!is_ulid("lab-group"));
        assert!(!is_ulid("01JABCDEF0123456789ABCDEFU"));
    }
}
