#![recursion_limit = "256"]
//! Operator CLI for inspecting, recovering, and maintaining a node.
use crate::cli::{
    Cli, Commands, ExploreCommands, IrohCommands, PortalCommands, ReclaimCommands, TopicCommands,
    TopicsCommands,
};
use crate::error::CliError;
use crate::explorer::{
    explore_entries, explore_keyspaces, print_node_state, print_topic_placements,
    print_topic_status, print_topics_list, scan_locations,
};
use crate::info::print_info;
use crate::iroh_check::print_iroh_check;
use crate::migrate::migrate;
use crate::portal::update_portal;
use crate::reclaim::{print_status as reclaim_status, seed_backend};
use crate::storage::{import, snapshot};
use crate::tokens::{create_bootstrap_token, create_oidc_token, recover_initial_admin, view_token};
use clap::Parser;

mod cli;
mod error;
mod explorer;
mod info;
mod iroh_check;
mod migrate;
mod portal;
mod reclaim;
mod storage;
#[cfg(test)]
mod tests;
mod tokens;

#[tokio::main]
async fn main() -> Result<(), CliError> {
    let args = Cli::parse();

    match args.command {
        Commands::CreateToken {
            oidc_username,
            oidc_password,
            oidc_scope,
            oidc_only,
            bootstrap_secret,
        } => {
            let token = if let Some(secret) = bootstrap_secret {
                create_bootstrap_token(oidc_username, oidc_password, oidc_scope, secret).await?
            } else {
                create_oidc_token(oidc_username, oidc_password, oidc_scope, oidc_only).await?
            };
            println!("{}", token)
        }
        Commands::ViewToken { token } => {
            let token = view_token(token).await?;
            println!("{}", token);
        }
        Commands::RecoverAdmin => {
            let secret = recover_initial_admin().await?;
            println!("{}", secret);
        }
        Commands::Snapshot {
            database_path,
            target_path,
        } => snapshot(database_path, target_path).await?,
        Commands::Explore { command } => match command {
            ExploreCommands::Keyspaces { database_path } => {
                explore_keyspaces(database_path).await?
            }
            ExploreCommands::Entries {
                database_path,
                keyspace,
            } => explore_entries(database_path, keyspace).await?,
            ExploreCommands::Locations {
                database_path,
                backends_path,
            } => scan_locations(database_path, backends_path).await?,
        },
        Commands::Topics { command } => match command {
            TopicsCommands::List { database_path } => print_topics_list(database_path).await?,
        },
        Commands::Topic { command } => match command {
            TopicCommands::Status { database_path, id } => {
                print_topic_status(database_path, id).await?
            }
            TopicCommands::Placements { database_path, id } => {
                print_topic_placements(database_path, id).await?
            }
        },
        Commands::NodeState { database_path } => print_node_state(database_path).await?,
        Commands::Import {
            snapshot_path,
            target_path,
        } => import(snapshot_path, target_path).await?,
        Commands::Iroh { command } => match command {
            IrohCommands::Check {
                info_url,
                timeout_secs,
                token,
            } => print_iroh_check(info_url, timeout_secs, token).await?,
        },
        Commands::Portal { command } => match command {
            PortalCommands::Update {
                portal_dir,
                artifact_url,
                artifact_sha256,
                latest_website_prerelease,
            } => {
                update_portal(
                    portal_dir,
                    artifact_url,
                    artifact_sha256,
                    latest_website_prerelease,
                )
                .await?
            }
        },
        Commands::Info { token } => print_info(token).await?,
        Commands::Reclaim { command } => match command {
            ReclaimCommands::Seed {
                database_path,
                backend,
            } => seed_backend(database_path, backend).await?,
            ReclaimCommands::Status { database_path } => reclaim_status(database_path).await?,
        },
        Commands::Migrate { database_path } => migrate(database_path).await?,
    };

    Ok(())
}
