#![recursion_limit = "256"]
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
use crate::tokens::{
    create_local_bootstrap_token, create_oidc_token, recover_initial_admin, view_token,
};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod error;
mod explorer;
mod info;
mod iroh_check;
mod migrate;
mod portal;
mod reclaim;
mod storage;
#[cfg(test)]
mod test_support;
mod tokens;

/// Operational CLI for inspecting, recovering and maintaining an Aruna node.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None, name = "aruna-doctor")]
struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    CreateToken {
        #[arg(long)]
        oidc_username: String,
        /// OIDC password. Prefer `ARUNA_OIDC_PASSWORD`: a value passed on the
        /// command line is visible to every process on the host.
        #[arg(long, env = "ARUNA_OIDC_PASSWORD", hide_env_values = true)]
        oidc_password: String,
        #[arg(long, default_value = "openid")]
        oidc_scope: String,
        #[arg(short = 'e', long)]
        oidc_only: bool,
        #[arg(short = 'b', long)]
        bootstrap_secret: Option<String>,
    },
    ViewToken {
        token: String,
    },
    /// Re-mint an initial-administrator onboarding secret on local storage.
    RecoverAdmin,
    Snapshot {
        database_path: String,
        target_path: String,
    },
    Explore {
        #[command(subcommand)]
        command: ExploreCommands,
    },
    Topics {
        #[command(subcommand)]
        command: TopicsCommands,
    },
    Topic {
        #[command(subcommand)]
        command: TopicCommands,
    },
    NodeState {
        #[arg(long)]
        database_path: String,
    },
    Import {
        snapshot_path: String,
        target_path: String,
    },
    Iroh {
        #[command(subcommand)]
        command: IrohCommands,
    },
    Portal {
        #[command(subcommand)]
        command: PortalCommands,
    },
    Info {
        /// Realm token used to read node topology and backend detail. Falls back
        /// to ARUNA_TOKEN.
        #[arg(long)]
        token: Option<String>,
    },
    Reclaim {
        #[command(subcommand)]
        command: ReclaimCommands,
    },
    /// Rewrite job family records stored before execution results carried
    /// stdout and stderr tails. Run with the node stopped; safe to repeat.
    Migrate {
        database_path: String,
    },
}

#[derive(Subcommand, Debug)]
pub enum ReclaimCommands {
    /// Queue every stored copy on one backend, for garbage that predates a
    /// switch to reclaim. Run with the node stopped.
    Seed {
        database_path: String,
        /// Backend key: `n:<name>` for a node backend, `g:<ulid>` for a tenant one.
        #[arg(long)]
        backend: String,
    },
    /// Queue depth and stuck physical deletes per backend.
    Status { database_path: String },
}

#[derive(Subcommand, Debug)]
pub enum ExploreCommands {
    Keyspaces {
        database_path: String,
    },
    Entries {
        database_path: String,
        keyspace: String,
    },
    /// Lists stored locations whose backend no longer resolves.
    Locations {
        database_path: String,
        /// Backends file to resolve node backends against. Without it only the
        /// implicit `default` backend counts as registered.
        #[arg(long)]
        backends_path: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
pub enum TopicsCommands {
    List {
        #[arg(long)]
        database_path: String,
    },
}

#[derive(Subcommand, Debug)]
pub enum TopicCommands {
    Status {
        #[arg(long)]
        database_path: String,
        #[arg(long)]
        id: String,
    },
    Placements {
        #[arg(long)]
        database_path: String,
        #[arg(long)]
        id: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
pub enum IrohCommands {
    Check {
        #[arg(long)]
        info_url: Option<String>,
        #[arg(long, default_value_t = 10)]
        timeout_secs: u64,
        /// Realm token used to read node topology. Falls back to ARUNA_TOKEN.
        #[arg(long)]
        token: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
pub enum PortalCommands {
    Update {
        #[arg(long)]
        portal_dir: Option<PathBuf>,
        #[arg(long)]
        artifact_url: Option<String>,
        #[arg(long)]
        artifact_sha256: Option<String>,
        #[arg(long)]
        latest_website_prerelease: bool,
    },
}

#[tokio::main]
pub async fn main() -> Result<(), CliError> {
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
                create_local_bootstrap_token(oidc_username, oidc_password, oidc_scope, secret)
                    .await?
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

pub async fn connect() -> () {
    todo!()
}
