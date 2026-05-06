use crate::error::CliError;
use crate::explorer::{explore_entries, explore_keyspaces};
use crate::storage::{import, snapshot};
use crate::tokens::{create_local_bootstrap_token, create_oidc_token, view_token};
use clap::{Parser, Subcommand};

mod error;
mod explorer;
mod storage;
#[cfg(test)]
mod test_support;
mod tokens;

/// Simple program to greet a person
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
        #[arg(long)]
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
    Snapshot {
        database_path: String,
        target_path: String,
    },
    Explore {
        #[command(subcommand)]
        command: ExploreCommands,
    },
    Import {
        snapshot_path: String,
        target_path: String,
    },
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
        },
        Commands::Import {
            snapshot_path,
            target_path,
        } => import(snapshot_path, target_path).await?,
    };

    Ok(())
}

pub async fn connect() -> () {
    todo!()
}
