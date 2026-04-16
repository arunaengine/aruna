use crate::error::CliError;
use crate::explorer::{explore_entries, explore_keyspaces};
use crate::storage::{import, snapshot};
use crate::tokens::{create_token, view_token};
use clap::{Args, Parser, Subcommand};

mod error;
mod explorer;
mod storage;
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
    /// Name of the person to greet
    CreateToken(CreateTokenArgs),
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

#[derive(Args, Debug)]
pub struct CreateTokenArgs {
    #[arg(long)]
    name: Option<String>,
    #[arg(long)]
    unsafe_arbitrary_user_id: bool,
    user_id: Option<String>,
    expiry: Option<u64>,
    #[arg(long)]
    oidc_username: Option<String>,
    #[arg(long)]
    oidc_password: Option<String>,
    #[arg(long)]
    oidc_name: Option<String>,
    #[arg(long, default_value = "openid")]
    oidc_scope: String,
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
        Commands::CreateToken(args) => {
            let token = create_token(
                args.name,
                args.unsafe_arbitrary_user_id,
                args.user_id,
                args.expiry,
                args.oidc_username,
                args.oidc_password,
                args.oidc_name,
                args.oidc_scope,
            )
            .await?;
            println!("TOKEN: {token}");
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
