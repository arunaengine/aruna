use crate::error::CliError;
use crate::storage::{import, snapshot};
use crate::tokens::{create_token, view_token};
use clap::{Parser, Subcommand};

mod error;
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
    CreateToken {
        user_id: Option<String>,
        expiry: Option<u64>,
    },
    ViewToken {
        token: String,
    },
    Snapshot {
        database_path: String,
        target_path: String,
    },
    Explore,
    Import {
        snapshot_path: String,
        target_path: String,
    },
}

#[tokio::main]
pub async fn main() -> Result<(), CliError> {
    let args = Cli::parse();

    match args.command {
        Commands::CreateToken { user_id, expiry } => {
            let token = create_token(user_id, expiry).await?;
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
        Commands::Explore => todo!(),
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
