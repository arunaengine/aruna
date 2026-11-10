//! Defines the command line: global options, commands and their arguments.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use clap::{Args, Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

const AFTER_HELP: &str = "\
Remote paths are written bucket/key or s3://bucket/key. For cp and sync, the source decides the \
direction: an existing local source uploads, any other source downloads. A bucket name that \
exists on several nodes needs --node.

Sign in with a token from the portal (Settings, API sessions):
  aruna-cli login --api https://node.example --token <TOKEN>
Scripts may skip login and set ARUNA_API and ARUNA_TOKEN instead.";

/// A command line client for an Aruna realm's REST and S3 APIs.
#[derive(Debug, Parser)]
#[command(name = "aruna-cli", version, after_help = AFTER_HELP)]
pub struct Cli {
    /// The node's API URL, such as https://node.example; login stores it.
    #[arg(long, global = true, env = "ARUNA_API")]
    pub api: Option<String>,
    /// Folder for the profile; login keeps the token in the OS keyring.
    #[arg(long, global = true, env = "ARUNA_CONFIG_DIR")]
    pub config_dir: Option<PathBuf>,
    /// The node that hosts a bucket, when its name exists on several nodes.
    #[arg(long, global = true)]
    pub node: Option<String>,
    /// S3 endpoint for a node that publishes none.
    #[arg(long, global = true, env = "ARUNA_S3_ENDPOINT")]
    pub s3_endpoint: Option<String>,
    /// Print JSON instead of tables.
    #[arg(long, global = true)]
    pub json: bool,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Sign in with a realm token and create a CLI session (label cli@<host>).
    Login(LoginArgs),
    /// End the CLI session and forget the token.
    Logout,
    /// Show the signed-in user, node and session.
    Whoami,
    /// Show the realm or its nodes.
    #[command(subcommand)]
    Realm(RealmCommand),
    /// List groups, or the members of one group.
    Groups {
        #[command(subcommand)]
        command: Option<GroupsCommand>,
    },
    /// List or create buckets.
    #[command(subcommand)]
    Buckets(BucketsCommand),
    /// List the objects and folders below bucket/prefix.
    Ls {
        /// bucket or bucket/prefix
        path: String,
        /// List every object below the prefix, not one folder level.
        #[arg(short, long)]
        recursive: bool,
    },
    /// Search the realm for datasets, buckets, groups and users.
    Search {
        query: String,
        /// Comma-separated subset of documents,buckets,groups,users.
        #[arg(long)]
        types: Option<String>,
        #[arg(long, default_value_t = 10)]
        limit: usize,
    },
    /// Copy one file between a local path and bucket/key.
    Cp(CopyArgs),
    /// Copy what differs between a local folder and bucket/prefix, in one direction.
    Sync(SyncArgs),
    /// S3 credentials for other tools.
    #[command(subcommand)]
    S3(S3Command),
    /// Submit and follow compute jobs through the GA4GH TES API.
    #[command(subcommand)]
    Jobs(JobsCommand),
    /// Create, find, validate and export RO-Crate metadata.
    #[command(subcommand)]
    Meta(MetaCommand),
    /// Create a share link for a bucket, folder or file.
    Share(ShareArgs),
    /// Start and build WASM plugins.
    #[command(subcommand)]
    Plugin(PluginCommand),
}

#[derive(Debug, Args)]
pub struct LoginArgs {
    /// A realm token or API session token; read from standard input when omitted.
    #[arg(long, env = "ARUNA_LOGIN_TOKEN", hide_env_values = true)]
    pub token: Option<String>,
    /// The realm website for share links; defaults to the API's origin.
    #[arg(long)]
    pub website: Option<String>,
}

#[derive(Debug, Subcommand)]
pub enum PluginCommand {
    /// Start a Rust plugin project: plugin.toml, Cargo.toml and src/lib.rs.
    New {
        /// Plugin name, such as dataset-announcer.
        name: String,
        /// Folder to create; defaults to the name.
        #[arg(long)]
        path: Option<PathBuf>,
    },
    /// Build the plugin in a folder into one checked artifact for publishing.
    Build {
        /// Folder with plugin.toml and Cargo.toml.
        #[arg(default_value = ".")]
        path: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
pub enum RealmCommand {
    /// Realm id, description, identity providers and node count.
    Info,
    /// The realm's nodes with kind, status and addresses.
    Nodes,
}

#[derive(Debug, Subcommand)]
pub enum GroupsCommand {
    /// List the members of a group, by id or name.
    Members { group: String },
}

#[derive(Debug, Subcommand)]
pub enum BucketsCommand {
    /// List the buckets you can read across the realm.
    Ls {
        /// Only buckets of this group, by id or name.
        #[arg(long)]
        group: Option<String>,
    },
    /// Create a bucket for a group on the signed-in node, or on --node.
    Create {
        name: String,
        /// The owning group, by id or name.
        #[arg(long)]
        group: String,
    },
}

#[derive(Debug, Args)]
pub struct CopyArgs {
    pub source: String,
    pub target: String,
    /// Part size in MiB; larger files use multipart uploads that resume.
    #[arg(long, default_value_t = 64, value_parser = clap::value_parser!(u64).range(5..=5120))]
    pub part_size: u64,
}

#[derive(Debug, Args)]
pub struct SyncArgs {
    pub source: String,
    pub target: String,
    /// Show the planned transfers and change nothing.
    #[arg(long)]
    pub dry_run: bool,
    /// Part size in MiB for multipart uploads.
    #[arg(long, default_value_t = 64, value_parser = clap::value_parser!(u64).range(5..=5120))]
    pub part_size: u64,
}

#[derive(Debug, Subcommand)]
pub enum S3Command {
    /// Print the S3 endpoint and a one-hour session as shell exports.
    Env {
        /// The group whose data the session reaches, by id or name.
        #[arg(long)]
        group: String,
        /// Print the secret key and session token too; anyone who sees them can use them.
        #[arg(long)]
        show_secret: bool,
    },
}

#[derive(Debug, Subcommand)]
pub enum JobsCommand {
    /// Submit a TES task from a JSON file.
    Submit { file: PathBuf },
    /// List TES tasks.
    Ls {
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    /// Show one task.
    Status { id: String },
    /// Cancel a task.
    Cancel { id: String },
    /// Poll a task until it ends.
    Watch {
        id: String,
        /// Seconds between polls.
        #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u64).range(1..))]
        interval: u64,
    },
}

#[derive(Debug, Subcommand)]
pub enum MetaCommand {
    /// Create a metadata document from ro-crate-metadata.json, a crate folder or a zip.
    Create(MetaCreateArgs),
    /// Import a crate archive or folder with its files into a bucket, as a job.
    Import(MetaImportArgs),
    /// Replace a document's RO-Crate with a new one.
    Update {
        id: String,
        /// ro-crate-metadata.json, a crate folder or a zip
        source: PathBuf,
        #[command(flatten)]
        visibility: Visibility,
        #[arg(long)]
        message: Option<String>,
    },
    /// Open a document's RO-Crate in $EDITOR and save the result.
    Edit {
        id: String,
        #[arg(long)]
        message: Option<String>,
    },
    /// Check a crate against the structural rules and its Profile, storing nothing.
    Validate {
        source: PathBuf,
        /// The group it would be saved in, by id or name; its private Profiles then apply.
        #[arg(long)]
        group: Option<String>,
        /// A Profile IRI to add to the root's conformsTo before checking.
        #[arg(long)]
        profile: Option<String>,
        /// Check it as a public dataset, which also lists files the public cannot read.
        #[arg(long)]
        public: bool,
    },
    /// List metadata documents, most recently changed first.
    Ls {
        /// Only this group, by id or name.
        #[arg(long)]
        group: Option<String>,
        /// Only public documents, read without your token.
        #[arg(long)]
        public: bool,
        #[arg(long, default_value_t = 50)]
        limit: usize,
        #[arg(long, default_value_t = 0)]
        offset: usize,
    },
    /// Full-text search over datasets and their metadata across the realm.
    Search {
        query: String,
        /// Only this group, by id or name.
        #[arg(long)]
        group: Option<String>,
        /// Only documents that conform to this specification or Profile IRI.
        #[arg(long)]
        conforms_to: Option<String>,
        #[arg(long, default_value_t = 25)]
        limit: usize,
        /// The next_cursor of a previous page.
        #[arg(long)]
        cursor: Option<String>,
    },
    /// Show a document's registry summary.
    Show { id: String },
    /// Write a document's RO-Crate JSON-LD to a file or standard output.
    Export {
        id: String,
        #[arg(short, long)]
        output: Option<PathBuf>,
    },
    /// Create a dataset that describes a whole bucket and link it to the bucket.
    Link(MetaLinkArgs),
}

#[derive(Debug, Args)]
pub struct Visibility {
    /// Make the document public.
    #[arg(long, conflicts_with = "private")]
    pub public: bool,
    /// Make the document private.
    #[arg(long)]
    pub private: bool,
}

impl Visibility {
    pub fn choice(&self) -> Option<bool> {
        match (self.public, self.private) {
            (true, _) => Some(true),
            (_, true) => Some(false),
            _ => None,
        }
    }
}

#[derive(Debug, Args)]
pub struct MetaCreateArgs {
    /// ro-crate-metadata.json, a crate folder or a zip
    pub source: PathBuf,
    /// The owning group, by id or name.
    #[arg(long)]
    pub group: String,
    /// Metadata path; defaults to datasets/<root name>.
    #[arg(long)]
    pub path: Option<String>,
    /// Make the document public; it is private otherwise.
    #[arg(long)]
    pub public: bool,
    #[arg(long)]
    pub message: Option<String>,
}

#[derive(Debug, Args)]
pub struct MetaImportArgs {
    /// A crate archive (.zip, .eln) or a crate folder, which is packed first.
    pub source: PathBuf,
    #[arg(long)]
    pub group: String,
    /// The bucket on the signed-in node that receives the crate's files.
    #[arg(long)]
    pub bucket: String,
    #[arg(long, default_value = "")]
    pub prefix: String,
    /// Metadata path; defaults to datasets/<root name>.
    #[arg(long)]
    pub path: Option<String>,
    #[arg(long)]
    pub public: bool,
    /// Wait until the import job ends.
    #[arg(long)]
    pub wait: bool,
}

#[derive(Debug, Args)]
pub struct MetaLinkArgs {
    /// The bucket the dataset describes; folders are not supported by the API.
    pub bucket: String,
    #[arg(long)]
    pub title: String,
    #[arg(long)]
    pub description: String,
    #[arg(long)]
    pub license: Option<String>,
    #[arg(long)]
    pub public: bool,
    /// Metadata path for the new dataset.
    #[arg(long)]
    pub path: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum ShareMode {
    Read,
    ReadUpload,
    Upload,
}

#[derive(Debug, Args)]
pub struct ShareArgs {
    /// bucket, bucket/folder/ or bucket/key
    pub path: String,
    #[arg(long, value_enum, default_value = "read")]
    pub mode: ShareMode,
    /// Lifetime such as 7d, 12h or 30m, or an RFC 3339 time.
    #[arg(long)]
    pub expires: Option<String>,
    #[arg(long, default_value = "Shared from aruna-cli")]
    pub label: String,
    /// Ask visitors for this password.
    #[arg(long)]
    pub password: Option<String>,
    /// Share a folder even when the path does not end with a slash.
    #[arg(long)]
    pub folder: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    fn parse(line: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(std::iter::once("aruna-cli").chain(line.iter().copied()))
    }

    #[test]
    fn checks_definition() {
        Cli::command().debug_assert();
    }

    #[test]
    fn parses_copy_options() {
        let cli = parse(&["--json", "cp", "a.txt", "data/a.txt", "--part-size", "8"]).unwrap();
        assert!(cli.json);
        let Command::Cp(args) = cli.command else {
            panic!("expected cp");
        };
        assert_eq!((args.source.as_str(), args.part_size), ("a.txt", 8));
        assert!(parse(&["cp", "a", "b", "--part-size", "1"]).is_err());
    }

    #[test]
    fn parses_trailing_globals() {
        let cli = parse(&["sync", "dir", "data/x", "--dry-run", "--node", "n1"]).unwrap();
        assert_eq!(cli.node.as_deref(), Some("n1"));
        assert!(matches!(
            cli.command,
            Command::Sync(SyncArgs { dry_run: true, .. })
        ));
    }

    #[test]
    fn parses_meta_commands() {
        let cli = parse(&["meta", "create", "crate", "--group", "g", "--public"]).unwrap();
        assert!(matches!(
            cli.command,
            Command::Meta(MetaCommand::Create(MetaCreateArgs { public: true, .. }))
        ));
        let cli = parse(&["meta", "update", "id", "f.json", "--private"]).unwrap();
        let Command::Meta(MetaCommand::Update { visibility, .. }) = cli.command else {
            panic!("expected update");
        };
        assert_eq!(visibility.choice(), Some(false));
        assert!(parse(&["meta", "update", "id", "f", "--public", "--private"]).is_err());
        assert!(parse(&["meta", "create", "crate"]).is_err());
    }

    #[test]
    fn parses_share_mode() {
        let cli = parse(&[
            "share",
            "data/x/",
            "--mode",
            "read-upload",
            "--expires",
            "7d",
        ])
        .unwrap();
        let Command::Share(args) = cli.command else {
            panic!("expected share");
        };
        assert_eq!(args.mode, ShareMode::ReadUpload);
        assert!(parse(&["share", "data", "--mode", "write"]).is_err());
    }

    #[test]
    fn parses_groups_members() {
        let cli = parse(&["groups"]).unwrap();
        assert!(matches!(cli.command, Command::Groups { command: None }));
        let cli = parse(&["groups", "members", "lab"]).unwrap();
        assert!(matches!(
            cli.command,
            Command::Groups {
                command: Some(GroupsCommand::Members { .. })
            }
        ));
    }
}
