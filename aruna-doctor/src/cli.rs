//! Defines the doctor command line parser and every subcommand it accepts.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// Operational CLI for inspecting, recovering and maintaining an Aruna node.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None, name = "aruna-doctor")]
pub(crate) struct Cli {
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
    Metadata {
        #[command(subcommand)]
        command: MetadataCommands,
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
    /// Write the current objects of a bucket as record rows for a `record` table import: one
    /// TSV per connector id plus `empty.tsv`. Reads the store, so run it on a stopped node or
    /// on a copy of its database.
    Export {
        database_path: String,
        bucket: String,
        target_path: String,
    },
    /// Rewrite legacy job, PID mapping and realm rows, rebuild the identifier index, seal
    /// plain secrets and delete the former node info topics. Writes the database and must run
    /// while the node is stopped. Current rows stay unchanged, so repeating it is safe.
    Migrate {
        database_path: String,
        /// Document sync store; defaults to `<database_path>/document-sync`.
        #[arg(long)]
        sync_path: Option<String>,
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
    /// Delete one document sync topic with its cursors, quarantine rows and queued publishes, so
    /// the node joins the realm's topic again. Run with the node stopped; without `--apply` it
    /// only lists what it would delete.
    Purge {
        #[arg(long)]
        database_path: String,
        /// Topic id as 64 hex characters.
        #[arg(long)]
        id: String,
        /// Document sync store; defaults to `<database_path>/document-sync`.
        #[arg(long)]
        sync_path: Option<String>,
        #[arg(long)]
        apply: bool,
    },
}

#[derive(Subcommand, Debug)]
pub enum MetadataCommands {
    /// Move one metadata graph's sync cursor from a replaced genesis onto the current one. Refuses
    /// when the genesis is unchanged or actor sequences could collide. Run with the node stopped;
    /// without `--apply` it only reports both branches and every actor the graph applied.
    RepairCursor {
        #[arg(long)]
        database_path: String,
        /// Graph IRI of the metadata document.
        #[arg(long)]
        graph: String,
        /// Craqle store; defaults to `<database_path>/craqle`.
        #[arg(long)]
        craqle_path: Option<String>,
        /// Document sync store; defaults to `<database_path>/document-sync`.
        #[arg(long)]
        sync_path: Option<String>,
        #[arg(long)]
        apply: bool,
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
