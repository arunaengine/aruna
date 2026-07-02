use crate::error::CliError;
use aruna::config::PortalArtifactConfig;
use aruna::portal::update_artifact;
use std::path::PathBuf;

pub async fn update_portal(
    portal_dir: Option<PathBuf>,
    artifact_url: Option<String>,
    artifact_sha256: Option<String>,
) -> Result<(), CliError> {
    let portal_dir = portal_dir
        .or_else(|| nonempty_env("PORTAL_DIR").map(PathBuf::from))
        .ok_or(CliError::MissingPortalConfig("PORTAL_DIR"))?;
    let artifact_url = artifact_url
        .or_else(|| nonempty_env("PORTAL_ARTIFACT_URL"))
        .ok_or(CliError::MissingPortalConfig("PORTAL_ARTIFACT_URL"))?;
    let artifact_sha256 = artifact_sha256.or_else(|| nonempty_env("PORTAL_ARTIFACT_SHA256"));

    let status = update_artifact(PortalArtifactConfig {
        artifact_url: Some(artifact_url),
        artifact_sha256,
        portal_dir: portal_dir.clone(),
    })
    .await?;

    println!(
        "Updated portal in {}{}",
        portal_dir.display(),
        status
            .checksum
            .as_deref()
            .map(|checksum| format!(" ({checksum})"))
            .unwrap_or_default()
    );
    Ok(())
}

fn nonempty_env(key: &'static str) -> Option<String> {
    dotenvy::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
}
