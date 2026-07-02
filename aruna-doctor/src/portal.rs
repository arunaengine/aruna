use crate::error::CliError;
use aruna::config::PortalArtifactConfig;
use aruna::portal::update_artifact;
use serde::Deserialize;
use std::path::PathBuf;

const WEBSITE_REPO: &str = "arunaengine/website";
const WEBSITE_RELEASES_URL: &str =
    "https://api.github.com/repos/arunaengine/website/releases?per_page=20";
const PORTAL_ARTIFACT_NAME: &str = "aruna-portal-dist.tar.gz";

pub async fn update_portal(
    portal_dir: Option<PathBuf>,
    artifact_url: Option<String>,
    artifact_sha256: Option<String>,
    latest_website_prerelease: bool,
) -> Result<(), CliError> {
    let portal_dir = portal_dir
        .or_else(|| nonempty_env("PORTAL_DIR").map(PathBuf::from))
        .ok_or(CliError::MissingPortalConfig("PORTAL_DIR"))?;
    let artifact_url = match artifact_url.or_else(|| nonempty_env("PORTAL_ARTIFACT_URL")) {
        Some(artifact_url) => artifact_url,
        None if latest_website_prerelease => latest_website_prerelease_artifact_url().await?,
        None => return Err(CliError::MissingPortalConfig("PORTAL_ARTIFACT_URL")),
    };
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

async fn latest_website_prerelease_artifact_url() -> Result<String, CliError> {
    let client = reqwest::Client::builder()
        .user_agent("aruna-doctor")
        .build()?;
    let mut request = client
        .get(WEBSITE_RELEASES_URL)
        .header("Accept", "application/vnd.github+json")
        .header("X-GitHub-Api-Version", "2022-11-28");
    if let Some(token) = nonempty_env("GITHUB_TOKEN") {
        request = request.bearer_auth(token);
    }

    let releases: Vec<GithubRelease> = request.send().await?.error_for_status()?.json().await?;
    select_website_prerelease_artifact(&releases).ok_or(CliError::MissingPortalWebsiteArtifact {
        repo: WEBSITE_REPO,
        asset: PORTAL_ARTIFACT_NAME,
    })
}

fn select_website_prerelease_artifact(releases: &[GithubRelease]) -> Option<String> {
    releases
        .iter()
        .filter(|release| release.prerelease && !release.draft)
        .find_map(|release| {
            release
                .assets
                .iter()
                .find(|asset| asset.name == PORTAL_ARTIFACT_NAME)
                .map(|asset| asset.browser_download_url.clone())
        })
}

fn nonempty_env(key: &'static str) -> Option<String> {
    dotenvy::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

#[derive(Clone, Debug, Deserialize)]
struct GithubRelease {
    draft: bool,
    prerelease: bool,
    assets: Vec<GithubReleaseAsset>,
}

#[derive(Clone, Debug, Deserialize)]
struct GithubReleaseAsset {
    name: String,
    browser_download_url: String,
}

#[cfg(test)]
mod tests {
    use super::{GithubRelease, GithubReleaseAsset, select_website_prerelease_artifact};

    #[test]
    fn selects_first_prerelease_with_portal_artifact() {
        let releases = vec![
            release(false, false, &[asset("aruna-portal-dist.tar.gz", "stable")]),
            release(true, true, &[asset("aruna-portal-dist.tar.gz", "draft")]),
            release(
                true,
                false,
                &[
                    asset("other.tar.gz", "other"),
                    asset("aruna-portal-dist.tar.gz", "latest-prerelease"),
                ],
            ),
            release(
                true,
                false,
                &[asset("aruna-portal-dist.tar.gz", "older-prerelease")],
            ),
        ];

        assert_eq!(
            select_website_prerelease_artifact(&releases).as_deref(),
            Some("latest-prerelease")
        );
    }

    fn release(prerelease: bool, draft: bool, assets: &[GithubReleaseAsset]) -> GithubRelease {
        GithubRelease {
            draft,
            prerelease,
            assets: assets.to_vec(),
        }
    }

    fn asset(name: &str, browser_download_url: &str) -> GithubReleaseAsset {
        GithubReleaseAsset {
            name: name.to_string(),
            browser_download_url: browser_download_url.to_string(),
        }
    }
}
