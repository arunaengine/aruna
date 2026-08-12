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
    fetch_prerelease_url(WEBSITE_RELEASES_URL).await
}

async fn fetch_prerelease_url(releases_url: &str) -> Result<String, CliError> {
    let client = reqwest::Client::builder()
        .user_agent("aruna-doctor")
        .build()?;
    let mut request = client
        .get(releases_url)
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
    use super::{
        GithubRelease, GithubReleaseAsset, PORTAL_ARTIFACT_NAME, fetch_prerelease_url,
        select_website_prerelease_artifact, update_portal,
    };
    use axum::{Router, body::Bytes, routing::get};
    use flate2::{Compression, write::GzEncoder};
    use sha2::{Digest, Sha256};
    use std::fs;
    use tempfile::tempdir;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn selects_prerelease_artifact() {
        let (base_url, _checksum) = start_server(portal_tarball()).await;

        let artifact_url = fetch_prerelease_url(&format!("{base_url}/releases"))
            .await
            .unwrap();

        assert_eq!(artifact_url, format!("{base_url}/{PORTAL_ARTIFACT_NAME}"));
    }

    #[tokio::test]
    async fn installs_selected_prerelease() {
        // Selection, download, extraction, and index.html validation in one path.
        let (base_url, checksum) = start_server(portal_tarball()).await;
        let root = tempdir().unwrap();
        let portal_dir = root.path().join("portal");
        let artifact_url = fetch_prerelease_url(&format!("{base_url}/releases"))
            .await
            .unwrap();

        update_portal(
            Some(portal_dir.clone()),
            Some(artifact_url),
            Some(checksum),
            false,
        )
        .await
        .unwrap();

        assert_eq!(
            fs::read_to_string(portal_dir.join("index.html")).unwrap(),
            "<html>portal</html>"
        );
    }

    #[tokio::test]
    async fn rejects_wrong_checksum() {
        let (base_url, _checksum) = start_server(portal_tarball()).await;
        let root = tempdir().unwrap();
        let portal_dir = root.path().join("portal");

        let result = update_portal(
            Some(portal_dir.clone()),
            Some(format!("{base_url}/{PORTAL_ARTIFACT_NAME}")),
            Some("0".repeat(64)),
            false,
        )
        .await;

        assert!(result.is_err());
        assert!(!portal_dir.exists());
    }

    fn portal_tarball() -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        let body = b"<html>portal</html>";
        {
            let mut builder = tar::Builder::new(&mut encoder);
            let mut header = tar::Header::new_gnu();
            header.set_size(body.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder
                .append_data(&mut header, "index.html", &body[..])
                .unwrap();
            builder.finish().unwrap();
        }

        encoder.finish().unwrap()
    }

    /// Serves the release list, the artifact, and its checksum sidecar locally.
    async fn start_server(tarball: Vec<u8>) -> (String, String) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let checksum = hex::encode(Sha256::digest(&tarball));
        let releases = releases_body(&base_url);
        let sidecar = format!("{checksum}  {PORTAL_ARTIFACT_NAME}\n");
        let artifact_route = format!("/{PORTAL_ARTIFACT_NAME}");
        let app = Router::new()
            .route(
                "/releases",
                get(move || {
                    let releases = releases.clone();
                    async move { releases }
                }),
            )
            .route(
                &artifact_route,
                get(move || {
                    let tarball = tarball.clone();
                    async move { Bytes::from(tarball) }
                }),
            )
            .route(
                &format!("{artifact_route}.sha256"),
                get(move || {
                    let sidecar = sidecar.clone();
                    async move { sidecar }
                }),
            );

        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        (base_url, checksum)
    }

    /// A stable release and a draft prerelease precede the wanted prerelease.
    fn releases_body(base_url: &str) -> String {
        format!(
            r#"[
              {{"draft": false, "prerelease": false,
                "assets": [{{"name": "{PORTAL_ARTIFACT_NAME}", "browser_download_url": "{base_url}/stable"}}]}},
              {{"draft": true, "prerelease": true,
                "assets": [{{"name": "{PORTAL_ARTIFACT_NAME}", "browser_download_url": "{base_url}/draft"}}]}},
              {{"draft": false, "prerelease": true,
                "assets": [{{"name": "other.tar.gz", "browser_download_url": "{base_url}/other"}},
                           {{"name": "{PORTAL_ARTIFACT_NAME}", "browser_download_url": "{base_url}/{PORTAL_ARTIFACT_NAME}"}}]}}
            ]"#
        )
    }

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
