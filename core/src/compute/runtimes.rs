use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct QuickRuntime {
    pub id: &'static str,
    pub label: &'static str,
    pub hint: &'static str,
    pub image: &'static str,
    pub command: &'static [&'static str],
    pub env: &'static [(&'static str, &'static str)],
    pub file: &'static str,
    pub lang: &'static str,
    pub content_type: &'static str,
    pub template: &'static str,
}

pub const QUICK_RUNTIMES: [QuickRuntime; 3] = [
    QuickRuntime {
        id: "python-uv",
        label: "Python",
        hint: "PyPI dependencies managed by uv.",
        image: "ghcr.io/astral-sh/uv:python3.13-bookworm-slim",
        command: &["uv", "run", "--no-project"],
        env: &[("UV_CACHE_DIR", ".uv-cache")],
        file: "script.py",
        lang: "python",
        content_type: "text/x-python",
        template: "print(\"hello from aruna\")\n",
    },
    QuickRuntime {
        id: "deno",
        label: "JavaScript / TypeScript",
        hint: "npm dependencies resolved by Deno.",
        image: "denoland/deno:alpine-2.9.3",
        command: &["deno", "run", "-A"],
        env: &[("DENO_DIR", ".deno-cache")],
        file: "script.ts",
        lang: "javascript",
        content_type: "text/typescript",
        template: "console.log(\"hello from aruna\");\n",
    },
    QuickRuntime {
        id: "bash",
        label: "Bash",
        hint: "Plain shell, no extra tooling.",
        image: "bash:5.2",
        command: &["bash"],
        env: &[],
        file: "script.sh",
        lang: "text",
        content_type: "text/x-shellscript",
        template: "echo \"hello from aruna\"\n",
    },
];

pub fn quick_runtime(id: &str) -> Option<&'static QuickRuntime> {
    QUICK_RUNTIMES.iter().find(|runtime| runtime.id == id)
}

/// Job tag marking an interactive session. `SESSION_TAG_NOTEBOOK` is its only
/// accepted value today.
pub const SESSION_TAG: &str = "aruna-engine.org/session";
pub const SESSION_TAG_NOTEBOOK: &str = "notebook";
/// Catalog runtime the node resolved the image from, stored on the spec so the
/// executing node reports it back without matching images.
pub const SESSION_RUNTIME_TAG: &str = "aruna-engine.org/session-runtime";
/// Wall-clock milliseconds the submitter's bearer token expires at. The
/// session credential never outlives it.
pub const SESSION_EXPIRY_TAG: &str = "aruna-engine.org/session-expires-at-ms";
/// Idle wait the submitter asked for. The executing node clamps it to the realm
/// value, so a longer request never extends the session.
pub const SESSION_IDLE_TAG: &str = "aruna-engine.org/session-idle-ms";

/// Unix socket the session helper listens on, relative to the working
/// directory. The node passes the absolute path in `ARUNA_SESSION_SOCKET`.
pub const SESSION_SOCKET_PATH: &str = ".aruna/session.sock";

/// Helper program every session image ships. Started by the image, and started
/// again in `SESSION_CLIENT_MODE` by an exec that bridges standard input and
/// output to the socket.
pub const SESSION_HELPER_PATH: &str = "/opt/aruna/session-helper";
pub const SESSION_CLIENT_MODE: &str = "session-client";

/// One interactive runtime a session job may pick. The node fills image,
/// entrypoint and command from this entry, so a caller never names an image.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct SessionRuntime {
    pub id: &'static str,
    pub label: &'static str,
    pub hint: &'static str,
    pub image: &'static str,
    pub command: &'static [&'static str],
    pub env: &'static [(&'static str, &'static str)],
    pub lang: &'static str,
}

pub const SESSION_RUNTIMES: [SessionRuntime; 2] = [
    SessionRuntime {
        id: "python-notebook",
        label: "Python notebook",
        hint: "IPython kernel with s3fs preconfigured for the workspace bucket.",
        image: "ghcr.io/arunaengine/aruna-session-python:0.1.0",
        command: &["/opt/aruna/session-start"],
        env: &[("PYTHONUNBUFFERED", "1")],
        lang: "python",
    },
    SessionRuntime {
        id: "deno-notebook",
        label: "Deno notebook",
        hint: "Deno kernel with the AWS SDK preconfigured for the workspace bucket.",
        image: "ghcr.io/arunaengine/aruna-session-deno:0.1.0",
        command: &["/opt/aruna/session-start"],
        env: &[("DENO_DIR", ".deno-cache")],
        lang: "typescript",
    },
];

pub fn session_runtime(id: &str) -> Option<&'static SessionRuntime> {
    SESSION_RUNTIMES.iter().find(|runtime| runtime.id == id)
}
