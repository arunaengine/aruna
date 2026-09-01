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
