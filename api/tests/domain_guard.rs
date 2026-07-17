use std::fs;
use std::path::Path;
use std::process::Command;

#[test]
fn rejects_legacy_domain() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("api crate has workspace parent");
    let forbidden = ["aruna", ".", "io"].concat().into_bytes();
    let files = Command::new("git")
        .args([
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "-z",
        ])
        .current_dir(root)
        .output()
        .expect("git must be available for the repository guard");
    assert!(
        files.status.success(),
        "git ls-files failed: {}",
        String::from_utf8_lossy(&files.stderr)
    );

    let mut matches = files
        .stdout
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .map(|path| std::str::from_utf8(path).expect("repository paths must be UTF-8"))
        .filter(|path| root.join(path).is_file())
        .filter(|path| {
            let bytes = fs::read(root.join(path))
                .unwrap_or_else(|error| panic!("failed to read {path}: {error}"));
            bytes
                .windows(forbidden.len())
                .any(|window| window == forbidden)
        })
        .map(str::to_owned)
        .collect::<Vec<_>>();
    matches.sort();

    assert!(
        matches.is_empty(),
        "legacy domain found in: {}",
        matches.join(", ")
    );
}
