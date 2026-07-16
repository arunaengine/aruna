use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};

const MAX_TRANSFER_BYTES: u64 = 4 * 1024 * 1024 * 1024;

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut arguments = std::env::args_os().skip(1);
    let command = arguments
        .next()
        .and_then(|value| value.into_string().ok())
        .ok_or_else(|| "expected stage, fetch, or probe".to_string())?;
    let arguments = arguments.collect::<Vec<_>>();
    match command.as_str() {
        "stage" => stage_command(&arguments),
        "fetch" => fetch_command(&arguments),
        "probe" => probe_command(&arguments),
        _ => Err(format!("unknown helper command `{command}`")),
    }
}

fn stage_command(arguments: &[std::ffi::OsString]) -> Result<(), String> {
    let workspace = required_path(arguments, "--workspace")?;
    let sentinel = required_path(arguments, "--sentinel")?;
    let marker = required_value(arguments, "--marker")?;
    stage_archive(io::stdin().lock(), &workspace, &sentinel, marker.as_bytes())
        .map_err(|error| format!("stage failed: {error}"))
}

fn fetch_command(arguments: &[std::ffi::OsString]) -> Result<(), String> {
    let workspace = required_path(arguments, "--workspace")?;
    let path = required_value(arguments, "--path")?;
    fetch_archive(&workspace, &path, io::stdout().lock())
        .map_err(|error| format!("fetch failed: {error}"))
}

fn probe_command(arguments: &[std::ffi::OsString]) -> Result<(), String> {
    if has_flag(arguments, "--hold") {
        loop {
            std::thread::park();
        }
    }
    if let Some(path) = optional_path(arguments, "--install")? {
        install_probe(&path).map_err(|error| format!("probe install failed: {error}"))?;
    }
    let marker = required_path(arguments, "--marker")?;
    let sentinel = required_path(arguments, "--sentinel")?;
    compare_marker(&marker, &sentinel).map_err(|error| format!("probe failed: {error}"))
}

fn stage_archive(
    reader: impl Read,
    workspace: &Path,
    sentinel: &Path,
    marker: &[u8],
) -> io::Result<()> {
    std::fs::create_dir_all(workspace)?;
    remove_sentinel(sentinel)?;
    clear_workspace(workspace)?;
    let mut archive = tar::Archive::new(reader);
    let mut transferred = 0u64;
    for entry in archive.entries()? {
        let mut entry = entry?;
        let relative = safe_relative(&entry.path()?)?;
        let target = workspace.join(&relative);
        match entry.header().entry_type() {
            kind if kind.is_dir() => {
                std::fs::create_dir_all(&target)?;
                std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o770))?;
                sync_parent(&target)?;
            }
            kind if kind.is_file() => {
                let size = entry.size();
                transferred = transferred
                    .checked_add(size)
                    .ok_or_else(|| io::Error::other("input size overflows"))?;
                if transferred > MAX_TRANSFER_BYTES {
                    return Err(io::Error::other("input transfer limit exceeded"));
                }
                if let Some(parent) = target.parent() {
                    std::fs::create_dir_all(parent)?;
                    reject_symlink_path(workspace, parent)?;
                }
                if target.symlink_metadata().is_ok() {
                    return Err(io::Error::other("archive path already exists"));
                }
                let mut file = OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&target)?;
                let copied = io::copy(&mut entry, &mut file)?;
                if copied != size {
                    return Err(io::Error::other("archive entry size mismatch"));
                }
                file.sync_all()?;
                std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o440))?;
                sync_parent(&target)?;
            }
            _ => return Err(io::Error::other("archive entry type is not allowed")),
        }
    }
    let workspace_file = File::open(workspace)?;
    rustix::fs::syncfs(&workspace_file)?;
    write_sentinel(sentinel, marker)?;
    rustix::fs::syncfs(workspace_file)?;
    Ok(())
}

fn fetch_archive(workspace: &Path, path: &str, writer: impl Write) -> io::Result<()> {
    let relative = safe_absolute(path)?;
    let root = workspace.canonicalize()?;
    let source = root.join(&relative).canonicalize()?;
    if !source.starts_with(&root) || !source.symlink_metadata()?.file_type().is_file() {
        return Err(io::Error::other("output is not a regular workspace file"));
    }
    let size = source.metadata()?.len();
    if size > MAX_TRANSFER_BYTES {
        return Err(io::Error::other("output transfer limit exceeded"));
    }
    let mut header = tar::Header::new_gnu();
    header.set_entry_type(tar::EntryType::Regular);
    header.set_mode(0o440);
    header.set_size(size);
    header.set_cksum();
    let mut archive = tar::Builder::new(writer);
    archive.append_data(&mut header, relative, File::open(source)?)?;
    archive.finish()
}

fn compare_marker(marker: &Path, sentinel: &Path) -> io::Result<()> {
    let marker: serde_json::Value = serde_json::from_slice(&std::fs::read(marker)?)?;
    let sentinel: serde_json::Value = serde_json::from_slice(&std::fs::read(sentinel)?)?;
    if marker != sentinel {
        return Err(io::Error::other("stage marker and sentinel differ"));
    }
    Ok(())
}

fn install_probe(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let source = std::env::current_exe()?;
    let temp = path.with_extension("tmp");
    let mut input = File::open(source)?;
    let mut output = File::create(&temp)?;
    io::copy(&mut input, &mut output)?;
    output.sync_all()?;
    std::fs::set_permissions(&temp, std::fs::Permissions::from_mode(0o555))?;
    std::fs::rename(&temp, path)?;
    sync_parent(path)
}

fn write_sentinel(path: &Path, marker: &[u8]) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::other("sentinel path has no parent"))?;
    std::fs::create_dir_all(parent)?;
    let temp = path.with_extension("tmp");
    let mut file = File::create(&temp)?;
    file.write_all(marker)?;
    file.sync_all()?;
    std::fs::rename(&temp, path)?;
    sync_parent(path)
}

fn remove_sentinel(path: &Path) -> io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => sync_parent(path),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn clear_workspace(workspace: &Path) -> io::Result<()> {
    for entry in std::fs::read_dir(workspace)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_dir() && !file_type.is_symlink() {
            std::fs::remove_dir_all(entry.path())?;
        } else {
            std::fs::remove_file(entry.path())?;
        }
    }
    File::open(workspace)?.sync_all()
}

fn reject_symlink_path(workspace: &Path, path: &Path) -> io::Result<()> {
    let relative = path
        .strip_prefix(workspace)
        .map_err(|_| io::Error::other("archive path escaped workspace"))?;
    let mut current = workspace.to_path_buf();
    for component in relative.components() {
        current.push(component);
        if current.symlink_metadata()?.file_type().is_symlink() {
            return Err(io::Error::other("archive path crosses a symlink"));
        }
    }
    Ok(())
}

fn safe_relative(path: &Path) -> io::Result<PathBuf> {
    if path.as_os_str().is_empty()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(io::Error::other("archive path is not a safe relative path"));
    }
    Ok(path.to_path_buf())
}

fn safe_absolute(path: &str) -> io::Result<PathBuf> {
    let path = Path::new(path);
    if !path.is_absolute() {
        return Err(io::Error::other("output path is not absolute"));
    }
    let mut relative = PathBuf::new();
    for component in path.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(part) => relative.push(part),
            _ => return Err(io::Error::other("output path has an unsafe component")),
        }
    }
    if relative.as_os_str().is_empty() {
        return Err(io::Error::other("output path is root"));
    }
    Ok(relative)
}

fn required_value(arguments: &[std::ffi::OsString], name: &str) -> Result<String, String> {
    option_value(arguments, name)?
        .and_then(|value| value.into_string().ok())
        .ok_or_else(|| format!("{name} is required and must be UTF-8"))
}

fn required_path(arguments: &[std::ffi::OsString], name: &str) -> Result<PathBuf, String> {
    optional_path(arguments, name)?.ok_or_else(|| format!("{name} is required"))
}

fn optional_path(arguments: &[std::ffi::OsString], name: &str) -> Result<Option<PathBuf>, String> {
    option_value(arguments, name).map(|value| value.map(PathBuf::from))
}

fn option_value(
    arguments: &[std::ffi::OsString],
    name: &str,
) -> Result<Option<std::ffi::OsString>, String> {
    let mut index = 0;
    while index < arguments.len() {
        if arguments[index] == name {
            return arguments
                .get(index + 1)
                .cloned()
                .map(Some)
                .ok_or_else(|| format!("{name} has no value"));
        }
        index += if arguments[index].to_string_lossy().starts_with("--") {
            2
        } else {
            1
        };
    }
    Ok(None)
}

fn has_flag(arguments: &[std::ffi::OsString], name: &str) -> bool {
    arguments.iter().any(|argument| argument == name)
}

fn sync_parent(path: &Path) -> io::Result<()> {
    File::open(
        path.parent()
            .ok_or_else(|| io::Error::other("path has no parent"))?,
    )?
    .sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unsafe_path() {
        assert!(safe_relative(Path::new("../escape")).is_err());
        assert!(safe_absolute("/safe/path").is_ok());
    }

    #[test]
    fn compares_stage_marker() {
        let directory = tempfile::tempdir().unwrap();
        let marker = directory.path().join("marker");
        let sentinel = directory.path().join("sentinel");
        std::fs::write(&marker, br#"{"epoch":1}"#).unwrap();
        std::fs::write(&sentinel, br#"{"epoch":1}"#).unwrap();
        assert!(compare_marker(&marker, &sentinel).is_ok());
        std::fs::write(&sentinel, br#"{"epoch":2}"#).unwrap();
        assert!(compare_marker(&marker, &sentinel).is_err());
    }

    #[test]
    fn stages_safe_archive() {
        let directory = tempfile::tempdir().unwrap();
        let workspace = directory.path().join("workspace");
        let sentinel = workspace.join(".aruna-stage");
        let mut bytes = Vec::new();
        {
            let mut archive = tar::Builder::new(&mut bytes);
            let mut header = tar::Header::new_gnu();
            header.set_size(4);
            header.set_mode(0o440);
            header.set_cksum();
            archive
                .append_data(&mut header, "input/data", &b"data"[..])
                .unwrap();
            archive.finish().unwrap();
        }
        stage_archive(&bytes[..], &workspace, &sentinel, br#"{"epoch":1}"#).unwrap();
        assert_eq!(
            std::fs::read(workspace.join("input/data")).unwrap(),
            b"data"
        );
        assert_eq!(std::fs::read(sentinel).unwrap(), br#"{"epoch":1}"#);
    }
}
