//! Reads an RO-Crate from its metadata file, a crate folder or a zip archive, and packs folders.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::error::CliError;
use serde_json::Value;
use std::io::Read;
use std::path::Path;

pub const METADATA_FILE: &str = "ro-crate-metadata.json";

/// Loads the crate's JSON-LD and checks it has a `@graph`.
pub fn load_crate(path: &Path) -> Result<Value, CliError> {
    let bytes = if path.is_dir() {
        std::fs::read(path.join(METADATA_FILE)).map_err(|error| {
            CliError::usage(format!(
                "{} has no {METADATA_FILE}: {error}",
                path.display()
            ))
        })?
    } else if is_archive(path)? {
        read_archive(path)?
    } else {
        std::fs::read(path)?
    };
    let value: Value = serde_json::from_slice(&bytes)
        .map_err(|error| CliError::usage(format!("{METADATA_FILE} is not JSON: {error}")))?;
    if !value.get("@graph").is_some_and(Value::is_array) {
        return Err(CliError::usage(format!(
            "{} is not an RO-Crate: it has no @graph list",
            path.display()
        )));
    }
    Ok(value)
}

/// Whether the file starts like a zip archive, whatever its name.
pub fn is_archive(path: &Path) -> Result<bool, CliError> {
    let mut magic = [0u8; 4];
    let mut file = std::fs::File::open(path)?;
    let read = file.read(&mut magic)?;
    Ok(read == 4 && magic == *b"PK\x03\x04")
}

/// The metadata file at the archive root, or inside its one top folder as ELN files have it.
fn read_archive(path: &Path) -> Result<Vec<u8>, CliError> {
    let file = std::fs::File::open(path)?;
    let mut archive = zip::ZipArchive::new(file)
        .map_err(|error| CliError::usage(format!("{}: {error}", path.display())))?;
    let name = archive
        .file_names()
        .filter_map(Result::ok)
        .map(|name| name.to_string())
        .filter(|name| {
            let parts: Vec<&str> = name.split('/').collect();
            parts.last() == Some(&METADATA_FILE) && parts.len() <= 2
        })
        .min_by_key(|name| name.len())
        .ok_or_else(|| {
            CliError::usage(format!("{} contains no {METADATA_FILE}", path.display()))
        })?;
    let mut entry = archive
        .by_name(&name)
        .map_err(|error| CliError::usage(format!("{name}: {error}")))?;
    let mut bytes = Vec::new();
    entry.read_to_end(&mut bytes)?;
    Ok(bytes)
}

/// The root data entity, which the metadata descriptor names in `about`.
fn root_entity(rocrate: &Value) -> Option<&Value> {
    let graph = rocrate.get("@graph")?.as_array()?;
    let root_id = graph
        .iter()
        .find(|entity| entity.get("@id").and_then(Value::as_str) == Some(METADATA_FILE))?
        .get("about")?
        .get("@id")?
        .as_str()?;
    graph
        .iter()
        .find(|entity| entity.get("@id").and_then(Value::as_str) == Some(root_id))
}

pub fn root_name(rocrate: &Value) -> Option<String> {
    let name = root_entity(rocrate)?.get("name")?.as_str()?.trim();
    (!name.is_empty()).then(|| name.to_string())
}

/// A metadata path from the root name, such as `datasets/proteomics-run-42`.
pub fn default_path(rocrate: &Value) -> Result<String, CliError> {
    let name = root_name(rocrate)
        .ok_or_else(|| CliError::usage("the crate's root has no name; pass --path"))?;
    let mut slug = String::new();
    for character in name.to_lowercase().chars() {
        match character.is_ascii_alphanumeric() {
            true => slug.push(character),
            false if !slug.ends_with('-') => slug.push('-'),
            false => {}
        }
    }
    let slug: String = slug.trim_matches('-').chars().take(64).collect();
    match slug.trim_end_matches('-') {
        "" => Err(CliError::usage(
            "the crate's name gives no path; pass --path",
        )),
        slug => Ok(format!("datasets/{slug}")),
    }
}

/// Adds a Profile IRI to the root's `conformsTo`, which selects the Profile the node checks.
pub fn add_profile(rocrate: &mut Value, profile: &str) -> Result<(), CliError> {
    let root_id = root_entity(rocrate)
        .and_then(|root| root.get("@id")?.as_str().map(str::to_string))
        .ok_or_else(|| CliError::usage("the crate has no root data entity"))?;
    let graph = rocrate
        .get_mut("@graph")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| CliError::usage("the crate has no @graph"))?;
    let root = graph
        .iter_mut()
        .find(|entity| entity.get("@id").and_then(Value::as_str) == Some(root_id.as_str()))
        .and_then(Value::as_object_mut)
        .ok_or_else(|| CliError::usage("the crate has no root data entity"))?;
    let reference = serde_json::json!({ "@id": profile });
    let list = match root.remove("conformsTo") {
        None => vec![reference],
        Some(Value::Array(mut items)) => {
            items.push(reference);
            items
        }
        Some(single) => vec![single, reference],
    };
    root.insert("conformsTo".into(), Value::Array(list));
    Ok(())
}

/// Packs a crate folder into an uncompressed zip archive at `dest`.
pub fn pack_folder(dir: &Path, dest: &Path) -> Result<usize, CliError> {
    if !dir.join(METADATA_FILE).is_file() {
        return Err(CliError::usage(format!(
            "{} has no {METADATA_FILE}",
            dir.display()
        )));
    }
    let files = crate::sync::walk_local(dir)?;
    let mut writer = zip::ZipWriter::new(std::fs::File::create(dest)?);
    let options = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Stored)
        .large_file(true);
    for file in &files {
        writer
            .start_file(file.path.as_str(), options)
            .map_err(|error| CliError::Io(std::io::Error::other(error)))?;
        let mut source = std::fs::File::open(dir.join(&file.path))?;
        std::io::copy(&mut source, &mut writer)?;
    }
    writer
        .finish()
        .map_err(|error| CliError::Io(std::io::Error::other(error)))?;
    Ok(files.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Value {
        serde_json::json!({
            "@context": "https://w3id.org/ro/crate/1.2/context",
            "@graph": [
                {"@id": "ro-crate-metadata.json", "@type": "CreativeWork",
                 "about": {"@id": "./"}},
                {"@id": "./", "@type": "Dataset", "name": "Run 42: LC/MS!"}
            ]
        })
    }

    #[test]
    fn loads_every_form() {
        let dir = tempfile::tempdir().unwrap();
        let folder = dir.path().join("crate");
        std::fs::create_dir_all(folder.join("data")).unwrap();
        let text = serde_json::to_vec(&sample()).unwrap();
        std::fs::write(folder.join(METADATA_FILE), &text).unwrap();
        std::fs::write(folder.join("data/a.csv"), b"x,y\n").unwrap();

        assert_eq!(load_crate(&folder.join(METADATA_FILE)).unwrap(), sample());
        assert_eq!(load_crate(&folder).unwrap(), sample());

        let archive = dir.path().join("crate.zip");
        assert_eq!(pack_folder(&folder, &archive).unwrap(), 2);
        assert!(is_archive(&archive).unwrap());
        assert_eq!(load_crate(&archive).unwrap(), sample());
    }

    #[test]
    fn loads_nested_archive() {
        let dir = tempfile::tempdir().unwrap();
        let archive = dir.path().join("export.eln");
        let mut writer = zip::ZipWriter::new(std::fs::File::create(&archive).unwrap());
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);
        writer
            .start_file("export/ro-crate-metadata.json", options)
            .unwrap();
        serde_json::to_writer(&mut writer, &sample()).unwrap();
        writer.finish().unwrap();
        assert_eq!(load_crate(&archive).unwrap(), sample());
    }

    #[test]
    fn rejects_non_crate() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("plain.json");
        std::fs::write(&path, b"{\"name\": 1}").unwrap();
        let error = load_crate(&path).unwrap_err().to_string();
        assert!(error.contains("no @graph"), "{error}");
        assert!(load_crate(dir.path()).is_err());
    }

    #[test]
    fn adjusts_crate_root() {
        let mut rocrate = sample();
        assert_eq!(default_path(&rocrate).unwrap(), "datasets/run-42-lc-ms");
        add_profile(&mut rocrate, "https://w3id.org/aruna/profile/p1").unwrap();
        let root = &rocrate["@graph"][1];
        assert_eq!(
            root["conformsTo"][0]["@id"],
            "https://w3id.org/aruna/profile/p1"
        );
    }
}
