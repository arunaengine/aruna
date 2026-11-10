//! Packages guests in both input forms and refuses components the host cannot run.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#![cfg(feature = "build")]

use aruna_plugins::artifact::{read_handlers, read_manifest};
use aruna_plugins::manifest::{Manifest, ManifestError};
use aruna_plugins::package::{BuildError, package};
use aruna_plugins::trigger::TriggerKind;
use wasm_encoder::{ComponentSection, CustomSection};
use wasmparser::{Parser, Payload};
use wit_component::{ComponentEncoder, StringEncoding, embed_component_metadata};
use wit_parser::{ManglingAndAbi, Resolve};

const FIXTURE: &[u8] = include_bytes!("fixture.wasm");

/// The fixture component without its manifest section, as a toolchain hands it over.
fn finished() -> (Vec<u8>, Manifest) {
    let manifest = read_manifest(FIXTURE).unwrap();
    let mut section = Vec::new();
    CustomSection {
        name: "aruna-plugin".into(),
        data: serde_json::to_vec(&manifest).unwrap().into(),
    }
    .append_to_component(&mut section);
    let component = FIXTURE.strip_suffix(section.as_slice()).unwrap().to_vec();
    (component, manifest)
}

/// The fixture's core module with its WIT embedded again, as cargo hands it over.
fn module() -> Vec<u8> {
    let (component, _) = finished();
    let module = (Parser::new(0).parse_all(&component))
        .filter_map(|payload| match payload.unwrap() {
            Payload::ModuleSection {
                unchecked_range, ..
            } => Some(
                component[unchecked_range.start as usize..unchecked_range.end as usize].to_vec(),
            ),
            _ => None,
        })
        .find(|module| read_handlers(module).unwrap().is_some())
        .expect("the module that lists its handlers");
    let wit_component::DecodedWasm::Component(resolve, world) =
        wit_component::decode(&component).unwrap()
    else {
        panic!("a component");
    };
    let mut module = module;
    embed_component_metadata(&mut module, &resolve, world, StringEncoding::UTF8).unwrap();
    module
}

/// A manifest whose dataset trigger names a handler the guest does not have.
fn absent(mut manifest: Manifest) -> Manifest {
    let created = manifest
        .triggers
        .get_mut(&TriggerKind::DatasetCreated)
        .unwrap();
    created.handler = Some("absent".into());
    manifest.abi = None;
    manifest
}

#[test]
fn forms_match_handlers() {
    let (component, manifest) = finished();
    for (form, bytes) in [("component", component), ("module", module())] {
        let mut ok = manifest.clone();
        assert!(package(&mut ok, &bytes).is_ok(), "{form}");
        let mut wrong = absent(manifest.clone());
        match package(&mut wrong, &bytes) {
            Err(BuildError::Manifest(error)) => {
                assert_eq!(
                    error,
                    ManifestError::UnknownHandler("absent".into()).to_string(),
                    "{form}"
                );
            }
            other => panic!("{form}: {other:?}"),
        }
    }
}

/// A component of `world` with trapping bodies, built the way toolchains build components.
fn dummy(wit: &str, world: &str) -> Vec<u8> {
    let mut resolve = Resolve::default();
    let package = resolve.push_str("test.wit", wit).unwrap();
    let world = resolve.select_world(&[package], Some(world)).unwrap();
    let mut module = wit_component::dummy_module(&resolve, world, ManglingAndAbi::Standard32);
    embed_component_metadata(&mut module, &resolve, world, StringEncoding::UTF8).unwrap();
    ComponentEncoder::default()
        .module(&module)
        .unwrap()
        .validate(true)
        .encode()
        .unwrap()
}

fn refused(wit: &str, world: &str) -> String {
    let (_, manifest) = finished();
    let mut manifest = manifest;
    manifest.abi = None;
    manifest.handlers.clear();
    match package(&mut manifest, &dummy(wit, world)) {
        Err(BuildError::Host(error)) => error,
        other => panic!("{world}: {other:?}"),
    }
}

#[test]
fn refuses_unrunnable_components() {
    let missing = "package test:bad; world empty { import aruna:plugin/log@0.2.0; }
        package aruna:plugin@0.2.0 { interface log { enum level { debug, info, warn, error } log: func(level: level, message: string); } }";
    assert!(
        refused(missing, "empty").contains("handle"),
        "missing export"
    );
    let signature =
        "package test:bad; world shape { export handle: func(handler: string) -> string; }";
    assert!(
        refused(signature, "shape").contains("handle"),
        "wrong signature"
    );
    let import = "package test:bad; world shout { import aruna:plugin/log@0.2.0; export handle: func(handler: string) -> string; }
        package aruna:plugin@0.2.0 { interface log { shout: func(message: string); } }";
    assert!(!refused(import, "shout").is_empty(), "unsupported import");
}
