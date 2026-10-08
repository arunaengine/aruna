//! Exercises holder-issued scoped reads over REST while the bucket stays locked.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#![recursion_limit = "256"]
mod shared;

use aruna_core::UserId;
use aruna_core::compute::{SecretBytes, SharedSecret};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::key_seal::{SealedSecret, open_sealed, seal_to};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction, Permission};
use aruna_core::structs::storage::abe::{
    AbeError, GRANT_PURPOSE, SysRng, create_parameters, derive_master, setup_context,
};
use aruna_core::structs::storage::abe_access::{
    GrantContext, KeyGrant, KeyScope, MAX_REQUESTS, MAX_WRITES,
};
use aruna_core::structs::storage::blob::{bucket_permission_path, group_permission_path};
use aruna_core::structs::storage::encryption::{BucketKeyRef, copy_info, public_key_of};
use aruna_kpabe::{Attribute, Envelope, Policy, UserKey};
use aruna_operations::abe::copies::complete_copies;
use aruna_operations::abe::rekey::rekey_page;
use aruna_operations::abe::{
    KeyAction, KeyError, KeyOperation, KeyResult, MemberKeysOperation, ReissueOperation,
};
use aruna_operations::driver::drive;
use aruna_operations::s3::bucket::key::unlock::{UnlockBucketOperation, UnlockInput};
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use base64::{Engine, engine::general_purpose::STANDARD};
use reqwest::StatusCode;
use serde_json::{Value, json};
use shared::{
    S3Credentials, SeedNode, TestResult, create_group_http, create_s3_credentials, s3_client,
    sign_scoped_token, sign_token, spawn_complete_seed, spawn_fixed_seed,
};
use ulid::Ulid;

const BUCKET: &str = "abe-access";

async fn create_bearer_token(
    context: &aruna_operations::driver::DriverContext,
    user_id: UserId,
    realm_id: aruna_core::structs::identity::realm::RealmId,
    capabilities: aruna_core::structs::identity::auth::NodeCapabilities,
) -> TestResult<String> {
    shared::add_user(context, user_id).await?;
    shared::create_bearer_token(context, user_id, realm_id, capabilities).await
}

fn bytes(value: &Value) -> Vec<u8> {
    STANDARD.decode(value.as_str().unwrap()).unwrap()
}

fn query_url(route: &str, query: &[(&str, &str)]) -> TestResult<reqwest::Url> {
    let mut url = reqwest::Url::parse(route)?;
    url.query_pairs_mut().extend_pairs(query.iter().copied());
    Ok(url)
}

#[test]
fn abe_master() {
    let realm = aruna_core::structs::identity::realm::RealmId::from_bytes([1; 32]);
    let node = iroh::SecretKey::from_bytes(&[2; 32]).public();
    let key = BucketKeyRef::new(Ulid::from_bytes([3; 16]), 1);
    let mut raw = [4; 32];
    raw[0] |= 7;
    raw[31] = 255;
    let mut clamped = raw;
    clamped[0] &= 248;
    clamped[31] &= 127;
    clamped[31] |= 64;
    let a = create_parameters(&SecretBytes::new(raw.to_vec()), realm, node, key).unwrap();
    let b = create_parameters(&SecretBytes::new(clamped.to_vec()), realm, node, key).unwrap();
    assert_eq!(a, b);
    let changed = create_parameters(
        &SecretBytes::new(clamped.to_vec()),
        realm,
        node,
        BucketKeyRef::new(key.bucket_id, 2),
    )
    .unwrap();
    assert_ne!(a.fingerprint, changed.fingerprint);
    assert_eq!(
        a.recompute(&SecretBytes::new(vec![9; 32])).err(),
        Some(AbeError::Parameters)
    );
    let mut substituted = a.clone();
    substituted.fingerprint = changed.fingerprint;
    assert!(substituted.public().is_err());
    let context = setup_context(realm, node, key).unwrap();
    let (parameters, master) =
        derive_master(&SecretBytes::new(clamped.to_vec()), &context).unwrap();
    let policy = KeyScope::Subtree("foo/".into()).policy(&a, &[1]).unwrap();
    let user = aruna_kpabe::issue(&parameters, &master, &policy, &mut SysRng).unwrap();
    for epoch in [1, 2] {
        let attrs = [
            Attribute::Domain(a.domain().unwrap()),
            Attribute::Epoch(epoch),
            Attribute::Prefix(b"foo/".to_vec()),
        ];
        let sealed =
            aruna_kpabe::seal(&parameters, &attrs, &[8; 32], b"envelope", &mut SysRng).unwrap();
        assert_eq!(
            aruna_kpabe::open(&parameters, &user, &sealed, b"envelope").is_ok(),
            epoch == 1
        );
        assert!(aruna_kpabe::open(&parameters, &user, &sealed, b"substituted").is_err());
    }
    assert!(Policy::new(&a.domain().unwrap(), &[], &[]).is_err());
    let scope = KeyScope::Subtree("foo/".into());
    assert_eq!(
        postcard::from_bytes::<KeyScope>(&postcard::to_allocvec(&scope).unwrap()).unwrap(),
        scope
    );
}

#[tokio::test]
async fn abe_read() -> TestResult<()> {
    let seed = spawn_complete_seed().await?;
    let result = async {
        let client = reqwest::Client::new();
        let token = create_bearer_token(seed.context.as_ref(),seed.user_id,seed.realm_id,seed.capabilities.clone()).await?;
        let group = create_group_http(&seed.base_url,&token,"ABE access").await?;
        let credentials = create_s3_credentials(&seed.base_url,&token,&group.group_id).await?;
        let s3 = s3_client(seed.s3.as_ref().unwrap(),&credentials);
        s3.create_bucket().bucket(BUCKET).send().await?;
        let user_private = SecretBytes::new(vec![7;32]);
        let user_public = public_key_of(&user_private).unwrap();
        let response = client.post(format!("{}/api/v1/access/users/me/keys",seed.base_url)).bearer_auth(&token)
            .json(&json!({"key_id":"abe-client","public_key":STANDARD.encode(user_public),"has_recovery":true})).send().await?;
        assert_eq!(response.status(),StatusCode::CREATED);
        let key_record: Value = response.json().await?;
        let encryption = format!("{}/api/v1/data/buckets/{BUCKET}/storage/encryption",seed.base_url);
        let response = client.put(&encryption).bearer_auth(&token)
            .json(&json!({"mode":"vault_locked","expected_generation":0})).send().await?;
        let status = response.status(); let settings: Value = response.json().await?;
        assert_eq!(status,StatusCode::OK,"{settings}");
        let response = client.get(format!("{encryption}/copies/me?generation={}",settings["key_generation"].as_u64().unwrap())).bearer_auth(&token).send().await?;
        assert_eq!(response.status(),StatusCode::OK);
        let copies: Value = response.json().await?;
        let copy = &copies["copies"][0];
        let bucket_id = Ulid::from_string(settings["bucket_id"].as_str().unwrap())?;
        let generation = settings["key_generation"].as_u64().unwrap();
        let reference = BucketKeyRef::new(bucket_id,generation);
        let recipient_record = Ulid::from_string(key_record["record_id"].as_str().unwrap())?;
        let info = copy_info(seed.realm_id,seed.net.node_id(),reference,seed.user_id,recipient_record);
        let private: &[u8;32] = user_private.expose().try_into()?;
        let opened = open_sealed(private,&SealedSecret { enc:bytes(&copy["enc"]).try_into().unwrap(),
            ciphertext:bytes(&copy["ciphertext"]) },&info,&[])?;
        let bucket_private = SecretBytes::new(opened.to_vec());
        let upload = s3.put_object().bucket(BUCKET).key("foo/data").body(b"scoped bytes".to_vec().into()).send().await?;
        let version = upload.version_id().unwrap().to_string();
        let other = s3.put_object().bucket(BUCKET).key("foobar/data").body(b"other bytes".to_vec().into()).send().await?;
        let other_version = other.version_id().unwrap().to_string();
        let source = format!("{BUCKET}/foo/data?versionId={version}");
        let copied = s3.copy_object().bucket(BUCKET).key("bar/copy").copy_source(&source).send().await?;
        let bar_version = copied.version_id().unwrap().to_string();
        let multi = s3.create_multipart_upload().bucket(BUCKET).key("foo/multi").send().await?;
        let upload_id = multi.upload_id().unwrap();
        let part = s3.upload_part().bucket(BUCKET).key("foo/multi").upload_id(upload_id).part_number(1)
            .body(b"multipart bytes".to_vec().into()).send().await?;
        // Part 2 starts at an unaligned offset, so this archive publishes without a content hash.
        let mut expected = vec![b'p';5*1024*1024];
        let pending = s3.create_multipart_upload().bucket(BUCKET).key("foo/pending").send().await?;
        let pending_id = pending.upload_id().unwrap();
        let mut pending_parts = Vec::new();
        for (number,body) in [(1,expected.clone()),(2,b"tail".to_vec())] {
            let part = s3.upload_part().bucket(BUCKET).key("foo/pending").upload_id(pending_id).part_number(number)
                .body(body.into()).send().await?;
            pending_parts.push(CompletedPart::builder().part_number(number).e_tag(part.e_tag().unwrap()).build());
        }
        expected.extend_from_slice(b"tail");
        let response = client.post(format!("{encryption}/lock")).bearer_auth(&token).send().await?;
        assert!(response.status().is_success());
        // Copies made while locked wait for their envelope; foo/c copies the pending foo/b.
        let copied = s3.copy_object().bucket(BUCKET).key("foo/copy").copy_source(&source).send().await?;
        let copy_version = copied.version_id().unwrap().to_string();
        let copied = s3.copy_object().bucket(BUCKET).key("foo/b").copy_source(&source).send().await?;
        let b_version = copied.version_id().unwrap().to_string();
        let copied = s3.copy_object().bucket(BUCKET).key("foo/c")
            .copy_source(format!("{BUCKET}/foo/b?versionId={b_version}")).send().await?;
        let c_version = copied.version_id().unwrap().to_string();
        // The upload's envelope was made at create, so it completes without any private key.
        let parts = CompletedMultipartUpload::builder().parts(CompletedPart::builder().part_number(1)
            .e_tag(part.e_tag().unwrap()).build()).build();
        let completed = s3.complete_multipart_upload().bucket(BUCKET).key("foo/multi").upload_id(upload_id)
            .multipart_upload(parts).send().await?;
        let multi_version = completed.version_id().unwrap().to_string();
        let completed = s3.complete_multipart_upload().bucket(BUCKET).key("foo/pending").upload_id(pending_id)
            .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(pending_parts)).build()).send().await?;
        let pending_version = completed.version_id().unwrap().to_string();
        let request_route = format!("{}/api/v1/data/buckets/{BUCKET}/abe/requests",seed.base_url);
        let response = client.post(&request_route).bearer_auth(&token)
            .json(&json!({"scope":{"kind":"subtree","value":"foo/"}})).send().await?;
        assert_eq!(response.status(),StatusCode::ACCEPTED);
        let pending: Value = response.json().await?;
        let request_id = pending["fields"]["request_id"].as_str().unwrap();
        let response = client.get(&request_route).bearer_auth(&token).send().await?;
        assert_eq!(response.status(),StatusCode::OK);
        let requests: Value = response.json().await?;
        let proposal = &requests["records"][0];
        let context: GrantContext = postcard::from_bytes(&bytes(&proposal["record"]))?;
        assert_eq!(context.request.request_id.to_string(),request_id);
        let parameters = context.request.parameters.public()?;
        let master = context.request.parameters.recompute(&bucket_private)?;
        let policy = context.request.scope.policy(&context.request.parameters,&context.request.epochs)?;
        let key = aruna_kpabe::issue(&parameters,&master,&policy,&mut SysRng)?;
        let aad = context.bytes()?;
        assert_eq!(aad,bytes(&proposal["aad"]));
        let sealed = key.seal(|plain|seal_to(&user_public,GRANT_PURPOSE,&aad,plain).map_err(|_|aruna_kpabe::Error))?;
        let grant_route = format!("{request_route}/{request_id}/grant");
        let mut forged = context.clone(); forged.request.epochs = vec![2];
        let response = client.post(&grant_route).bearer_auth(&token).json(&json!({
            "context":STANDARD.encode(postcard::to_allocvec(&forged)?),"enc":STANDARD.encode(sealed.enc),
            "ciphertext":STANDARD.encode(&sealed.ciphertext)})).send().await?;
        assert_eq!(response.status(),StatusCode::CONFLICT);
        let response = client.post(&grant_route).bearer_auth(&token).json(&json!({
            "context":proposal["record"],"enc":STANDARD.encode(sealed.enc),"ciphertext":STANDARD.encode(sealed.ciphertext)})).send().await?;
        let status = response.status(); let admitted: Value = response.json().await?;
        assert_eq!(status,StatusCode::OK,"{admitted}");
        let grant: KeyGrant = KeyGrant::from_bytes(&bytes(&admitted["record"]))?;
        let transport = [&grant.enc[..],&grant.ciphertext].concat();
        let key = UserKey::open(&parameters,&transport,|_|open_sealed(private,
            &SealedSecret {enc:grant.enc,ciphertext:grant.ciphertext.clone()},GRANT_PURPOSE,&grant.context.bytes().unwrap())
            .map_err(|_|aruna_kpabe::Error))?;
        let envelope_route = format!("{}/api/v1/data/blobs/envelope",seed.base_url);
        let query = [("bucket",BUCKET),("key","foo/data"),("version_id",&version)];
        let response = client.get(query_url(&envelope_route,&query)?).bearer_auth(&token).send().await?;
        let status = response.status(); let env: Value = response.json().await?;
        assert_eq!(status,StatusCode::OK,"{env}");
        let cipher = Envelope::from_bytes(&parameters,&bytes(&env["envelope"]["abe"]))?;
        let object = aruna_kpabe::open(&parameters,&key,&cipher,&bytes(&env["context"]["bytes"]))?;
        let object_header = STANDARD.encode(object.as_bytes());
        let content_route = format!("{}/api/v1/data/blobs/content",seed.base_url);
        let response = client.get(query_url(&content_route,&query)?).bearer_auth(&token).send().await?;
        assert_eq!(response.status(),StatusCode::LOCKED);
        let response = client.get(query_url(&content_route,&query)?).bearer_auth(&token)
            .header("x-aruna-object-key",&object_header).send().await?;
        assert_eq!(response.status(),StatusCode::OK);
        assert_eq!(response.bytes().await?.as_ref(),b"scoped bytes");
        let response = client.get(query_url(&content_route,&query)?).bearer_auth(&token)
            .header("x-aruna-object-key",&object_header).header("Range","bytes=1-5").send().await?;
        assert_eq!(response.status(),StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.headers()["content-range"],"bytes 1-5/12");
        assert_eq!(response.bytes().await?.as_ref(),b"coped");
        let multi_query = [("bucket",BUCKET),("key","foo/multi"),("version_id",&multi_version)];
        let response = client.get(query_url(&envelope_route,&multi_query)?).bearer_auth(&token).send().await?;
        let status = response.status(); let env: Value = response.json().await?;
        assert_eq!(status,StatusCode::OK,"{env}");
        let cipher = Envelope::from_bytes(&parameters,&bytes(&env["envelope"]["abe"]))?;
        let multi_key = aruna_kpabe::open(&parameters,&key,&cipher,&bytes(&env["context"]["bytes"]))?;
        let response = client.get(query_url(&content_route,&multi_query)?).bearer_auth(&token)
            .header("x-aruna-object-key",STANDARD.encode(multi_key.as_bytes())).send().await?;
        assert_eq!(response.status(),StatusCode::OK);
        assert_eq!(response.bytes().await?.as_ref(),b"multipart bytes");
        // The first keyed read promotes the pending archive under its object key; the second reads it.
        let pending_query = [("bucket",BUCKET),("key","foo/pending"),("version_id",&pending_version)];
        for _ in 0..2 {
            let response = client.get(query_url(&envelope_route,&pending_query)?).bearer_auth(&token).send().await?;
            let status = response.status(); let env: Value = response.json().await?;
            assert_eq!(status,StatusCode::OK,"{env}");
            let cipher = Envelope::from_bytes(&parameters,&bytes(&env["envelope"]["abe"]))?;
            let pending_key = aruna_kpabe::open(&parameters,&key,&cipher,&bytes(&env["context"]["bytes"]))?;
            let response = client.get(query_url(&content_route,&pending_query)?).bearer_auth(&token)
                .header("x-aruna-object-key",STANDARD.encode(pending_key.as_bytes())).send().await?;
            assert_eq!(response.status(),StatusCode::OK);
            assert!(response.bytes().await?.as_ref() == expected.as_slice());
        }
        let wrong_query = [("bucket",BUCKET),("key","foobar/data"),("version_id",&other_version)];
        let response = client.get(query_url(&content_route,&wrong_query)?).bearer_auth(&token)
            .header("x-aruna-object-key",&object_header).send().await?;
        assert_eq!(response.status(),StatusCode::FORBIDDEN);
        let response = client.get(query_url(&envelope_route,&wrong_query)?).bearer_auth(&token).send().await?;
        let env: Value = response.json().await?;
        let cipher = Envelope::from_bytes(&parameters,&bytes(&env["envelope"]["abe"]))?;
        assert!(aruna_kpabe::open(&parameters,&key,&cipher,&bytes(&env["context"]["bytes"])).is_err());
        let response = client.get(&encryption).bearer_auth(&token).send().await?;
        let status: Value = response.json().await?;
        assert_eq!(status["unlock"]["state"],"locked");
        assert_eq!(status["bucket_id"],bucket_id.to_string());
        // The unlocked copy has its own envelope for bar/copy, which the foo/ key does not open.
        let bar_query = [("bucket",BUCKET),("key","bar/copy"),("version_id",&bar_version)];
        let response = client.get(query_url(&envelope_route,&bar_query)?).bearer_auth(&token).send().await?;
        assert_eq!(response.status(),StatusCode::OK);
        let env: Value = response.json().await?;
        let cipher = Envelope::from_bytes(&parameters,&bytes(&env["envelope"]["abe"]))?;
        assert!(aruna_kpabe::open(&parameters,&key,&cipher,&bytes(&env["context"]["bytes"])).is_err());
        let copy_query = [("bucket",BUCKET),("key","foo/copy"),("version_id",&copy_version)];
        let response = client.get(query_url(&envelope_route,&copy_query)?).bearer_auth(&token).send().await?;
        assert_eq!(response.status(),StatusCode::CONFLICT);
        s3.delete_object().bucket(BUCKET).key("foo/b").version_id(&b_version).send().await?;
        s3.delete_object().bucket(BUCKET).key("foo/data").version_id(&version).send().await?;
        let response = client.post(format!("{encryption}/unlock?bucket_id={bucket_id}&generation={generation}"))
            .bearer_auth(&token).header("content-type","application/octet-stream")
            .body(bucket_private.expose().to_vec()).send().await?;
        assert!(response.status().is_success());
        // The unlock wrote both pending envelopes, so the foo/ key reads them.
        for (copy_key,copy_version) in [("foo/copy",&copy_version),("foo/c",&c_version)] {
            let query = [("bucket",BUCKET),("key",copy_key),("version_id",copy_version)];
            let response = client.get(query_url(&envelope_route,&query)?).bearer_auth(&token).send().await?;
            let status = response.status(); let env: Value = response.json().await?;
            assert_eq!(status,StatusCode::OK,"{env}");
            let cipher = Envelope::from_bytes(&parameters,&bytes(&env["envelope"]["abe"]))?;
            let object = aruna_kpabe::open(&parameters,&key,&cipher,&bytes(&env["context"]["bytes"]))?;
            let response = client.get(query_url(&content_route,&query)?).bearer_auth(&token)
                .header("x-aruna-object-key",STANDARD.encode(object.as_bytes())).send().await?;
            assert_eq!(response.status(),StatusCode::OK);
            assert_eq!(response.bytes().await?.as_ref(),b"scoped bytes");
        }
        let _ = group;
        Ok::<(),Box<dyn std::error::Error>>(())
    }.await;
    seed.shutdown().await;
    result
}

const RESTART_ROOT: &str = "ARUNA_ABE_RESTART_ROOT";

#[tokio::test]
async fn abe_restart() -> TestResult<()> {
    // A child process creates and uploads; this one reopens its state, completes while locked
    // and reads with the scoped object key.
    let root = tempfile::tempdir()?;
    let output = std::process::Command::new(std::env::current_exe()?)
        .args(["--ignored", "--exact", "abe_restart_child", "--nocapture"])
        .env(RESTART_ROOT, root.path())
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let state: Value = serde_json::from_slice(&std::fs::read(root.path().join("restart.json"))?)?;
    let seed = spawn_fixed_seed(root.path(), true).await?;
    let result = async {
        let client = reqwest::Client::new();
        let token = create_bearer_token(
            seed.context.as_ref(),
            seed.user_id,
            seed.realm_id,
            seed.capabilities.clone(),
        )
        .await?;
        let credentials = S3Credentials {
            access_key_id: state["access"].as_str().unwrap().into(),
            access_secret: state["secret"].as_str().unwrap().into(),
        };
        let s3 = s3_client(seed.s3.as_ref().unwrap(), &credentials);
        let parts = CompletedMultipartUpload::builder()
            .parts(
                CompletedPart::builder()
                    .part_number(1)
                    .e_tag(state["etag"].as_str().unwrap())
                    .build(),
            )
            .build();
        let completed = s3
            .complete_multipart_upload()
            .bucket(BUCKET)
            .key("foo/multi")
            .upload_id(state["upload_id"].as_str().unwrap())
            .multipart_upload(parts)
            .send()
            .await?;
        let version = completed.version_id().unwrap().to_string();
        let grant = KeyGrant::from_bytes(&bytes(&state["grant"]))?;
        let parameters = grant.context.request.parameters.public()?;
        let transport = [&grant.enc[..], &grant.ciphertext].concat();
        let key = UserKey::open(&parameters, &transport, |_| {
            open_sealed(
                &[7; 32],
                &SealedSecret {
                    enc: grant.enc,
                    ciphertext: grant.ciphertext.clone(),
                },
                GRANT_PURPOSE,
                &grant.context.bytes().unwrap(),
            )
            .map_err(|_| aruna_kpabe::Error)
        })?;
        let query = [
            ("bucket", BUCKET),
            ("key", "foo/multi"),
            ("version_id", &version),
        ];
        let envelope_route = format!("{}/api/v1/data/blobs/envelope", seed.base_url);
        let response = client
            .get(query_url(&envelope_route, &query)?)
            .bearer_auth(&token)
            .send()
            .await?;
        let status = response.status();
        let env: Value = response.json().await?;
        assert_eq!(status, StatusCode::OK, "{env}");
        let cipher = Envelope::from_bytes(&parameters, &bytes(&env["envelope"]["abe"]))?;
        let object =
            aruna_kpabe::open(&parameters, &key, &cipher, &bytes(&env["context"]["bytes"]))?;
        let content_route = format!("{}/api/v1/data/blobs/content", seed.base_url);
        let response = client
            .get(query_url(&content_route, &query)?)
            .bearer_auth(&token)
            .header("x-aruna-object-key", STANDARD.encode(object.as_bytes()))
            .send()
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.bytes().await?.as_ref(), b"restart bytes");
        let encryption = format!(
            "{}/api/v1/data/buckets/{BUCKET}/storage/encryption",
            seed.base_url
        );
        let status: Value = client
            .get(&encryption)
            .bearer_auth(&token)
            .send()
            .await?
            .json()
            .await?;
        assert_eq!(status["unlock"]["state"], "locked");
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    seed.shutdown().await;
    result
}

#[tokio::test]
#[ignore = "spawned by abe_restart"]
async fn abe_restart_child() -> TestResult<()> {
    let Some(root) = std::env::var_os(RESTART_ROOT) else {
        return Ok(());
    };
    let root = std::path::PathBuf::from(root);
    let seed = spawn_fixed_seed(&root, false).await?;
    let client = reqwest::Client::new();
    let token = create_bearer_token(
        seed.context.as_ref(),
        seed.user_id,
        seed.realm_id,
        seed.capabilities.clone(),
    )
    .await?;
    let group = create_group_http(&seed.base_url, &token, "ABE restart").await?;
    let credentials = create_s3_credentials(&seed.base_url, &token, &group.group_id).await?;
    let s3 = s3_client(seed.s3.as_ref().unwrap(), &credentials);
    s3.create_bucket().bucket(BUCKET).send().await?;
    let user_public = public_key_of(&SecretBytes::new(vec![7; 32])).unwrap();
    let response = client.post(format!("{}/api/v1/access/users/me/keys",seed.base_url)).bearer_auth(&token)
        .json(&json!({"key_id":"abe-client","public_key":STANDARD.encode(user_public),"has_recovery":true})).send().await?;
    let key_record: Value = response.json().await?;
    let encryption = format!(
        "{}/api/v1/data/buckets/{BUCKET}/storage/encryption",
        seed.base_url
    );
    let settings: Value = client
        .put(&encryption)
        .bearer_auth(&token)
        .json(&json!({"mode":"vault_locked","expected_generation":0}))
        .send()
        .await?
        .json()
        .await?;
    let generation = settings["key_generation"].as_u64().unwrap();
    let copies: Value = client
        .get(format!("{encryption}/copies/me?generation={generation}"))
        .bearer_auth(&token)
        .send()
        .await?
        .json()
        .await?;
    let copy = &copies["copies"][0];
    let reference = BucketKeyRef::new(
        Ulid::from_string(settings["bucket_id"].as_str().unwrap())?,
        generation,
    );
    let recipient_record = Ulid::from_string(key_record["record_id"].as_str().unwrap())?;
    let info = copy_info(
        seed.realm_id,
        seed.net.node_id(),
        reference,
        seed.user_id,
        recipient_record,
    );
    let opened = open_sealed(
        &[7; 32],
        &SealedSecret {
            enc: bytes(&copy["enc"]).try_into().unwrap(),
            ciphertext: bytes(&copy["ciphertext"]),
        },
        &info,
        &[],
    )?;
    let bucket_private = SecretBytes::new(opened.to_vec());
    let multi = s3
        .create_multipart_upload()
        .bucket(BUCKET)
        .key("foo/multi")
        .send()
        .await?;
    let upload_id = multi.upload_id().unwrap();
    let part = s3
        .upload_part()
        .bucket(BUCKET)
        .key("foo/multi")
        .upload_id(upload_id)
        .part_number(1)
        .body(b"restart bytes".to_vec().into())
        .send()
        .await?;
    let response = client
        .post(format!("{encryption}/lock"))
        .bearer_auth(&token)
        .send()
        .await?;
    assert!(response.status().is_success());
    let request_route = format!(
        "{}/api/v1/data/buckets/{BUCKET}/abe/requests",
        seed.base_url
    );
    let response = client
        .post(&request_route)
        .bearer_auth(&token)
        .json(&json!({"scope":{"kind":"subtree","value":"foo/"}}))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let requests: Value = client
        .get(&request_route)
        .bearer_auth(&token)
        .send()
        .await?
        .json()
        .await?;
    let proposal = &requests["records"][0];
    let context: GrantContext = postcard::from_bytes(&bytes(&proposal["record"]))?;
    let parameters = context.request.parameters.public()?;
    let master = context.request.parameters.recompute(&bucket_private)?;
    let policy = context
        .request
        .scope
        .policy(&context.request.parameters, &context.request.epochs)?;
    let key = aruna_kpabe::issue(&parameters, &master, &policy, &mut SysRng)?;
    let aad = context.bytes()?;
    let sealed = key.seal(|plain| {
        seal_to(&user_public, GRANT_PURPOSE, &aad, plain).map_err(|_| aruna_kpabe::Error)
    })?;
    let grant_route = format!("{request_route}/{}/grant", context.request.request_id);
    let response = client.post(&grant_route).bearer_auth(&token).json(&json!({
        "context":proposal["record"],"enc":STANDARD.encode(sealed.enc),"ciphertext":STANDARD.encode(sealed.ciphertext)})).send().await?;
    let status = response.status();
    let admitted: Value = response.json().await?;
    assert_eq!(status, StatusCode::OK, "{admitted}");
    let state = json!({"access":credentials.access_key_id,"secret":credentials.access_secret,
        "upload_id":upload_id,"etag":part.e_tag().unwrap(),"grant":admitted["record"]});
    std::fs::write(root.join("restart.json"), serde_json::to_vec(&state)?)?;
    let storage = seed.context.storage_handle.clone();
    seed.shutdown().await;
    storage.sync_all().await?;
    std::process::exit(0);
}

#[test]
fn abe_scopes() {
    use aruna_core::structs::identity::auth::{PathRestriction, Permission, Role};
    use aruna_operations::auth::permission_rules::{CollectedRole, PermissionRules};
    use std::collections::{HashMap, HashSet};
    let root = "/realm/g/group/data/node/bucket";
    let role = |patterns: &[(&str, Permission)], direct, public| CollectedRole {
        role: Role {
            role_id: Ulid::from_bytes([1; 16]),
            name: "scope".into(),
            assigned_users: HashSet::new(),
            permissions: patterns
                .iter()
                .map(|(p, v)| (format!("{root}/{p}"), v.clone()))
                .collect::<HashMap<_, _>>(),
        },
        direct,
        public,
    };
    let rules = PermissionRules::from_roles(
        vec![role(
            &[("**", Permission::WRITE), ("foobar/**", Permission::DENY)],
            true,
            false,
        )],
        None,
    )
    .unwrap();
    assert!(rules.admits_scope(root, &KeyScope::Subtree("foo/".into())));
    assert!(!rules.admits_scope(root, &KeyScope::Subtree("foobar/".into())));
    let restricted = [PathRestriction {
        pattern: format!("{root}/foo/**"),
        permission: Permission::READ,
    }];
    let rules = PermissionRules::from_roles(
        vec![role(&[("**", Permission::READ)], true, false)],
        Some(&restricted),
    )
    .unwrap();
    assert!(rules.admits_scope(root, &KeyScope::Subtree("foo/".into())));
    assert!(rules.admits_scope(root, &KeyScope::Exact("foo/a".into())));
    assert!(!rules.admits_scope(root, &KeyScope::Exact("foobar/a".into())));
    assert!(!rules.admits_scope(root, &KeyScope::Subtree(String::new())));
    let rules = PermissionRules::from_roles(
        vec![role(
            &[
                ("**", Permission::READ),
                ("foo/private/**", Permission::DENY),
            ],
            true,
            false,
        )],
        None,
    )
    .unwrap();
    assert!(!rules.admits_scope(root, &KeyScope::Subtree("foo/".into())));
    let rules = PermissionRules::from_roles(
        vec![role(&[("foo/**", Permission::READ)], false, true)],
        None,
    )
    .unwrap();
    assert!(rules.admits_scope(root, &KeyScope::Subtree("foo/".into())));
    assert!(rules.admits_scope(root, &KeyScope::Exact("foo//a".into())));
    let rules =
        PermissionRules::from_roles(vec![role(&[("**", Permission::WRITE)], false, true)], None)
            .unwrap();
    assert!(!rules.admits_scope(root, &KeyScope::Subtree(String::new())));
}

async fn add_key(base: &str, token: &str, key_id: &str, public: [u8; 32]) -> TestResult<Value> {
    let response = reqwest::Client::new()
        .post(format!("{base}/api/v1/access/users/me/keys"))
        .bearer_auth(token)
        .json(&json!({"key_id":key_id,"public_key":STANDARD.encode(public),"has_recovery":true}))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    Ok(response.json().await?)
}

async fn add_member(seed: &SeedNode, token: &str, group: &str, roles: Value) -> TestResult<String> {
    let user = UserId::local(Ulid::generate(), seed.realm_id);
    shared::add_user(seed.context.as_ref(), user).await?;
    let response = reqwest::Client::new()
        .post(format!(
            "{}/api/v1/access/groups/{group}/members",
            seed.base_url
        ))
        .bearer_auth(token)
        .json(&json!({"user_id":user.to_string(),"role_ids":roles}))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    create_bearer_token(
        seed.context.as_ref(),
        user,
        seed.realm_id,
        seed.capabilities.clone(),
    )
    .await
}

async fn send(request: reqwest::RequestBuilder) -> TestResult<(StatusCode, Value)> {
    let response = request.send().await?;
    let status = response.status();
    Ok((status, response.json().await.unwrap_or(Value::Null)))
}

fn submission(record: &Value, enc: [u8; 32], ciphertext: &[u8]) -> Value {
    json!({"context":record,"enc":STANDARD.encode(enc),"ciphertext":STANDARD.encode(ciphertext)})
}

#[tokio::test]
async fn abe_holders() -> TestResult<()> {
    let seed = spawn_complete_seed().await?;
    let result = async {
        let http = reqwest::Client::new();
        let base = seed.base_url.clone();
        let owner = create_bearer_token(seed.context.as_ref(), seed.user_id, seed.realm_id, seed.capabilities.clone()).await?;
        let group = create_group_http(&base, &owner, "ABE holders").await?;
        let admin = &group.roles.iter().find(|r| r.name == "admin").unwrap().role_id;
        let holder = add_member(&seed, &owner, &group.group_id, json!([admin])).await?;
        let reader = add_member(&seed, &owner, &group.group_id, Value::Null).await?;
        let reader_private = SecretBytes::new(vec![11; 32]);
        let reader_public = public_key_of(&reader_private).unwrap();
        add_key(&base, &reader, "reader-1", reader_public).await?;
        let owner_private = SecretBytes::new(vec![7; 32]);
        let owner_key = add_key(&base, &owner, "owner-1", public_key_of(&owner_private).unwrap()).await?;
        let credentials = create_s3_credentials(&base, &owner, &group.group_id).await?;
        let s3 = s3_client(seed.s3.as_ref().unwrap(), &credentials);
        s3.create_bucket().bucket(BUCKET).send().await?;
        let encryption = format!("{base}/api/v1/data/buckets/{BUCKET}/storage/encryption");
        let (status, settings) = send(http.put(&encryption).bearer_auth(&owner).json(&json!({"mode":"vault_locked","expected_generation":0}))).await?;
        assert_eq!(status, StatusCode::OK, "{settings}");
        let generation = settings["key_generation"].as_u64().unwrap();
        let bucket_id = Ulid::from_string(settings["bucket_id"].as_str().unwrap())?;
        let (_, copies) = send(http.get(format!("{encryption}/copies/me?generation={generation}")).bearer_auth(&owner)).await?;
        let copy = &copies["copies"][0];
        let record_id = Ulid::from_string(owner_key["record_id"].as_str().unwrap())?;
        let info = copy_info(seed.realm_id, seed.net.node_id(), BucketKeyRef::new(bucket_id, generation), seed.user_id, record_id);
        let owner_secret: &[u8; 32] = owner_private.expose().try_into()?;
        let opened = open_sealed(owner_secret, &SealedSecret { enc: bytes(&copy["enc"]).try_into().unwrap(), ciphertext: bytes(&copy["ciphertext"]) }, &info, &[])?;
        let bucket_private = SecretBytes::new(opened.to_vec());
        let upload = s3.put_object().bucket(BUCKET).key("foo/data").body(b"scoped bytes".to_vec().into()).send().await?;
        let version = upload.version_id().unwrap().to_string();
        let copied = s3.copy_object().bucket(BUCKET).key("foo/copy").copy_source(format!("{BUCKET}/foo/data")).send().await?;
        let copy_version = copied.version_id().unwrap().to_string();
        let requests = format!("{base}/api/v1/data/buckets/{BUCKET}/abe/requests");
        let grants = format!("{base}/api/v1/data/buckets/{BUCKET}/abe/grants");
        let subtree = json!({"scope":{"kind":"subtree","value":"foo/"}});
        let exact = json!({"scope":{"kind":"exact","value":"foo/data"}});

        // An unlocked bucket issues at once; repeating returns the same node grant.
        let (status, node_grant) = send(http.post(&requests).bearer_auth(&reader).json(&subtree)).await?;
        assert_eq!(status, StatusCode::OK, "{node_grant}");
        assert_eq!(node_grant["fields"]["issuer"]["kind"], "node");
        let (status, repeated) = send(http.post(&requests).bearer_auth(&reader).json(&subtree)).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(repeated["record"], node_grant["record"]);
        let response = http.post(format!("{encryption}/lock")).bearer_auth(&owner).send().await?;
        assert!(response.status().is_success());

        // An unlocked copy has its own envelope and a missing version is not found.
        let content = format!("{base}/api/v1/data/blobs/content");
        let envelope_route = format!("{base}/api/v1/data/blobs/envelope");
        let copy_query = [("bucket", BUCKET), ("key", "foo/copy"), ("version_id", &copy_version)];
        let (status, body) = send(http.get(query_url(&content, &copy_query)?).bearer_auth(&reader)).await?;
        assert_eq!((status, body["code"].as_str()), (StatusCode::LOCKED, Some("object_key_required")));
        let missing = Ulid::generate().to_string();
        let missing_query = [("bucket", BUCKET), ("key", "foo/data"), ("version_id", &missing)];
        let (status, _) = send(http.get(query_url(&envelope_route, &missing_query)?).bearer_auth(&reader)).await?;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // Holders see the reader's request; the reader is no issuer.
        let (status, pending) = send(http.post(&requests).bearer_auth(&reader).json(&exact)).await?;
        assert_eq!(status, StatusCode::ACCEPTED);
        let request_id = pending["fields"]["request_id"].as_str().unwrap().to_string();

        // A new own request notifies each holder once; the reused repeat sends nothing.
        let (status, again) = send(http.post(&requests).bearer_auth(&reader).json(&exact)).await?;
        assert_eq!((status, again["fields"]["request_id"].as_str()), (StatusCode::ACCEPTED, Some(request_id.as_str())));
        let inbox = format!("{base}/api/v1/system/notifications");
        let notices = |token: String| {
            let request = http.get(&inbox).bearer_auth(token);
            async move {
                let (_, list) = send(request).await.unwrap_or((StatusCode::OK, Value::Null));
                list["notifications"].as_array().into_iter().flatten().filter(|n| n["kind"] == "bucket_key_pending" && n["bucket"] == BUCKET).count()
            }
        };
        let (holders, notices) = (&[owner.clone(), holder.clone()], &notices);
        let notified = |count: usize| async move {
            for token in holders {
                let wait = std::time::Duration::from_millis(50);
                shared::wait_until("key pending notice", std::time::Duration::from_secs(120), wait, || async { notices(token.clone()).await >= count }).await?;
                // The outbox drains in order, so an extra notice would be here already.
                assert_eq!(notices(token.clone()).await, count);
            }
            Ok::<(), Box<dyn std::error::Error>>(())
        };
        notified(1).await?;
        let (status, _) = send(http.get(&requests).bearer_auth(&reader)).await?;
        assert_eq!(status, StatusCode::FORBIDDEN);
        let (_, owned) = send(http.get(&requests).bearer_auth(&owner)).await?;
        let (_, held) = send(http.get(&requests).bearer_auth(&holder)).await?;
        let owner_record = &owned["records"][0]["record"];
        let holder_record = &held["records"][0]["record"];
        let context: GrantContext = postcard::from_bytes(&bytes(owner_record))?;
        assert_eq!(context.request.request_id.to_string(), request_id);
        let parameters = context.request.parameters.public()?;
        let master = context.request.parameters.recompute(&bucket_private)?;
        let policy = context.request.scope.policy(&context.request.parameters, &context.request.epochs)?;
        let issued = aruna_kpabe::issue(&parameters, &master, &policy, &mut SysRng)?;
        let seal = |record: &Value| -> TestResult<Value> {
            let context: GrantContext = postcard::from_bytes(&bytes(record))?;
            let sealed = issued.seal(|plain| seal_to(&reader_public, GRANT_PURPOSE, &context.bytes().unwrap(), plain).map_err(|_| aruna_kpabe::Error))?;
            Ok(submission(record, sealed.enc, &sealed.ciphertext))
        };
        let grant_route = format!("{requests}/{request_id}/grant");
        let (status, _) = send(http.post(&grant_route).bearer_auth(&reader).json(&seal(owner_record)?)).await?;
        assert_eq!(status, StatusCode::FORBIDDEN);
        let mut substituted = context.clone();
        substituted.request.recipient_public = Some([9; 32]);
        let forged = json!(STANDARD.encode(postcard::to_allocvec(&substituted)?));
        let (status, _) = send(http.post(&grant_route).bearer_auth(&owner).json(&submission(&forged, [1; 32], &[1; 16]))).await?;
        assert_eq!(status, StatusCode::CONFLICT);

        // Two holders publish the same request; the second gets the first grant.
        let (status, admitted) = send(http.post(&grant_route).bearer_auth(&owner).json(&seal(owner_record)?)).await?;
        assert_eq!(status, StatusCode::OK, "{admitted}");
        let (status, replay) = send(http.post(&grant_route).bearer_auth(&holder).json(&seal(holder_record)?)).await?;
        assert_eq!(status, StatusCode::OK, "{replay}");
        assert_eq!(replay["record"], admitted["record"]);
        assert_eq!(replay["fields"]["issuer"]["id"], seed.user_id.to_string());
        let (status, repeated) = send(http.post(&requests).bearer_auth(&reader).json(&exact)).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(repeated["record"], admitted["record"]);

        // Only the recipient fetches grants, and the grant opens the locked object.
        let (_, own) = send(http.get(&grants).bearer_auth(&reader)).await?;
        assert_eq!(own["records"].as_array().unwrap().len(), 2);
        let (_, foreign) = send(http.get(&grants).bearer_auth(&owner)).await?;
        assert!(foreign["records"].as_array().unwrap().is_empty());
        let grant = KeyGrant::from_bytes(&bytes(&admitted["record"]))?;
        let reader_secret: &[u8; 32] = reader_private.expose().try_into()?;
        let transport = [&grant.enc[..], &grant.ciphertext].concat();
        let key = UserKey::open(&parameters, &transport, |_| open_sealed(reader_secret, &SealedSecret { enc: grant.enc, ciphertext: grant.ciphertext.clone() }, GRANT_PURPOSE, &grant.context.bytes().unwrap()).map_err(|_| aruna_kpabe::Error))?;
        let query = [("bucket", BUCKET), ("key", "foo/data"), ("version_id", &version)];
        let (_, envelope) = send(http.get(query_url(&envelope_route, &query)?).bearer_auth(&reader)).await?;
        let cipher = Envelope::from_bytes(&parameters, &bytes(&envelope["envelope"]["abe"]))?;
        let object = aruna_kpabe::open(&parameters, &key, &cipher, &bytes(&envelope["context"]["bytes"]))?;
        let header = STANDARD.encode(object.as_bytes());
        let response = http.get(query_url(&content, &query)?).header("x-aruna-object-key", &header).send().await?;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        for malformed in [vec!["AAAA"], vec![header.as_str(), header.as_str()]] {
            let mut request = http.get(query_url(&content, &query)?).bearer_auth(&reader);
            for value in malformed {
                request = request.header("x-aruna-object-key", value);
            }
            assert_eq!(request.send().await?.status(), StatusCode::BAD_REQUEST);
        }
        let response = http.get(query_url(&content, &query)?).bearer_auth(&reader).header("x-aruna-object-key", &header).send().await?;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.bytes().await?.as_ref(), b"scoped bytes");

        // A replacement recipient key invalidates grants while the old record remains.
        add_key(&base, &reader, "reader-2", public_key_of(&SecretBytes::new(vec![12; 32])).unwrap()).await?;
        let (_, own) = send(http.get(&grants).bearer_auth(&reader)).await?;
        assert!(own["records"].as_array().unwrap().is_empty());
        let (status, pending) = send(http.post(&requests).bearer_auth(&reader).json(&exact)).await?;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_ne!(pending["fields"]["request_id"].as_str(), Some(request_id.as_str()));
        notified(2).await?;

        // A key published between recipient lookup and grant commit aborts the publication.
        let (_, owned) = send(http.get(&requests).bearer_auth(&owner)).await?;
        let context: GrantContext = postcard::from_bytes(&bytes(&owned["records"][0]["record"]))?;
        let racing = KeyGrant { context: context.clone(), enc: [1; 32], ciphertext: vec![1; 16] };
        let auth = AuthContext { user_id: seed.user_id, realm_id: seed.realm_id, path_restrictions: None, session: None };
        let now = aruna_core::time::unix_timestamp_millis();
        let mut operation = KeyOperation::new(BUCKET.into(), auth, seed.net.node_id(), KeyAction::Publish(racing), now);
        let mut effects: std::collections::VecDeque<Effect> = operation.start().into_iter().collect();
        while let Some(effect) = effects.pop_front() {
            let Effect::Storage(effect) = effect else { panic!("unexpected effect {effect:?}") };
            if matches!(effect, StorageEffect::CommitTransaction { .. }) {
                add_key(&base, &reader, "reader-3", public_key_of(&SecretBytes::new(vec![13; 32])).unwrap()).await?;
            }
            let event = seed.context.storage_handle.send_storage_effect(effect).await;
            if !operation.is_complete() {
                effects.extend(operation.step(event));
            }
        }
        assert_eq!(operation.finalize(), Err(KeyError::Storage));
        let record = json!(STANDARD.encode(postcard::to_allocvec(&context)?));
        let route = format!("{requests}/{}/grant", context.request.request_id);
        let (status, _) = send(http.post(&route).bearer_auth(&owner).json(&submission(&record, [1; 32], &[1; 16]))).await?;
        assert_eq!(status, StatusCode::CONFLICT);

        // An authority change before publication refuses the proposal.
        let (status, _) = send(http.post(&requests).bearer_auth(&reader).json(&exact)).await?;
        assert_eq!(status, StatusCode::ACCEPTED);
        let (_, owned) = send(http.get(&requests).bearer_auth(&owner)).await?;
        let record = owned["records"][0]["record"].clone();
        let context: GrantContext = postcard::from_bytes(&bytes(&record))?;
        add_member(&seed, &owner, &group.group_id, Value::Null).await?;
        let route = format!("{requests}/{}/grant", context.request.request_id);
        let (status, _) = send(http.post(&route).bearer_auth(&owner).json(&submission(&record, [1; 32], &[1; 16]))).await?;
        assert_eq!(status, StatusCode::CONFLICT);

        // The listing removes the stale request; the member's repeat opens a new one and notifies.
        // The holder's member addition above notified for the new member's request too.
        notified(4).await?;
        let (_, owned) = send(http.get(&requests).bearer_auth(&owner)).await?;
        assert!(owned["records"].as_array().unwrap().is_empty(), "{owned}");
        let (status, pending) = send(http.post(&requests).bearer_auth(&reader).json(&exact)).await?;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_ne!(pending["fields"]["request_id"], json!(context.request.request_id.to_string()));
        notified(5).await?;

        // A group CEL policy that can apply to reads refuses continuing keys.
        let policies = format!("{base}/api/v1/access/policies/group/{}", group.group_id);
        let policy = json!({"policies":[{"name":"no-tmp","kind":"deny","expression":"path.startsWith('/tmp')","enabled":true}]});
        let (status, body) = send(http.put(&policies).bearer_auth(&owner).json(&policy)).await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, body) = send(http.post(&requests).bearer_auth(&reader).json(&subtree)).await?;
        assert_eq!((status, body["code"].as_str()), (StatusCode::UNPROCESSABLE_ENTITY, Some("scope_unsupported")));
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    seed.shutdown().await;
    result
}

#[tokio::test]
async fn abe_limits() -> TestResult<()> {
    let seed = spawn_complete_seed().await?;
    let result = async {
        let http = reqwest::Client::new();
        let base = seed.base_url.clone();
        let owner = create_bearer_token(
            seed.context.as_ref(),
            seed.user_id,
            seed.realm_id,
            seed.capabilities.clone(),
        )
        .await?;
        let group = create_group_http(&base, &owner, "ABE limits").await?;
        let reader = add_member(&seed, &owner, &group.group_id, Value::Null).await?;
        add_key(
            &base,
            &reader,
            "reader-1",
            public_key_of(&SecretBytes::new(vec![11; 32])).unwrap(),
        )
        .await?;
        add_key(
            &base,
            &owner,
            "owner-1",
            public_key_of(&SecretBytes::new(vec![7; 32])).unwrap(),
        )
        .await?;
        let credentials = create_s3_credentials(&base, &owner, &group.group_id).await?;
        s3_client(seed.s3.as_ref().unwrap(), &credentials)
            .create_bucket()
            .bucket(BUCKET)
            .send()
            .await?;
        let encryption = format!("{base}/api/v1/data/buckets/{BUCKET}/storage/encryption");
        let (status, settings) = send(
            http.put(&encryption)
                .bearer_auth(&owner)
                .json(&json!({"mode":"vault_locked","expected_generation":0})),
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "{settings}");
        let requests = format!("{base}/api/v1/data/buckets/{BUCKET}/abe/requests");
        let scope = |key: &str| json!({"scope":{"kind":"exact","value":key}});
        let request = |key: String| http.post(&requests).bearer_auth(&reader).json(&scope(&key));

        // The node issues grants up to the cap; a new scope is refused and a held one replays.
        let (status, first) = send(request("foo/0".into())).await?;
        assert_eq!(status, StatusCode::OK, "{first}");
        for i in 1..MAX_REQUESTS {
            let (status, body) = send(request(format!("foo/{i}"))).await?;
            assert_eq!(status, StatusCode::OK, "{body}");
        }
        let (status, _) = send(request(format!("foo/{MAX_REQUESTS}"))).await?;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        let (status, repeated) = send(request("foo/0".into())).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(repeated["record"], first["record"]);

        // A request queued while locked is not published past the cap.
        let response = http
            .post(format!("{encryption}/lock"))
            .bearer_auth(&owner)
            .send()
            .await?;
        assert!(response.status().is_success());
        let (status, _) = send(request("bar/0".into())).await?;
        assert_eq!(status, StatusCode::ACCEPTED);
        let (_, owned) = send(http.get(&requests).bearer_auth(&owner)).await?;
        let record = owned["records"][0]["record"].clone();
        let context: GrantContext = postcard::from_bytes(&bytes(&record))?;
        let route = format!("{requests}/{}/grant", context.request.request_id);
        let (status, _) = send(
            http.post(&route)
                .bearer_auth(&owner)
                .json(&submission(&record, [1; 32], &[1; 16])),
        )
        .await?;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);

        // With a full open-request queue, a held scope still returns its grant.
        for i in 1..MAX_REQUESTS {
            let (status, body) = send(request(format!("bar/{i}"))).await?;
            assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        }
        let (status, repeated) = send(request("foo/0".into())).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(repeated["record"], first["record"]);
        let (status, _) = send(request(format!("bar/{MAX_REQUESTS}"))).await?;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    seed.shutdown().await;
    result
}

#[tokio::test]
async fn abe_queued_grants() -> TestResult<()> {
    let seed = spawn_complete_seed().await?;
    let result = async {
        let http = reqwest::Client::new();
        let base = seed.base_url.clone();
        let owner = create_bearer_token(
            seed.context.as_ref(),
            seed.user_id,
            seed.realm_id,
            seed.capabilities.clone(),
        )
        .await?;
        let group = create_group_http(&base, &owner, "ABE queued").await?;
        let user = UserId::local(Ulid::generate(), seed.realm_id);
        shared::add_user(seed.context.as_ref(), user).await?;
        let (status, _) = send(
            http.post(format!(
                "{base}/api/v1/access/groups/{}/members",
                group.group_id
            ))
            .bearer_auth(&owner)
            .json(&json!({"user_id":user.to_string(),"role_ids":null})),
        )
        .await?;
        assert_eq!(status, StatusCode::CREATED);
        let reader = sign_token(&seed, user, None, 600)?;
        let root =
            group_permission_path(seed.realm_id, group.group_id.parse()?, seed.net.node_id());
        let restricted = sign_scoped_token(
            &seed,
            user,
            vec![PathRestriction {
                pattern: format!("{root}/{BUCKET}/bar/**"),
                permission: Permission::READ,
            }],
        )?;
        let reader_public = public_key_of(&SecretBytes::new(vec![11; 32])).unwrap();
        add_key(&base, &reader, "reader-1", reader_public).await?;
        let owner_public = public_key_of(&SecretBytes::new(vec![7; 32])).unwrap();
        add_key(&base, &owner, "owner-1", owner_public).await?;
        let credentials = create_s3_credentials(&base, &owner, &group.group_id).await?;
        s3_client(seed.s3.as_ref().unwrap(), &credentials)
            .create_bucket()
            .bucket(BUCKET)
            .send()
            .await?;
        let encryption = format!("{base}/api/v1/data/buckets/{BUCKET}/storage/encryption");
        let (status, settings) = send(
            http.put(&encryption)
                .bearer_auth(&owner)
                .json(&json!({"mode":"vault_locked","expected_generation":0})),
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "{settings}");
        let response = http
            .post(format!("{encryption}/lock"))
            .bearer_auth(&owner)
            .send()
            .await?;
        assert!(response.status().is_success());

        // The restricted request is queued first, then an unrestricted one.
        let requests = format!("{base}/api/v1/data/buckets/{BUCKET}/abe/requests");
        let subtree = |value: &str| json!({"scope":{"kind":"subtree","value":value}});
        let (status, body) = send(
            http.post(&requests)
                .bearer_auth(&restricted)
                .json(&subtree("bar/")),
        )
        .await?;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        let (status, body) = send(
            http.post(&requests)
                .bearer_auth(&reader)
                .json(&subtree("foo/")),
        )
        .await?;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");

        // Publishing `bar/` after `foo/` keeps the `foo/` grant.
        let (_, owned) = send(http.get(&requests).bearer_auth(&owner)).await?;
        for prefix in ["foo/", "bar/"] {
            let record = owned["records"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| r["record"].clone())
                .find(|r| {
                    let context: GrantContext = postcard::from_bytes(&bytes(r)).unwrap();
                    context.request.scope == KeyScope::Subtree(prefix.into())
                })
                .unwrap();
            let context: GrantContext = postcard::from_bytes(&bytes(&record))?;
            let route = format!("{requests}/{}/grant", context.request.request_id);
            let (status, body) = send(
                http.post(&route)
                    .bearer_auth(&owner)
                    .json(&submission(&record, [1; 32], &[1; 16])),
            )
            .await?;
            assert_eq!(status, StatusCode::OK, "{body}");
        }
        let grants = format!("{base}/api/v1/data/buckets/{BUCKET}/abe/grants");
        let (_, own) = send(http.get(&grants).bearer_auth(&reader)).await?;
        assert_eq!(own["records"].as_array().unwrap().len(), 2, "{own}");
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    seed.shutdown().await;
    result
}

#[tokio::test]
async fn abe_restricted_keeps() -> TestResult<()> {
    let seed = spawn_complete_seed().await?;
    let result = async {
        let http = reqwest::Client::new();
        let base = seed.base_url.clone();
        let owner = create_bearer_token(
            seed.context.as_ref(),
            seed.user_id,
            seed.realm_id,
            seed.capabilities.clone(),
        )
        .await?;
        let group = create_group_http(&base, &owner, "ABE restricted").await?;
        let user = UserId::local(Ulid::generate(), seed.realm_id);
        shared::add_user(seed.context.as_ref(), user).await?;
        grant_roles(&base, &owner, &group.group_id, user, Value::Null).await?;
        let reader = sign_token(&seed, user, None, 600)?;
        let root =
            group_permission_path(seed.realm_id, group.group_id.parse()?, seed.net.node_id());
        let restricted = sign_scoped_token(
            &seed,
            user,
            vec![PathRestriction {
                pattern: format!("{root}/{BUCKET}/foo/**"),
                permission: Permission::READ,
            }],
        )?;
        let reader_public = public_key_of(&SecretBytes::new(vec![11; 32])).unwrap();
        add_key(&base, &reader, "reader-1", reader_public).await?;
        let credentials = create_s3_credentials(&base, &owner, &group.group_id).await?;
        s3_client(seed.s3.as_ref().unwrap(), &credentials)
            .create_bucket()
            .bucket(BUCKET)
            .send()
            .await?;
        let encryption = format!("{base}/api/v1/data/buckets/{BUCKET}/storage/encryption");
        let (status, settings) = send(
            http.put(&encryption)
                .bearer_auth(&owner)
                .json(&json!({"mode":"node_managed","expected_generation":0})),
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "{settings}");

        // The restricted `foo/` issuance scans the unrestricted `bar/` grant and keeps it.
        let requests = format!("{base}/api/v1/data/buckets/{BUCKET}/abe/requests");
        let subtree = |value: &str| json!({"scope":{"kind":"subtree","value":value}});
        for (token, prefix) in [(&reader, "bar/"), (&restricted, "foo/")] {
            let request = http.post(&requests).bearer_auth(token);
            let (status, body) = send(request.json(&subtree(prefix))).await?;
            assert_eq!(status, StatusCode::OK, "{body}");
        }
        let grants = format!("{base}/api/v1/data/buckets/{BUCKET}/abe/grants");
        let (_, own) = send(http.get(&grants).bearer_auth(&reader)).await?;
        assert_eq!(own["records"].as_array().unwrap().len(), 2, "{own}");
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    seed.shutdown().await;
    result
}

#[tokio::test]
async fn abe_issuance_reissues() -> TestResult<()> {
    let seed = spawn_complete_seed().await?;
    let result = async {
        let http = reqwest::Client::new();
        let base = seed.base_url.clone();
        let owner = create_bearer_token(
            seed.context.as_ref(),
            seed.user_id,
            seed.realm_id,
            seed.capabilities.clone(),
        )
        .await?;
        let group = create_group_http(&base, &owner, "ABE issuance").await?;
        let mut readers = Vec::new();
        for seed_byte in [11, 12] {
            let user = UserId::local(Ulid::generate(), seed.realm_id);
            shared::add_user(seed.context.as_ref(), user).await?;
            grant_roles(&base, &owner, &group.group_id, user, Value::Null).await?;
            let token = sign_token(&seed, user, None, 600)?;
            let public = public_key_of(&SecretBytes::new(vec![seed_byte; 32])).unwrap();
            add_key(&base, &token, "reader-1", public).await?;
            readers.push(token);
        }
        let credentials = create_s3_credentials(&base, &owner, &group.group_id).await?;
        let s3 = s3_client(seed.s3.as_ref().unwrap(), &credentials);
        s3.create_bucket().bucket(BUCKET).send().await?;
        let encryption = format!("{base}/api/v1/data/buckets/{BUCKET}/storage/encryption");
        let (status, settings) = send(
            http.put(&encryption)
                .bearer_auth(&owner)
                .json(&json!({"mode":"node_managed","expected_generation":0})),
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "{settings}");
        let bucket_id = Ulid::from_string(settings["bucket_id"].as_str().unwrap())?;
        let requests = format!("{base}/api/v1/data/buckets/{BUCKET}/abe/requests");
        let subtree = json!({"scope":{"kind":"subtree","value":"foo/"}});
        let (status, body) =
            send(http.post(&requests).bearer_auth(&readers[0]).json(&subtree)).await?;
        assert_eq!(status, StatusCode::OK, "{body}");

        // A synced removal left the bucket due; the next node issuance raises and reissues.
        let id = bucket_id.to_bytes().to_vec();
        let due = || StorageEffect::Write {
            key_space: aruna_core::keyspaces::ABE_DUE_KEYSPACE.to_string(),
            key: id.clone().into(),
            value: vec![1].into(),
            txn_id: None,
        };
        seed.context.storage_handle.send_storage_effect(due()).await;
        let (status, body) =
            send(http.post(&requests).bearer_auth(&readers[1]).json(&subtree)).await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["fields"]["request"]["epochs"], json!([1, 2]));
        // One page reissued the grants; the request rows stay saved for the next issuance.
        let progress = aruna_core::keyspaces::ABE_REISSUE_KEYSPACE;
        assert!(stored(&seed, progress, id.clone()).await.is_some());
        let (status, body) =
            send(http.post(&requests).bearer_auth(&readers[0]).json(&subtree)).await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(stored(&seed, progress, id.clone()).await.is_none());
        let grants = format!("{base}/api/v1/data/buckets/{BUCKET}/abe/grants");
        let (_, own) = send(http.get(&grants).bearer_auth(&readers[0])).await?;
        let epochs: Vec<u64> = own["records"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|r| {
                KeyGrant::from_bytes(&bytes(&r["record"]))
                    .unwrap()
                    .context
                    .request
                    .epochs
            })
            .collect();
        assert!(epochs.contains(&2), "{own}");

        // A due raise by an enumerated request also pages the reissue for continuing readers.
        let body = b"x".to_vec().into();
        s3.put_object()
            .bucket(BUCKET)
            .key("foo/x")
            .body(body)
            .send()
            .await?;
        seed.context.storage_handle.send_storage_effect(due()).await;
        let writes = json!({"scope":{"kind":"writes","value":"foo/"}});
        for _ in 0..2 {
            let (status, body) =
                send(http.post(&requests).bearer_auth(&readers[1]).json(&writes)).await?;
            assert_eq!(status, StatusCode::OK, "{body}");
        }
        assert!(stored(&seed, progress, id).await.is_none());
        let (_, own) = send(http.get(&grants).bearer_auth(&readers[0])).await?;
        let continuing = own["records"].as_array().unwrap().iter().any(|r| {
            let grant = KeyGrant::from_bytes(&bytes(&r["record"])).unwrap();
            grant.context.request.epochs.contains(&3)
        });
        assert!(continuing, "{own}");
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    seed.shutdown().await;
    result
}

async fn grant_roles(
    base: &str,
    actor: &str,
    group: &str,
    user: UserId,
    roles: Value,
) -> TestResult<Value> {
    let route = format!("{base}/api/v1/access/groups/{group}/members");
    let request = reqwest::Client::new().post(route).bearer_auth(actor);
    let (status, body) =
        send(request.json(&json!({"user_id":user.to_string(),"role_ids":roles}))).await?;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    Ok(body)
}

#[tokio::test]
async fn abe_members() -> TestResult<()> {
    let seed = spawn_complete_seed().await?;
    let result = async {
        let http = reqwest::Client::new();
        let base = seed.base_url.clone();
        let owner = create_bearer_token(
            seed.context.as_ref(),
            seed.user_id,
            seed.realm_id,
            seed.capabilities.clone(),
        )
        .await?;
        let group = create_group_http(&base, &owner, "ABE members").await?;
        add_key(
            &base,
            &owner,
            "owner-1",
            public_key_of(&SecretBytes::new(vec![7; 32])).unwrap(),
        )
        .await?;
        let credentials = create_s3_credentials(&base, &owner, &group.group_id).await?;
        let s3 = s3_client(seed.s3.as_ref().unwrap(), &credentials);
        let (vault, node) = ("abe-members-vault", "abe-members-node");
        for (bucket, mode) in [(vault, "vault_locked"), (node, "node_managed")] {
            s3.create_bucket().bucket(bucket).send().await?;
            let encryption = format!("{base}/api/v1/data/buckets/{bucket}/storage/encryption");
            let (status, body) = send(
                http.put(&encryption)
                    .bearer_auth(&owner)
                    .json(&json!({"mode":mode,"expected_generation":0})),
            )
            .await?;
            assert_eq!(status, StatusCode::OK, "{body}");
        }
        let lock = format!("{base}/api/v1/data/buckets/{vault}/storage/encryption/lock");
        assert!(
            http.post(lock)
                .bearer_auth(&owner)
                .send()
                .await?
                .status()
                .is_success()
        );
        let group_ulid = Ulid::from_string(&group.group_id)?;
        let data = group_permission_path(seed.realm_id, group_ulid, seed.net.node_id());
        let roles = format!("{base}/api/v1/access/groups/{}/roles", group.group_id);
        let mut role_ids = Vec::new();
        for (name, path, permission) in [
            (
                "member-manager",
                format!("/{}/g/{}/admin/users/**", seed.realm_id, group.group_id),
                "write",
            ),
            ("other-reader", format!("{data}/other/**"), "read"),
        ] {
            let permissions = std::collections::HashMap::from([(path, permission)]);
            let (status, role) = send(
                http.post(&roles)
                    .bearer_auth(&owner)
                    .json(&json!({"name":name,"permissions":permissions})),
            )
            .await?;
            assert_eq!(status, StatusCode::CREATED, "{role}");
            role_ids.push(role["role_id"].clone());
        }
        let seed_ref = &seed;
        let user = |n: u8| async move {
            let seed = seed_ref;
            let id = UserId::local(Ulid::generate(), seed.realm_id);
            let token = create_bearer_token(
                seed.context.as_ref(),
                id,
                seed.realm_id,
                seed.capabilities.clone(),
            )
            .await?;
            add_key(
                &seed.base_url,
                &token,
                "member-1",
                public_key_of(&SecretBytes::new(vec![n; 32])).unwrap(),
            )
            .await?;
            Ok::<_, Box<dyn std::error::Error>>((id, token))
        };
        let ((first, first_token), (manager, manager_token), (second, _)) =
            (user(21).await?, user(22).await?, user(23).await?);

        // A holder grant opens the locked bucket's request, while node_managed issues at once.
        let body = grant_roles(&base, &owner, &group.group_id, first, Value::Null).await?;
        let opened = body["key_requests"].as_array().unwrap().clone();
        assert_eq!(opened.len(), 1, "{body}");
        let (_, held) = send(
            http.get(format!("{base}/api/v1/data/buckets/{vault}/abe/requests"))
                .bearer_auth(&owner),
        )
        .await?;
        assert_eq!(held["records"][0]["fields"]["request_id"], opened[0]);
        let (_, grants) = send(
            http.get(format!("{base}/api/v1/data/buckets/{node}/abe/grants"))
                .bearer_auth(&first_token),
        )
        .await?;
        assert_eq!(grants["records"].as_array().unwrap().len(), 1);

        // Roles without a read scope in these buckets open nothing.
        let body = grant_roles(
            &base,
            &owner,
            &group.group_id,
            manager,
            json!([role_ids[0], role_ids[1]]),
        )
        .await?;
        assert_eq!(body["key_requests"], json!([]));

        // A repeated approval by a non-holder returns the same request and notifies once.
        let ((third, third_token), (fourth, _)) = (user(24).await?, user(25).await?);
        let joins = format!(
            "{base}/api/v1/access/groups/{}/join-requests",
            group.group_id
        );
        let (status, join) =
            send(http.post(&joins).bearer_auth(&third_token).json(&json!({}))).await?;
        assert_eq!(status, StatusCode::CREATED, "{join}");
        let decide = format!("{joins}/{}/decide", join["request_id"].as_str().unwrap());
        let mut decided = Vec::new();
        for _ in 0..2 {
            let approve = http.post(&decide).bearer_auth(&manager_token);
            let (status, body) = send(approve.json(&json!({"approve":true}))).await?;
            assert_eq!(status, StatusCode::OK, "{body}");
            decided.push(body["key_requests"].clone());
        }
        assert_eq!(decided[0].as_array().unwrap().len(), 1, "{decided:?}");
        assert_eq!(decided[0], decided[1]);

        // Every new request notifies each holder once per member and bucket, a granting one too.
        let body = grant_roles(&base, &manager_token, &group.group_id, second, Value::Null).await?;
        assert_eq!(body["key_requests"].as_array().unwrap().len(), 1, "{body}");
        let inbox = format!("{base}/api/v1/system/notifications");
        let pending = || async {
            let (_, list) = send(http.get(&inbox).bearer_auth(&owner))
                .await
                .unwrap_or((StatusCode::OK, Value::Null));
            list["notifications"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|n| n["kind"] == "bucket_key_pending")
                .cloned()
                .collect::<Vec<_>>()
        };
        shared::wait_until(
            "key pending notification",
            std::time::Duration::from_secs(120),
            std::time::Duration::from_millis(50),
            || async {
                pending()
                    .await
                    .iter()
                    .any(|n| n["member_user_id"] == second.to_string())
            },
        )
        .await?;
        // The outbox drains in order, so a notice from the repeated approval would be here.
        let notices = pending().await;
        assert_eq!(notices.len(), 3, "{notices:?}");
        assert!(notices.iter().all(|n| n["bucket"] == vault));
        for member in [first, third, second] {
            assert!(
                notices
                    .iter()
                    .any(|n| n["member_user_id"] == member.to_string())
            );
        }

        // A created role with assigned users opens their requests too.
        let path = bucket_permission_path(seed.realm_id, group_ulid, seed.net.node_id(), vault);
        let permissions = json!({format!("{path}/**"): "read"});
        let role = json!({"name":"vault-reader","permissions":permissions,
            "assigned_users":[fourth.to_string()]});
        let (status, body) = send(http.post(&roles).bearer_auth(&owner).json(&role)).await?;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        assert_eq!(body["key_requests"].as_array().unwrap().len(), 1, "{body}");

        // A group whose last encrypted bucket was turned off pays one index scan and nothing else.
        let plain = create_group_http(&base, &owner, "ABE plain").await?;
        let credentials = create_s3_credentials(&base, &owner, &plain.group_id).await?;
        let plain_s3 = s3_client(seed.s3.as_ref().unwrap(), &credentials);
        for bucket in ["abe-plain-a", "abe-plain-b", "abe-plain-c"] {
            plain_s3.create_bucket().bucket(bucket).send().await?;
        }
        let encryption =
            |bucket: &str| format!("{base}/api/v1/data/buckets/{bucket}/storage/encryption");
        for (mode, generation) in [("node_managed", 0), ("off", 1)] {
            let body = json!({"mode":mode,"expected_generation":generation});
            let request = http.put(encryption("abe-plain-a")).bearer_auth(&owner);
            let response = request.json(&body).send().await?;
            assert!(response.status().is_success(), "{mode}");
        }
        let auth = AuthContext {
            user_id: seed.user_id,
            realm_id: seed.realm_id,
            path_restrictions: None,
            session: None,
        };
        let (node_id, plain_id) = (seed.net.node_id(), Ulid::from_string(&plain.group_id)?);
        let now = aruna_core::time::unix_timestamp_millis();
        let mut operation = MemberKeysOperation::new(auth, node_id, plain_id, vec![first], now);
        let mut effects = operation.start();
        assert_eq!(effects.len(), 1, "{effects:?}");
        let Some(Effect::Storage(scan)) = effects.pop() else {
            panic!("expected the index scan")
        };
        let event = seed.context.storage_handle.send_storage_effect(scan).await;
        assert!(operation.step(event).is_empty());
        assert_eq!(operation.finalize(), Ok(Vec::new()));

        // An unindexed bucket joins the index when a change admits a new generation.
        let bucket = "abe-plain-b";
        for (mode, generation) in [("node_managed", 0), ("vault_locked", 1)] {
            if mode == "vault_locked" {
                let key = [&plain_id.to_bytes()[..], bucket.as_bytes()].concat();
                let unindex = StorageEffect::Delete {
                    key_space: aruna_core::keyspaces::GROUP_ENCRYPTED_KEYSPACE.to_string(),
                    key: key.into(),
                    txn_id: None,
                };
                seed.context
                    .storage_handle
                    .send_storage_effect(unindex)
                    .await;
            }
            let body = json!({"mode":mode,"expected_generation":generation});
            let request = http.put(encryption(bucket)).bearer_auth(&owner);
            let response = request.json(&body).send().await?;
            assert!(response.status().is_success(), "{mode}");
        }
        let lock = http.post(format!("{}/lock", encryption(bucket)));
        assert!(lock.bearer_auth(&owner).send().await?.status().is_success());
        let (member, _) = user(26).await?;
        let body = grant_roles(&base, &owner, &plain.group_id, member, Value::Null).await?;
        assert_eq!(body["key_requests"].as_array().unwrap().len(), 1, "{body}");
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    seed.shutdown().await;
    result
}

#[tokio::test]
async fn abe_rotation() -> TestResult<()> {
    let seed = spawn_complete_seed().await?;
    let result = async {
        let http = &reqwest::Client::new();
        let base = seed.base_url.clone();
        let token = &create_bearer_token(
            seed.context.as_ref(),
            seed.user_id,
            seed.realm_id,
            seed.capabilities.clone(),
        )
        .await?;
        let group = create_group_http(&base, token, "ABE rotation").await?;
        let user_private = SecretBytes::new(vec![7; 32]);
        add_key(
            &base,
            token,
            "rotation-1",
            public_key_of(&user_private).unwrap(),
        )
        .await?;
        let credentials = create_s3_credentials(&base, token, &group.group_id).await?;
        let s3 = s3_client(seed.s3.as_ref().unwrap(), &credentials);
        let bucket = "abe-rotation";
        s3.create_bucket().bucket(bucket).send().await?;
        let encryption = format!("{base}/api/v1/data/buckets/{bucket}/storage/encryption");
        let body = json!({"mode":"node_managed","expected_generation":0});
        let (status, settings) = send(http.put(&encryption).bearer_auth(token).json(&body)).await?;
        assert_eq!(status, StatusCode::OK, "{settings}");
        let upload = s3
            .put_object()
            .bucket(bucket)
            .key("foo/data")
            .body(b"rotated bytes".to_vec().into())
            .send()
            .await?;
        let first = upload.version_id().unwrap().to_string();
        let source = format!("{bucket}/foo/data?versionId={first}");
        // Every copy shares the archive and object key; each version moves in its own unit.
        let mut versions = vec![("foo/data".to_string(), first)];
        for n in 0..12 {
            let key = format!("foo/copy-{n}");
            let copied = s3
                .copy_object()
                .bucket(bucket)
                .key(&key)
                .copy_source(&source)
                .send()
                .await?;
            versions.push((key, copied.version_id().unwrap().to_string()));
        }
        let versions = &versions;
        let private: &[u8; 32] = user_private.expose().try_into()?;
        let request = &format!("{base}/api/v1/data/buckets/{bucket}/abe/requests");
        let envelope_route = &format!("{base}/api/v1/data/blobs/envelope");
        let content_route = &format!("{base}/api/v1/data/blobs/content");
        // Issues a foo/ key and opens the object key of every version with it.
        let open_all = || async move {
            let scope = json!({"scope":{"kind":"subtree","value":"foo/"}});
            let (status, issued) = send(http.post(request).bearer_auth(token).json(&scope)).await?;
            assert_eq!(status, StatusCode::OK, "{issued}");
            let grant = KeyGrant::from_bytes(&bytes(&issued["record"]))?;
            let parameters = grant.context.request.parameters.public()?;
            let sealed = SealedSecret {
                enc: grant.enc,
                ciphertext: grant.ciphertext.clone(),
            };
            let (aad, transport) = (
                grant.context.bytes()?,
                [&grant.enc[..], &grant.ciphertext].concat(),
            );
            let key = UserKey::open(&parameters, &transport, |_| {
                open_sealed(private, &sealed, GRANT_PURPOSE, &aad).map_err(|_| aruna_kpabe::Error)
            })?;
            let mut objects = Vec::new();
            for (name, version) in versions {
                let query = [
                    ("bucket", bucket),
                    ("key", name.as_str()),
                    ("version_id", version.as_str()),
                ];
                let request = http
                    .get(query_url(envelope_route, &query)?)
                    .bearer_auth(token);
                let (status, env) = send(request).await?;
                assert_eq!(status, StatusCode::OK, "{env}");
                let cipher = Envelope::from_bytes(&parameters, &bytes(&env["envelope"]["abe"]))?;
                let context = bytes(&env["context"]["bytes"]);
                let object = aruna_kpabe::open(&parameters, &key, &cipher, &context)?;
                objects.push(STANDARD.encode(object.as_bytes()));
            }
            Ok::<_, Box<dyn std::error::Error>>(objects)
        };
        let old = open_all().await?;
        let rotate = json!({"expected_generation":settings["storage_generation"]});
        let (status, rotated) = send(
            http.post(format!("{encryption}/rotate"))
                .bearer_auth(token)
                .json(&rotate),
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "{rotated}");
        let generation = rotated["key_generation"].as_u64();
        shared::wait_until(
            "rotated envelopes",
            std::time::Duration::from_secs(120),
            std::time::Duration::from_millis(100),
            || async move {
                for (name, version) in versions {
                    let query = [
                        ("bucket", bucket),
                        ("key", name.as_str()),
                        ("version_id", version.as_str()),
                    ];
                    let Ok(url) = query_url(envelope_route, &query) else {
                        return false;
                    };
                    match send(http.get(url).bearer_auth(token)).await {
                        Ok((_, env)) if env["context"]["generation"].as_u64() == generation => {}
                        _ => return false,
                    }
                }
                true
            },
        )
        .await?;
        let new = open_all().await?;
        let distinct: std::collections::HashSet<_> = new.iter().collect();
        assert_eq!(
            distinct.len(),
            versions.len(),
            "every version gets its own object key"
        );
        for (((name, version), old), new) in versions.iter().zip(&old).zip(&new) {
            let query = [
                ("bucket", bucket),
                ("key", name.as_str()),
                ("version_id", version.as_str()),
            ];
            let read = |object: &str| {
                let url = query_url(content_route, &query).unwrap();
                http.get(url)
                    .bearer_auth(token)
                    .header("x-aruna-object-key", object)
                    .send()
            };
            let response = read(new).await?;
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.bytes().await?.as_ref(), b"rotated bytes");
            // The old object key opens nothing current.
            let status = read(old).await?.status();
            assert!(!status.is_success(), "{name}: {status}");
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    seed.shutdown().await;
    result
}

#[tokio::test]
async fn abe_conversion() -> TestResult<()> {
    let seed = spawn_complete_seed().await?;
    let result = async {
        let http = &reqwest::Client::new();
        let base = seed.base_url.clone();
        let token = &create_bearer_token(
            seed.context.as_ref(),
            seed.user_id,
            seed.realm_id,
            seed.capabilities.clone(),
        )
        .await?;
        let group = create_group_http(&base, token, "ABE conversion").await?;
        let user_private = SecretBytes::new(vec![7; 32]);
        let public = public_key_of(&user_private).unwrap();
        add_key(&base, token, "conversion-1", public).await?;
        let credentials = create_s3_credentials(&base, token, &group.group_id).await?;
        let s3 = s3_client(seed.s3.as_ref().unwrap(), &credentials);
        let bucket = "abe-conversion";
        s3.create_bucket().bucket(bucket).send().await?;
        let upload = s3
            .put_object()
            .bucket(bucket)
            .key("foo/data")
            .body(b"plain bytes".to_vec().into())
            .send()
            .await?;
        let version = upload.version_id().unwrap().to_string();
        let encryption = &format!("{base}/api/v1/data/buckets/{bucket}/storage/encryption");
        let body = json!({"mode":"vault_locked","expected_generation":0});
        let (status, settings) = send(http.put(encryption).bearer_auth(token).json(&body)).await?;
        assert_eq!(status, StatusCode::OK, "{settings}");
        let query = [
            ("bucket", bucket),
            ("key", "foo/data"),
            ("version_id", version.as_str()),
        ];
        let envelope_url = &query_url(&format!("{base}/api/v1/data/blobs/envelope"), &query)?;
        shared::wait_until(
            "converted version",
            std::time::Duration::from_secs(120),
            std::time::Duration::from_millis(100),
            || async move {
                let Ok((_, status)) = send(http.get(encryption).bearer_auth(token)).await else {
                    return false;
                };
                let request = http.get(envelope_url.clone()).bearer_auth(token);
                // The old plaintext waits out its reclaim grace, so the transition stays in cleanup.
                status["transition"]["done"] == 1
                    && send(request)
                        .await
                        .is_ok_and(|(code, _)| code == StatusCode::OK)
            },
        )
        .await?;
        let request = format!("{base}/api/v1/data/buckets/{bucket}/abe/requests");
        let scope = json!({"scope":{"kind":"subtree","value":"foo/"}});
        let (status, issued) = send(http.post(&request).bearer_auth(token).json(&scope)).await?;
        assert_eq!(status, StatusCode::OK, "{issued}");
        let grant = KeyGrant::from_bytes(&bytes(&issued["record"]))?;
        let parameters = grant.context.request.parameters.public()?;
        let private: &[u8; 32] = user_private.expose().try_into()?;
        let sealed = SealedSecret {
            enc: grant.enc,
            ciphertext: grant.ciphertext.clone(),
        };
        let (aad, transport) = (
            grant.context.bytes()?,
            [&grant.enc[..], &grant.ciphertext].concat(),
        );
        let key = UserKey::open(&parameters, &transport, |_| {
            open_sealed(private, &sealed, GRANT_PURPOSE, &aad).map_err(|_| aruna_kpabe::Error)
        })?;
        let (status, env) = send(http.get(envelope_url.clone()).bearer_auth(token)).await?;
        assert_eq!(status, StatusCode::OK, "{env}");
        let cipher = Envelope::from_bytes(&parameters, &bytes(&env["envelope"]["abe"]))?;
        let context = bytes(&env["context"]["bytes"]);
        let object = aruna_kpabe::open(&parameters, &key, &cipher, &context)?;
        let response = http
            .post(format!("{encryption}/lock"))
            .bearer_auth(token)
            .send()
            .await?;
        assert!(response.status().is_success());
        let content_url = query_url(&format!("{base}/api/v1/data/blobs/content"), &query)?;
        let response = http
            .get(content_url.clone())
            .bearer_auth(token)
            .send()
            .await?;
        assert_eq!(response.status(), StatusCode::LOCKED);
        let response = http
            .get(content_url)
            .bearer_auth(token)
            .header("x-aruna-object-key", STANDARD.encode(object.as_bytes()))
            .send()
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.bytes().await?.as_ref(), b"plain bytes");
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    seed.shutdown().await;
    result
}

async fn stored(seed: &SeedNode, space: &str, key: Vec<u8>) -> Option<Vec<u8>> {
    let read = StorageEffect::Read {
        key_space: space.to_string(),
        key: key.into(),
        txn_id: None,
    };
    match seed.context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value.map(|v| v.to_vec()),
        _ => None,
    }
}

/// Issues every open request listed to `token` with the bucket key; returns their contexts.
async fn issue_open(
    base: &str,
    token: &str,
    bucket_private: &SecretBytes,
) -> TestResult<Vec<GrantContext>> {
    let http = reqwest::Client::new();
    let requests = format!("{base}/api/v1/data/buckets/{BUCKET}/abe/requests");
    let (status, list) = send(http.get(&requests).bearer_auth(token)).await?;
    assert_eq!(status, StatusCode::OK, "{list}");
    let mut issued = Vec::new();
    for record in list["records"].as_array().unwrap() {
        let grant = sealed_grant(&record["record"], bucket_private)?;
        let route = format!("{requests}/{}/grant", grant.context.request.request_id);
        let (status, body) = send(http.post(&route).bearer_auth(token).json(&submission(
            &record["record"],
            grant.enc,
            &grant.ciphertext,
        )))
        .await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        issued.push(grant.context);
    }
    Ok(issued)
}

/// The grant a holder issues for a listed request record with the bucket key.
fn sealed_grant(record: &Value, bucket_private: &SecretBytes) -> TestResult<KeyGrant> {
    let context: GrantContext = postcard::from_bytes(&bytes(record))?;
    let r = &context.request;
    let parameters = r.parameters.public()?;
    let master = r.parameters.recompute(bucket_private)?;
    let policy = r.scope.policy(&r.parameters, &r.epochs)?;
    let key = aruna_kpabe::issue(&parameters, &master, &policy, &mut SysRng)?;
    let recipient = r.recipient_public.unwrap();
    let sealed = key.seal(|plain| {
        seal_to(&recipient, GRANT_PURPOSE, &context.bytes().unwrap(), plain)
            .map_err(|_| aruna_kpabe::Error)
    })?;
    Ok(KeyGrant {
        context,
        enc: sealed.enc,
        ciphertext: sealed.ciphertext,
    })
}

type HeldKey = (Vec<u64>, aruna_kpabe::PublicParameters, UserKey);

/// The epochs, parameters and user keys of the caller's grants, opened with `private`.
async fn user_keys(base: &str, token: &str, private: &SecretBytes) -> TestResult<Vec<HeldKey>> {
    let route = format!("{base}/api/v1/data/buckets/{BUCKET}/abe/grants");
    let (_, own) = send(reqwest::Client::new().get(route).bearer_auth(token)).await?;
    let secret: &[u8; 32] = private.expose().try_into()?;
    let mut keys = Vec::new();
    for record in own["records"].as_array().unwrap() {
        let grant = KeyGrant::from_bytes(&bytes(&record["record"]))?;
        let parameters = grant.context.request.parameters.public()?;
        let transport = [&grant.enc[..], &grant.ciphertext].concat();
        let sealed = SealedSecret {
            enc: grant.enc,
            ciphertext: grant.ciphertext.clone(),
        };
        let aad = grant.context.bytes()?;
        let key = UserKey::open(&parameters, &transport, |_| {
            open_sealed(secret, &sealed, GRANT_PURPOSE, &aad).map_err(|_| aruna_kpabe::Error)
        })?;
        keys.push((grant.context.request.epochs.clone(), parameters, key));
    }
    Ok(keys)
}

/// The envelope epoch of a version and whether any of `keys` opens it.
async fn opens(
    base: &str,
    token: &str,
    keys: &[HeldKey],
    key: &str,
    version: &str,
) -> TestResult<(u64, bool)> {
    let route = format!("{base}/api/v1/data/blobs/envelope");
    let query = [("bucket", BUCKET), ("key", key), ("version_id", version)];
    let request = reqwest::Client::new().get(query_url(&route, &query)?);
    let (status, env) = send(request.bearer_auth(token)).await?;
    assert_eq!(status, StatusCode::OK, "{env}");
    let (context, abe) = (
        bytes(&env["context"]["bytes"]),
        bytes(&env["envelope"]["abe"]),
    );
    let epoch = env["context"]["epoch"].as_u64().unwrap();
    let opened = keys.iter().any(|(_, p, user)| {
        Envelope::from_bytes(p, &abe)
            .is_ok_and(|c| aruna_kpabe::open(p, user, &c, &context).is_ok())
    });
    Ok((epoch, opened))
}

/// Turns on `vault_locked` for `BUCKET`; returns its bucket id, generation and opened key.
async fn locked_key(
    seed: &SeedNode,
    owner: &str,
    owner_key: &Value,
    owner_private: &SecretBytes,
) -> TestResult<(Ulid, u64, SecretBytes)> {
    let http = reqwest::Client::new();
    let base = &seed.base_url;
    let encryption = format!("{base}/api/v1/data/buckets/{BUCKET}/storage/encryption");
    let (status, settings) = send(
        http.put(&encryption)
            .bearer_auth(owner)
            .json(&json!({"mode":"vault_locked","expected_generation":0})),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{settings}");
    let generation = settings["key_generation"].as_u64().unwrap();
    let bucket_id = Ulid::from_string(settings["bucket_id"].as_str().unwrap())?;
    let (_, copies) = send(
        http.get(format!("{encryption}/copies/me?generation={generation}"))
            .bearer_auth(owner),
    )
    .await?;
    let copy = &copies["copies"][0];
    let record_id = Ulid::from_string(owner_key["record_id"].as_str().unwrap())?;
    let info = copy_info(
        seed.realm_id,
        seed.net.node_id(),
        BucketKeyRef::new(bucket_id, generation),
        seed.user_id,
        record_id,
    );
    let owner_secret: &[u8; 32] = owner_private.expose().try_into()?;
    let opened = open_sealed(
        owner_secret,
        &SealedSecret {
            enc: bytes(&copy["enc"]).try_into().unwrap(),
            ciphertext: bytes(&copy["ciphertext"]),
        },
        &info,
        &[],
    )?;
    let bucket_private = SecretBytes::new(opened.to_vec());
    Ok((bucket_id, generation, bucket_private))
}

#[tokio::test]
async fn abe_epochs() -> TestResult<()> {
    let seed = spawn_complete_seed().await?;
    let result = async {
        let http = reqwest::Client::new();
        let base = seed.base_url.clone();
        let owner = create_bearer_token(
            seed.context.as_ref(),
            seed.user_id,
            seed.realm_id,
            seed.capabilities.clone(),
        )
        .await?;
        let group = create_group_http(&base, &owner, "ABE epochs").await?;
        let owner_private = SecretBytes::new(vec![7; 32]);
        let owner_key = add_key(
            &base,
            &owner,
            "owner-1",
            public_key_of(&owner_private).unwrap(),
        )
        .await?;
        let reader_id = UserId::local(Ulid::generate(), seed.realm_id);
        grant_roles(&base, &owner, &group.group_id, reader_id, Value::Null).await?;
        let reader = create_bearer_token(
            seed.context.as_ref(),
            reader_id,
            seed.realm_id,
            seed.capabilities.clone(),
        )
        .await?;
        let reader_private = SecretBytes::new(vec![11; 32]);
        add_key(
            &base,
            &reader,
            "reader-1",
            public_key_of(&reader_private).unwrap(),
        )
        .await?;
        let leaver_id = UserId::local(Ulid::generate(), seed.realm_id);
        grant_roles(&base, &owner, &group.group_id, leaver_id, Value::Null).await?;
        let leaver = create_bearer_token(
            seed.context.as_ref(),
            leaver_id,
            seed.realm_id,
            seed.capabilities.clone(),
        )
        .await?;
        add_key(
            &base,
            &leaver,
            "leaver-1",
            public_key_of(&SecretBytes::new(vec![12; 32])).unwrap(),
        )
        .await?;
        let credentials = create_s3_credentials(&base, &owner, &group.group_id).await?;
        let s3 = s3_client(seed.s3.as_ref().unwrap(), &credentials);
        s3.create_bucket().bucket(BUCKET).send().await?;
        let encryption = format!("{base}/api/v1/data/buckets/{BUCKET}/storage/encryption");
        let (bucket_id, generation, bucket_private) =
            locked_key(&seed, &owner, &owner_key, &owner_private).await?;
        let put = |key: &'static str| {
            let request = s3
                .put_object()
                .bucket(BUCKET)
                .key(key)
                .body(key.as_bytes().to_vec().into());
            async move {
                Ok::<_, Box<dyn std::error::Error>>(
                    request.send().await?.version_id().unwrap().to_string(),
                )
            }
        };
        let first = put("foo/a").await?;
        assert!(
            http.post(format!("{encryption}/lock"))
                .bearer_auth(&owner)
                .send()
                .await?
                .status()
                .is_success()
        );
        let requests = format!("{base}/api/v1/data/buckets/{BUCKET}/abe/requests");
        let subtree = json!({"scope":{"kind":"subtree","value":"foo/"}});
        for token in [&reader, &leaver] {
            let (status, body) =
                send(http.post(&requests).bearer_auth(token).json(&subtree)).await?;
            assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        }
        issue_open(&base, &owner, &bucket_private).await?;
        let old_keys = user_keys(&base, &reader, &reader_private).await?;
        assert!(old_keys.iter().all(|(epochs, _, _)| epochs == &[1]));

        // Only holders raise by hand; remaining readers get requests for the new epoch.
        let raise = format!("{base}/api/v1/data/buckets/{BUCKET}/abe/epoch");
        let (status, _) = send(http.post(&raise).bearer_auth(&reader)).await?;
        assert_eq!(status, StatusCode::FORBIDDEN);
        let (status, body) = send(http.post(&raise).bearer_auth(&owner)).await?;
        assert_eq!(
            (status, body["epoch"].as_u64()),
            (StatusCode::OK, Some(2)),
            "{body}"
        );
        let reopened = issue_open(&base, &owner, &bucket_private).await?;
        let recipients: std::collections::BTreeSet<_> =
            reopened.iter().map(|c| c.request.recipient_user).collect();
        assert_eq!(recipients, [reader_id, leaver_id].into_iter().collect());
        assert!(reopened.iter().all(|c| c.request.epochs.contains(&2)));

        // A key from before the raise opens no later envelope but keeps old reads.
        let second = put("foo/b").await?;
        assert_eq!(
            opens(&base, &reader, &old_keys, "foo/b", &second).await?,
            (2, false)
        );
        assert_eq!(
            opens(&base, &reader, &old_keys, "foo/a", &first).await?,
            (1, true)
        );
        let new_keys = user_keys(&base, &reader, &reader_private).await?;
        assert_eq!(
            opens(&base, &reader, &new_keys, "foo/b", &second).await?,
            (2, true)
        );
        // The old grant merged into the new one: one key opens what both opened, no more.
        assert_eq!(new_keys.len(), 1);
        assert_eq!(new_keys[0].0, vec![1, 2]);
        assert_eq!(
            opens(&base, &reader, &new_keys, "foo/a", &first).await?,
            (1, true)
        );
        let outside = put("bar/x").await?;
        assert_eq!(
            opens(&base, &owner, &new_keys, "bar/x", &outside).await?,
            (2, false)
        );

        // A removal marks the locked bucket due without raising it.
        let leave = format!(
            "{base}/api/v1/access/groups/{}/members/{leaver_id}",
            group.group_id
        );
        assert_eq!(
            http.delete(&leave)
                .bearer_auth(&owner)
                .send()
                .await?
                .status(),
            StatusCode::NO_CONTENT
        );
        let id = bucket_id.to_bytes().to_vec();
        let epoch_row = || stored(&seed, aruna_core::keyspaces::ABE_EPOCH_KEYSPACE, id.clone());
        let due_row = || stored(&seed, aruna_core::keyspaces::ABE_DUE_KEYSPACE, id.clone());
        // A removal's marker is a blind write of the due row.
        let mark = || {
            let write = StorageEffect::Write {
                key_space: aruna_core::keyspaces::ABE_DUE_KEYSPACE.to_string(),
                key: id.clone().into(),
                value: vec![1].into(),
                txn_id: None,
            };
            seed.context.storage_handle.send_storage_effect(write)
        };
        let progress_row = || {
            stored(
                &seed,
                aruna_core::keyspaces::ABE_REISSUE_KEYSPACE,
                id.clone(),
            )
        };
        assert_eq!(epoch_row().await, Some(2u64.to_be_bytes().to_vec()));
        assert!(due_row().await.is_some());

        // A removal racing a raise aborts that raise and stays due.
        let auth = AuthContext {
            user_id: seed.user_id,
            realm_id: seed.realm_id,
            path_restrictions: None,
            session: None,
        };
        let node = seed.net.node_id();
        let now = aruna_core::time::unix_timestamp_millis();
        let action = KeyAction::Epoch { instant: false };
        let mut operation =
            KeyOperation::new(BUCKET.into(), auth.clone(), node, action.clone(), now);
        let mut effects: std::collections::VecDeque<Effect> =
            operation.start().into_iter().collect();
        while let Some(effect) = effects.pop_front() {
            let Effect::Storage(effect) = effect else {
                panic!("unexpected effect {effect:?}")
            };
            if matches!(effect, StorageEffect::CommitTransaction { .. }) {
                mark().await;
            }
            let event = seed
                .context
                .storage_handle
                .send_storage_effect(effect)
                .await;
            if !operation.is_complete() {
                effects.extend(operation.step(event));
            }
        }
        assert_eq!(operation.finalize(), Err(KeyError::Storage));
        assert_eq!(epoch_row().await, Some(2u64.to_be_bytes().to_vec()));
        assert!(due_row().await.is_some());

        // The next issuance run consumes the due state it observed.
        let raised = drive(
            KeyOperation::new(BUCKET.into(), auth.clone(), node, action, now),
            &seed.context,
        )
        .await;
        assert_eq!(raised, Ok(KeyResult::Epoch(3)));
        assert!(due_row().await.is_none());

        // Interrupted reissue resumes from its cursor at the next run.
        let page = drive(
            ReissueOperation::new(BUCKET.into(), auth.clone(), node, now, 1),
            &seed.context,
        )
        .await;
        assert_eq!(page, Ok(true));
        assert!(progress_row().await.is_some_and(|row| row.len() > 9));
        let reopened = issue_open(&base, &owner, &bucket_private).await?;
        assert!(progress_row().await.is_none());
        assert!(!reopened.is_empty());
        assert!(
            reopened
                .iter()
                .all(|c| c.request.recipient_user != leaver_id && c.request.epochs.contains(&3))
        );

        // A removal after a committed raise leaves a new due state for the next run.
        mark().await;
        assert!(due_row().await.is_some());
        issue_open(&base, &owner, &bucket_private).await?;
        assert_eq!(epoch_row().await, Some(4u64.to_be_bytes().to_vec()));
        assert!(due_row().await.is_none());

        // A member who joins after several raises reads old and new objects with one key.
        let third = put("foo/c").await?;
        let joiner = add_member(&seed, &owner, &group.group_id, Value::Null).await?;
        let joiner_private = SecretBytes::new(vec![13; 32]);
        add_key(
            &base,
            &joiner,
            "joiner-1",
            public_key_of(&joiner_private).unwrap(),
        )
        .await?;
        let (status, body) = send(http.post(&requests).bearer_auth(&joiner).json(&subtree)).await?;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        assert_eq!(body["fields"]["epochs"], json!([1, 2, 3, 4]));
        issue_open(&base, &owner, &bucket_private).await?;
        let keys = user_keys(&base, &joiner, &joiner_private).await?;
        assert_eq!(keys.len(), 1);
        for (key, version, epoch) in [
            ("foo/a", &first, 1),
            ("foo/b", &second, 2),
            ("foo/c", &third, 4),
        ] {
            assert_eq!(
                opens(&base, &joiner, &keys, key, version).await?,
                (epoch, true)
            );
        }

        // A member who joins after more than 16 raises gets the oldest epochs in a second batch.
        for epoch in 5..=18 {
            let (status, body) = send(http.post(&raise).bearer_auth(&owner)).await?;
            assert_eq!(
                (status, body["epoch"].as_u64()),
                (StatusCode::OK, Some(epoch))
            );
        }
        let fourth = put("foo/d").await?;
        let late = add_member(&seed, &owner, &group.group_id, Value::Null).await?;
        let late_private = SecretBytes::new(vec![14; 32]);
        add_key(
            &base,
            &late,
            "late-1",
            public_key_of(&late_private).unwrap(),
        )
        .await?;
        let (status, body) = send(http.post(&requests).bearer_auth(&late).json(&subtree)).await?;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        assert_eq!(
            body["fields"]["epochs"],
            json!((3..=18).collect::<Vec<u64>>())
        );
        issue_open(&base, &owner, &bucket_private).await?;
        issue_open(&base, &owner, &bucket_private).await?;
        let keys = user_keys(&base, &late, &late_private).await?;
        assert_eq!(keys.len(), 2);
        for (key, version, epoch) in [("foo/a", &first, 1), ("foo/d", &fourth, 18)] {
            assert_eq!(
                opens(&base, &late, &keys, key, version).await?,
                (epoch, true)
            );
        }

        // A request opened while locked and repeated after unlock gets every epoch from the node.
        let after = add_member(&seed, &owner, &group.group_id, Value::Null).await?;
        let after_private = SecretBytes::new(vec![15; 32]);
        let after_public = public_key_of(&after_private).unwrap();
        add_key(&base, &after, "after-1", after_public).await?;
        let (status, body) = send(http.post(&requests).bearer_auth(&after).json(&subtree)).await?;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        let unlock = format!("{encryption}/unlock?bucket_id={bucket_id}&generation={generation}");
        let response = http
            .post(unlock)
            .bearer_auth(&owner)
            .header("content-type", "application/octet-stream")
            .body(bucket_private.expose().to_vec())
            .send()
            .await?;
        assert!(response.status().is_success());
        let (status, body) = send(http.post(&requests).bearer_auth(&after).json(&subtree)).await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        let keys = user_keys(&base, &after, &after_private).await?;
        let epochs: std::collections::BTreeSet<u64> =
            keys.iter().flat_map(|(e, _, _)| e.clone()).collect();
        assert_eq!(epochs, (1..=18).collect());
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    seed.shutdown().await;
    result
}

/// The status and body of the envelope route for one version.
async fn envelope(
    base: &str,
    token: &str,
    key: &str,
    version: &str,
) -> TestResult<(StatusCode, Value)> {
    let route = format!("{base}/api/v1/data/blobs/envelope");
    let query = [("bucket", BUCKET), ("key", key), ("version_id", version)];
    send(
        reqwest::Client::new()
            .get(query_url(&route, &query)?)
            .bearer_auth(token),
    )
    .await
}

#[tokio::test]
async fn abe_rekey() -> TestResult<()> {
    let seed = spawn_complete_seed().await?;
    let result = async {
        let http = reqwest::Client::new();
        let base = seed.base_url.clone();
        let owner = create_bearer_token(
            seed.context.as_ref(),
            seed.user_id,
            seed.realm_id,
            seed.capabilities.clone(),
        )
        .await?;
        let group = create_group_http(&base, &owner, "ABE rekey").await?;
        let owner_private = SecretBytes::new(vec![7; 32]);
        let owner_public = public_key_of(&owner_private).unwrap();
        let owner_key = add_key(&base, &owner, "owner-1", owner_public).await?;
        let mut members = Vec::new();
        for (byte, name) in [(11u8, "reader-1"), (12, "leaver-1")] {
            let id = UserId::local(Ulid::generate(), seed.realm_id);
            grant_roles(&base, &owner, &group.group_id, id, Value::Null).await?;
            let context = seed.context.as_ref();
            let token =
                create_bearer_token(context, id, seed.realm_id, seed.capabilities.clone()).await?;
            let private = SecretBytes::new(vec![byte; 32]);
            add_key(&base, &token, name, public_key_of(&private).unwrap()).await?;
            members.push((id, token, private));
        }
        let [
            (_, reader, reader_private),
            (leaver_id, leaver, leaver_private),
        ] = <[_; 2]>::try_from(members).unwrap();
        let credentials = create_s3_credentials(&base, &owner, &group.group_id).await?;
        let s3 = s3_client(seed.s3.as_ref().unwrap(), &credentials);
        s3.create_bucket().bucket(BUCKET).send().await?;
        let encryption = format!("{base}/api/v1/data/buckets/{BUCKET}/storage/encryption");
        let (bucket_id, generation, bucket_private) =
            locked_key(&seed, &owner, &owner_key, &owner_private).await?;
        let key = BucketKeyRef::new(bucket_id, generation);
        let put = |key: &'static str| {
            let request = s3
                .put_object()
                .bucket(BUCKET)
                .key(key)
                .body(key.as_bytes().to_vec().into());
            async move {
                Ok::<_, Box<dyn std::error::Error>>(
                    request.send().await?.version_id().unwrap().to_string(),
                )
            }
        };
        let (a, b, other) = (
            put("foo/a").await?,
            put("foo/b").await?,
            put("bar/b").await?,
        );
        let lock = http.post(format!("{encryption}/lock")).bearer_auth(&owner);
        assert!(lock.send().await?.status().is_success());
        // A copy made while locked stays pending: outside the subtree, its source inside.
        let copied = s3
            .copy_object()
            .bucket(BUCKET)
            .key("bar/copy")
            .copy_source(format!("{BUCKET}/foo/a?versionId={a}"))
            .send()
            .await?;
        let copied = copied.version_id().unwrap().to_string();
        let pending = envelope(&base, &owner, "bar/copy", &copied).await?;
        assert_eq!(pending.0, StatusCode::CONFLICT, "{}", pending.1);
        let requests = format!("{base}/api/v1/data/buckets/{BUCKET}/abe/requests");
        let subtree = json!({"scope":{"kind":"subtree","value":"foo/"}});
        for token in [&reader, &leaver] {
            let (status, body) =
                send(http.post(&requests).bearer_auth(token).json(&subtree)).await?;
            assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        }
        issue_open(&base, &owner, &bucket_private).await?;
        let leaver_keys = user_keys(&base, &leaver, &leaver_private).await?;
        let (_, kept) = envelope(&base, &owner, "bar/b", &other).await?;
        let leave = format!(
            "{base}/api/v1/access/groups/{}/members/{leaver_id}",
            group.group_id
        );
        let left = http.delete(&leave).bearer_auth(&owner).send().await?;
        assert_eq!(left.status(), StatusCode::NO_CONTENT);

        // Unlocking without the wake keeps the copy pending while its source is re-keyed.
        let input = UnlockInput {
            bucket: BUCKET.into(),
            group_id: Ulid::from_string(&group.group_id)?,
            realm_id: seed.realm_id,
            node_id: seed.net.node_id(),
            caller: seed.user_id,
            key,
            duration: None,
            now_ms: aruna_core::time::unix_timestamp_millis(),
        };
        let secret = SharedSecret::new(SecretBytes::new(bucket_private.expose().to_vec()));
        drive(UnlockBucketOperation::new(input, secret), &seed.context).await?;
        // A page before the due raise covers no removal: the route raises and walks again.
        let (progress, done) = rekey_page(&seed.context, BUCKET, "foo/", 1).await?;
        assert_eq!((progress.epoch, progress.rekeyed, done), (1, 1, false));
        let abe = || async {
            let (status, body) = send(http.get(&encryption).bearer_auth(&owner)).await?;
            assert_eq!(status, StatusCode::OK, "{body}");
            Ok::<_, Box<dyn std::error::Error>>(body["abe"].clone())
        };
        let running = json!({"epoch":1,"raise_due":true,"rekey":{"prefix":"foo/","rekeyed":1}});
        assert_eq!(abe().await?, running);
        let rekey = format!("{base}/api/v1/data/buckets/{BUCKET}/abe/rekey");
        let prefix = json!({"prefix":"foo/"});
        let (status, _) = send(http.post(&rekey).bearer_auth(&reader).json(&prefix)).await?;
        assert_eq!(status, StatusCode::FORBIDDEN);
        let (status, view) = send(http.post(&rekey).bearer_auth(&owner).json(&prefix)).await?;
        assert_eq!(status, StatusCode::OK, "{view}");
        let expected = json!({"prefix":"foo/","epoch":2,"rekeyed":2,"done":true});
        assert_eq!(view, expected);
        assert_eq!(
            abe().await?,
            json!({"epoch":2,"raise_due":false,"rekey":null})
        );

        // A removed reader's old key opens no re-keyed envelope; a remaining reader's new one does.
        let reader_keys = user_keys(&base, &reader, &reader_private).await?;
        for (name, version) in [("foo/a", &a), ("foo/b", &b)] {
            let opened = opens(&base, &owner, &leaver_keys, name, version).await?;
            assert_eq!(opened, (2, false));
            let opened = opens(&base, &owner, &reader_keys, name, version).await?;
            assert_eq!(opened, (2, true));
        }
        assert_eq!(envelope(&base, &owner, "bar/b", &other).await?.1, kept);
        // The pending copy outside the subtree still reads the archive of its re-keyed source.
        let read_copy = || async {
            let read = s3
                .get_object()
                .bucket(BUCKET)
                .key("bar/copy")
                .send()
                .await?;
            Ok::<_, Box<dyn std::error::Error>>(read.body.collect().await?.into_bytes())
        };
        assert_eq!(read_copy().await?.as_ref(), b"foo/a");
        let origin = (seed.realm_id, seed.net.node_id());
        complete_copies(&seed.context, key, origin).await?;
        let completed = envelope(&base, &owner, "bar/copy", &copied).await?;
        assert_eq!(completed.0, StatusCode::OK, "{}", completed.1);
        assert_eq!(read_copy().await?.as_ref(), b"foo/a");

        // An interrupted pass resumes after its cursor; versions before it are not re-keyed again.
        let c = put("foo/c").await?;
        let (_, old_a) = envelope(&base, &owner, "foo/a", &a).await?;
        let (_, old_c) = envelope(&base, &owner, "foo/c", &c).await?;
        let (progress, done) = rekey_page(&seed.context, BUCKET, "foo/", 1).await?;
        assert_eq!((progress.rekeyed, done), (1, false));
        let (_, new_a) = envelope(&base, &owner, "foo/a", &a).await?;
        assert_ne!(new_a, old_a);
        let (status, view) = send(http.post(&rekey).bearer_auth(&owner).json(&prefix)).await?;
        assert_eq!(status, StatusCode::OK, "{view}");
        assert_eq!(
            (view["rekeyed"].as_u64(), view["done"].as_bool()),
            (Some(3), Some(true))
        );
        assert_eq!(envelope(&base, &owner, "foo/a", &a).await?.1, new_a);
        assert_ne!(envelope(&base, &owner, "foo/c", &c).await?.1, old_c);
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    seed.shutdown().await;
    result
}

/// Publishes `grant` as the seed owner by hand; `race` commits just before its transaction does.
async fn publish_racing(
    seed: &SeedNode,
    grant: KeyGrant,
    race: impl std::future::Future<Output = TestResult<()>>,
) -> TestResult<Result<KeyResult, KeyError>> {
    let auth = AuthContext {
        user_id: seed.user_id,
        realm_id: seed.realm_id,
        path_restrictions: None,
        session: None,
    };
    let now = aruna_core::time::unix_timestamp_millis();
    let action = KeyAction::Publish(grant);
    let mut operation = KeyOperation::new(BUCKET.into(), auth, seed.net.node_id(), action, now);
    let mut effects: std::collections::VecDeque<Effect> = operation.start().into_iter().collect();
    let mut race = Some(race);
    while let Some(effect) = effects.pop_front() {
        let Effect::Storage(effect) = effect else {
            panic!("unexpected effect {effect:?}")
        };
        if matches!(effect, StorageEffect::CommitTransaction { .. })
            && let Some(race) = race.take()
        {
            race.await?;
        }
        let event = seed
            .context
            .storage_handle
            .send_storage_effect(effect)
            .await;
        if !operation.is_complete() {
            effects.extend(operation.step(event));
        }
    }
    Ok(operation.finalize())
}

/// The epoch sets of the stored grants of `user` in the bucket `bucket_id`.
async fn grant_epochs(seed: &SeedNode, bucket_id: Ulid, user: UserId) -> Vec<Vec<u64>> {
    let iter = StorageEffect::Iter {
        key_space: aruna_core::keyspaces::ABE_GRANT_KEYSPACE.to_string(),
        prefix: Some(bucket_id.to_bytes().to_vec().into()),
        start: None,
        limit: MAX_REQUESTS,
        txn_id: None,
    };
    let event = seed.context.storage_handle.send_storage_effect(iter).await;
    let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
        panic!("unexpected storage event {event:?}")
    };
    let grants = values.iter().map(|(_, v)| KeyGrant::from_bytes(v).unwrap());
    let own = grants.filter(|g| g.context.request.recipient_user == user);
    own.map(|g| g.context.request.epochs).collect()
}

#[tokio::test]
async fn abe_merge_races() -> TestResult<()> {
    let seed = spawn_complete_seed().await?;
    let result = async {
        let http = reqwest::Client::new();
        let base = seed.base_url.clone();
        let owner = create_bearer_token(
            seed.context.as_ref(),
            seed.user_id,
            seed.realm_id,
            seed.capabilities.clone(),
        )
        .await?;
        let group = create_group_http(&base, &owner, "ABE merge races").await?;
        let owner_private = SecretBytes::new(vec![7; 32]);
        let owner_key = add_key(
            &base,
            &owner,
            "owner-1",
            public_key_of(&owner_private).unwrap(),
        )
        .await?;
        let mut readers = Vec::new();
        for (byte, name) in [(11u8, "leaver-1"), (12, "reader-1")] {
            let id = UserId::local(Ulid::generate(), seed.realm_id);
            grant_roles(&base, &owner, &group.group_id, id, Value::Null).await?;
            let context = seed.context.as_ref();
            let token =
                create_bearer_token(context, id, seed.realm_id, seed.capabilities.clone()).await?;
            let private = SecretBytes::new(vec![byte; 32]);
            add_key(&base, &token, name, public_key_of(&private).unwrap()).await?;
            readers.push((id, token, private));
        }
        let [(leaver_id, leaver, _), (reader_id, reader, reader_private)] =
            <[_; 2]>::try_from(readers).unwrap();
        let credentials = create_s3_credentials(&base, &owner, &group.group_id).await?;
        let s3 = s3_client(seed.s3.as_ref().unwrap(), &credentials);
        s3.create_bucket().bucket(BUCKET).send().await?;
        let encryption = format!("{base}/api/v1/data/buckets/{BUCKET}/storage/encryption");
        let (bucket_id, _, bucket_private) =
            locked_key(&seed, &owner, &owner_key, &owner_private).await?;
        let put = |key: &'static str| {
            let request = s3
                .put_object()
                .bucket(BUCKET)
                .key(key)
                .body(key.as_bytes().to_vec().into());
            async move {
                Ok::<_, Box<dyn std::error::Error>>(
                    request.send().await?.version_id().unwrap().to_string(),
                )
            }
        };
        let first = put("foo/a").await?;
        let lock = http.post(format!("{encryption}/lock")).bearer_auth(&owner);
        assert!(lock.send().await?.status().is_success());
        let requests = format!("{base}/api/v1/data/buckets/{BUCKET}/abe/requests");
        let subtree = json!({"scope":{"kind":"subtree","value":"foo/"}});
        for token in [&leaver, &reader] {
            let (status, body) =
                send(http.post(&requests).bearer_auth(token).json(&subtree)).await?;
            assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        }
        issue_open(&base, &owner, &bucket_private).await?;
        let raise = format!("{base}/api/v1/data/buckets/{BUCKET}/abe/epoch");
        let (status, body) = send(http.post(&raise).bearer_auth(&owner)).await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        // The reopened requests merge the epoch 1 grants when they are published.
        let listed = || async {
            let (status, list) = send(http.get(&requests).bearer_auth(&owner)).await?;
            assert_eq!(status, StatusCode::OK, "{list}");
            Ok::<_, Box<dyn std::error::Error>>(list)
        };
        let grant_of = |list: &Value, user: UserId| {
            let records = list["records"].as_array().unwrap().iter();
            let mut grants = records.map(|r| sealed_grant(&r["record"], &bucket_private).unwrap());
            grants
                .find(|g| g.context.request.recipient_user == user)
                .unwrap()
        };

        // A removal commits while the leaver's merge is in flight: the merge commits nothing.
        let leave = format!(
            "{base}/api/v1/access/groups/{}/members/{leaver_id}",
            group.group_id
        );
        let removal = async {
            let left = http.delete(&leave).bearer_auth(&owner).send().await?;
            assert_eq!(left.status(), StatusCode::NO_CONTENT);
            Ok(())
        };
        let grant = grant_of(&listed().await?, leaver_id);
        let published = publish_racing(&seed, grant, removal).await?;
        assert_eq!(published, Err(KeyError::Storage));
        assert_eq!(
            grant_epochs(&seed, bucket_id, leaver_id).await,
            vec![vec![1]]
        );

        // A raise consumes the due removal; the reader's reopened request merges epoch 1 too.
        let (status, body) = send(http.post(&raise).bearer_auth(&owner)).await?;
        assert_eq!((status, body["epoch"].as_u64()), (StatusCode::OK, Some(3)));
        let grant = grant_of(&listed().await?, reader_id);
        assert_eq!(grant.context.request.epochs, vec![1, 2, 3]);
        // A raise and its reissue commit while the reader's merge is in flight: the same holds.
        let reissue = async {
            let (status, body) = send(http.post(&raise).bearer_auth(&owner)).await?;
            assert_eq!((status, body["epoch"].as_u64()), (StatusCode::OK, Some(4)));
            Ok(())
        };
        let published = publish_racing(&seed, grant, reissue).await?;
        assert_eq!(published, Err(KeyError::Storage));
        assert_eq!(
            grant_epochs(&seed, bucket_id, reader_id).await,
            vec![vec![1]]
        );

        // The next issuance gives the reader one key over exactly the admitted epochs. The epoch 1
        // grant stays: it was issued under the group document before the removal.
        issue_open(&base, &owner, &bucket_private).await?;
        assert_eq!(
            grant_epochs(&seed, bucket_id, leaver_id).await,
            vec![vec![1]]
        );
        let held = grant_epochs(&seed, bucket_id, reader_id).await;
        assert_eq!(held, vec![vec![1], vec![1, 2, 3, 4]]);
        let mut keys = user_keys(&base, &reader, &reader_private).await?;
        keys.retain(|(epochs, _, _)| epochs.len() == 4);
        let third = put("foo/c").await?;
        let outside = put("bar/x").await?;
        for (key, version, expected) in [
            ("foo/a", &first, (1, true)),
            ("foo/c", &third, (4, true)),
            ("bar/x", &outside, (4, false)),
        ] {
            assert_eq!(opens(&base, &owner, &keys, key, version).await?, expected);
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    seed.shutdown().await;
    result
}

/// The object keys of the enumerated grants the caller holds.
async fn listed_writes(base: &str, token: &str) -> TestResult<Vec<String>> {
    let route = format!("{base}/api/v1/data/buckets/{BUCKET}/abe/grants");
    let (status, own) = send(reqwest::Client::new().get(route).bearer_auth(token)).await?;
    assert_eq!(status, StatusCode::OK, "{own}");
    let mut keys = Vec::new();
    for record in own["records"].as_array().unwrap() {
        let scope = &record["fields"]["request"]["scope"];
        assert_eq!(scope["kind"], json!("writes"), "{scope}");
        for write in scope["value"].as_array().unwrap() {
            keys.push(write["key"].as_str().unwrap().to_string());
        }
    }
    keys.sort();
    Ok(keys)
}

#[tokio::test]
async fn abe_enumerated() -> TestResult<()> {
    let seed = spawn_complete_seed().await?;
    let result = async {
        let http = reqwest::Client::new();
        let base = seed.base_url.clone();
        let owner = create_bearer_token(
            seed.context.as_ref(),
            seed.user_id,
            seed.realm_id,
            seed.capabilities.clone(),
        )
        .await?;
        let group = create_group_http(&base, &owner, "ABE enumerated").await?;
        let owner_private = SecretBytes::new(vec![7; 32]);
        let owner_public = public_key_of(&owner_private).unwrap();
        let owner_key = add_key(&base, &owner, "owner-1", owner_public).await?;
        // READ on foo/ and bulk/ with a DENY inside foo/: no continuing key fits.
        let group_ulid = Ulid::from_string(&group.group_id)?;
        let data = group_permission_path(seed.realm_id, group_ulid, seed.net.node_id());
        let permissions = std::collections::HashMap::from([
            (format!("{data}/{BUCKET}/foo/**"), "read"),
            (format!("{data}/{BUCKET}/bulk/**"), "read"),
            (format!("{data}/{BUCKET}/foo/private/**"), "deny"),
        ]);
        let roles = format!("{base}/api/v1/access/groups/{}/roles", group.group_id);
        let role = json!({"name":"enumerated","permissions":permissions});
        let (status, role) = send(http.post(&roles).bearer_auth(&owner).json(&role)).await?;
        assert_eq!(status, StatusCode::CREATED, "{role}");
        let reader_id = UserId::local(Ulid::generate(), seed.realm_id);
        let role_ids = json!([role["role_id"].clone()]);
        grant_roles(&base, &owner, &group.group_id, reader_id, role_ids).await?;
        let context = seed.context.as_ref();
        let reader =
            create_bearer_token(context, reader_id, seed.realm_id, seed.capabilities.clone())
                .await?;
        let reader_private = SecretBytes::new(vec![11; 32]);
        let reader_public = public_key_of(&reader_private).unwrap();
        add_key(&base, &reader, "reader-1", reader_public).await?;
        let credentials = create_s3_credentials(&base, &owner, &group.group_id).await?;
        let s3 = s3_client(seed.s3.as_ref().unwrap(), &credentials);
        s3.create_bucket().bucket(BUCKET).send().await?;
        let (bucket_id, _, bucket_private) = locked_key(&seed, &owner, &owner_key, &owner_private).await?;
        let put = |key: String| {
            let request = s3
                .put_object()
                .bucket(BUCKET)
                .key(&key)
                .body(key.as_bytes().to_vec().into());
            async move {
                Ok::<_, Box<dyn std::error::Error>>(
                    request.send().await?.version_id().unwrap().to_string(),
                )
            }
        };
        let a = put("foo/a".into()).await?;
        let b = put("foo/b".into()).await?;
        let private = put("foo/private/s".into()).await?;
        let requests = format!("{base}/api/v1/data/buckets/{BUCKET}/abe/requests");
        let request = |kind: &str, value: &str| {
            let body = json!({"scope":{"kind":kind,"value":value}});
            send(http.post(&requests).bearer_auth(&reader).json(&body))
        };

        // The continuing scope is refused and points to an enumerated grant.
        let (status, body) = request("subtree", "foo/").await?;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert_eq!(body["code"], json!("scope_unsupported"));
        assert!(body["error"].as_str().unwrap().contains("writes"), "{body}");
        let (status, body) = request("writes", "foo/").await?;
        assert_eq!((status, &body["fields"]), (StatusCode::OK, &json!({"request_ids":[]})));
        assert_eq!(listed_writes(&base, &reader).await?, ["foo/a", "foo/b"]);
        let keys = user_keys(&base, &reader, &reader_private).await?;
        assert_eq!(keys.len(), 1);
        for (key, version, opened) in [("foo/a", &a, true), ("foo/b", &b, true)] {
            assert_eq!(opens(&base, &owner, &keys, key, version).await?, (1, opened));
        }
        assert_eq!(
            opens(&base, &owner, &keys, "foo/private/s", &private).await?,
            (1, false)
        );
        // A write created later is not covered.
        let later = put("foo/c".into()).await?;
        assert_eq!(opens(&base, &owner, &keys, "foo/c", &later).await?, (1, false));

        // A CEL deny policy refuses continuing keys; the denied file leaves the enumerated grant.
        let policies = format!("{base}/api/v1/access/policies/group/{}", group.group_id);
        let expression = format!("user == '{reader_id}' && path.endsWith('/foo/b')");
        let policy = json!({"policies":[{"name":"no-b","kind":"deny","expression":expression,"enabled":true}]});
        let (status, body) = send(http.put(&policies).bearer_auth(&owner).json(&policy)).await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, body) = request("exact", "foo/a").await?;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        let (status, body) = request("writes", "foo/").await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(listed_writes(&base, &reader).await?, ["foo/a", "foo/c"]);
        let keys = user_keys(&base, &reader, &reader_private).await?;
        for (key, version, opened) in [("foo/a", &a, true), ("foo/b", &b, false), ("foo/c", &later, true)] {
            assert_eq!(opens(&base, &owner, &keys, key, version).await?, (1, opened));
        }

        // While locked the requests wait for a holder, who issues them like any other.
        let encryption = format!("{base}/api/v1/data/buckets/{BUCKET}/storage/encryption");
        let lock = http.post(format!("{encryption}/lock")).bearer_auth(&owner);
        assert!(lock.send().await?.status().is_success());
        put("foo/d".into()).await?;
        let (status, body) = request("writes", "foo/").await?;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        assert_eq!(body["fields"]["request_ids"].as_array().map(Vec::len), Some(1));
        issue_open(&base, &owner, &bucket_private).await?;
        // Each write keeps its own epoch; the identical epoch 1 grant was reused.
        let held = grant_epochs(&seed, bucket_id, reader_id).await;
        assert_eq!(held, vec![vec![1], vec![2]]);
        let listed = listed_writes(&base, &reader).await?;
        assert_eq!(listed, ["foo/a", "foo/c", "foo/d"]);

        // More readable files than one grant names are refused with the count, not truncated.
        for index in 0..=MAX_WRITES {
            put(format!("bulk/{index:02}")).await?;
        }
        let (status, body) = request("writes", "bulk/").await?;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
        assert_eq!(body["code"], json!("enumeration_limit"));
        assert!(body["error"].as_str().unwrap().contains("at least 1 more"), "{body}");
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    seed.shutdown().await;
    result
}
