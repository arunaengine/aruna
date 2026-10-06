//! Exercises holder-issued scoped reads over REST while the bucket stays locked.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#![recursion_limit = "256"]
mod shared;

use aruna_core::compute::SecretBytes;
use aruna_core::key_seal::{SealedSecret, open_sealed, seal_to};
use aruna_core::structs::storage::abe::{
    AbeError, GRANT_PURPOSE, SystemRng, create_parameters, derive_master, setup_context,
};
use aruna_core::structs::storage::abe_access::{GrantContext, KeyGrant, KeyScope};
use aruna_core::structs::storage::encryption::{BucketKeyRef, copy_info, public_key_of};
use aruna_kpabe::{Attribute, Envelope, Policy, UserKey};
use base64::{Engine, engine::general_purpose::STANDARD};
use reqwest::StatusCode;
use serde_json::{Value, json};
use shared::{
    TestResult, create_bearer_token, create_group_http, create_s3_credentials, s3_client,
    spawn_complete_seed,
};
use ulid::Ulid;

const BUCKET: &str = "abe-access";
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
    let user = aruna_kpabe::issue(&parameters, &master, &policy, &mut SystemRng).unwrap();
    for epoch in [1, 2] {
        let attrs = [
            Attribute::Domain(a.domain().unwrap()),
            Attribute::Epoch(epoch),
            Attribute::Prefix(b"foo/".to_vec()),
        ];
        let sealed =
            aruna_kpabe::seal(&parameters, &attrs, &[8; 32], b"envelope", &mut SystemRng).unwrap();
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
        let response = client.post(format!("{encryption}/lock")).bearer_auth(&token).send().await?;
        assert!(response.status().is_success());
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
        let key = aruna_kpabe::issue(&parameters,&master,&policy,&mut SystemRng)?;
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
        let _ = group;
        Ok::<(),Box<dyn std::error::Error>>(())
    }.await;
    seed.shutdown().await;
    result
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
