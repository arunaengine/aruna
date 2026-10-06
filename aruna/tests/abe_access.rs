//! Exercises holder-issued scoped reads over REST while the bucket stays locked.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#![recursion_limit = "256"]
mod shared;

use aruna_core::UserId;
use aruna_core::compute::SecretBytes;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::key_seal::{SealedSecret, open_sealed, seal_to};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction, Permission};
use aruna_core::structs::storage::abe::{
    AbeError, GRANT_PURPOSE, SysRng, create_parameters, derive_master, setup_context,
};
use aruna_core::structs::storage::abe_access::{GrantContext, KeyGrant, KeyScope, MAX_REQUESTS};
use aruna_core::structs::storage::blob::group_permission_path;
use aruna_core::structs::storage::encryption::{BucketKeyRef, copy_info, public_key_of};
use aruna_kpabe::{Attribute, Envelope, Policy, UserKey};
use aruna_operations::abe::{KeyAction, KeyError, KeyOperation};
use base64::{Engine, engine::general_purpose::STANDARD};
use reqwest::StatusCode;
use serde_json::{Value, json};
use shared::{
    SeedNode, TestResult, create_bearer_token, create_group_http, create_s3_credentials, s3_client,
    sign_scoped_token, sign_token, spawn_complete_seed,
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

        // A copied version has no envelope and a missing version is not found.
        let content = format!("{base}/api/v1/data/blobs/content");
        let envelope_route = format!("{base}/api/v1/data/blobs/envelope");
        let copy_query = [("bucket", BUCKET), ("key", "foo/copy"), ("version_id", &copy_version)];
        let (status, body) = send(http.get(query_url(&content, &copy_query)?).bearer_auth(&reader)).await?;
        assert_eq!((status, body["code"].as_str()), (StatusCode::CONFLICT, Some("envelope_pending")));
        let missing = Ulid::generate().to_string();
        let missing_query = [("bucket", BUCKET), ("key", "foo/data"), ("version_id", &missing)];
        let (status, _) = send(http.get(query_url(&envelope_route, &missing_query)?).bearer_auth(&reader)).await?;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // Holders see the reader's request; the reader is no issuer.
        let (status, pending) = send(http.post(&requests).bearer_auth(&reader).json(&exact)).await?;
        assert_eq!(status, StatusCode::ACCEPTED);
        let request_id = pending["fields"]["request_id"].as_str().unwrap().to_string();
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

        // A grant by a non-holder notifies each holder once per member and bucket.
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
            || async { !pending().await.is_empty() },
        )
        .await?;
        let notices = pending().await;
        assert_eq!(notices.len(), 1, "{notices:?}");
        assert_eq!(notices[0]["bucket"], vault);
        assert_eq!(notices[0]["member_user_id"], second.to_string());
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    seed.shutdown().await;
    result
}
