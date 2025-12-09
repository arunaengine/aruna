use crate::caching::structs::PubKeyEnum;
use anyhow::{anyhow, bail, Result};
use chrono::{NaiveDateTime, Utc};
use jsonwebtoken::{decode_header, jwk::JwkSet, DecodingKey};
use std::fmt::Debug;

use super::token_handler::ArunaTokenClaims;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum IssuerType {
    ARUNA,
    DATAPROXY,
    OIDC,
}

pub struct Issuer {
    pub issuer_name: String,
    pub pubkey_endpoint: Option<String>,
    pub decoding_keys: Vec<(String, DecodingKey)>,
    pub last_updated: NaiveDateTime,
    pub audiences: Option<Vec<String>>,
    pub issuer_type: IssuerType,
}

impl Debug for Issuer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.issuer_name)?;
        f.write_fmt(format_args!("{:?}", self.pubkey_endpoint))?;
        self.decoding_keys.iter().for_each(|(k, _)| {
            f.write_fmt(format_args!("{k} - DecodingKey")).unwrap();
        });
        f.write_fmt(format_args!("{:?}", self.last_updated))?;
        f.write_fmt(format_args!("{:?}", self.audiences))?;
        f.write_fmt(format_args!("{:?}", self.issuer_type))
    }
}

impl Issuer {
    pub async fn new_with_endpoint(
        issuer_name: String,
        pubkey_endpoint: String,
        audiences: Option<Vec<String>>,
    ) -> Result<Self> {
        let (decoding_keys, last_updated) = Self::fetch_jwks(&pubkey_endpoint).await?;
        Ok(Self {
            issuer_name,
            pubkey_endpoint: Some(pubkey_endpoint),
            decoding_keys,
            last_updated,
            audiences,
            issuer_type: IssuerType::OIDC,
        })
    }

    pub async fn new_with_keys(
        issuer_name: String,
        decoding_keys: Vec<(String, DecodingKey)>,
        audiences: Option<Vec<String>>,
        issuer_type: IssuerType,
    ) -> Result<Self> {
        Ok(Self {
            issuer_name,
            pubkey_endpoint: None,
            decoding_keys,
            last_updated: Utc::now().naive_utc(),
            audiences,
            issuer_type,
        })
    }

    pub async fn fetch_jwks(endpoint: &str) -> Result<(Vec<(String, DecodingKey)>, NaiveDateTime)> {
        let client = reqwest::Client::new();
        let res = client.get(endpoint).send().await?;
        let jwks: JwkSet = res.json().await?;

        Ok((
            jwks.keys
                .iter()
                .filter_map(|jwk| {
                    let key = DecodingKey::from_jwk(jwk).ok()?;
                    Some((jwk.common.clone().key_id?, key))
                })
                .collect::<Vec<_>>(),
            Utc::now().naive_utc(),
        ))
    }

    pub async fn refresh_jwks(&mut self) -> Result<()> {
        if self.last_updated
            + chrono::Duration::try_minutes(5).ok_or_else(|| anyhow!("time conversion failed"))?
            > Utc::now().naive_utc()
        {
            bail!("JWKS was updated less than 5 minutes ago");
        }

        if self.issuer_type != IssuerType::OIDC {
            bail!("Only OIDC issuers can refresh JWKS");
        }
        let (decodings_keys, last_updated) = Self::fetch_jwks(
            self.pubkey_endpoint
                .as_ref()
                .ok_or_else(|| anyhow!("Invalid endpoint type"))?,
        )
        .await?;
        self.decoding_keys = decodings_keys;
        self.last_updated = last_updated;
        Ok(())
    }

    pub fn find(&self, kid: &str) -> Option<&DecodingKey> {
        self.decoding_keys
            .iter()
            .find(|(key_id, _)| key_id == kid)
            .map(|(_, key)| key)
    }

    pub async fn check_token(&self, token: &str) -> Result<(String, ArunaTokenClaims)> {
        let kid = decode_header(token)?
            .kid
            .ok_or_else(|| anyhow!("No kid in header"))?;
        match self.find(&kid) {
            Some(decoding_key) => Ok((
                kid,
                Self::get_validate_claims(token, decoding_key, &self.audiences)?,
            )),
            None => {
                bail!("No matching key found");
            }
        }
    }

    pub fn get_validate_claims(
        token: &str,
        decoding_key: &DecodingKey,
        audiences: &Option<Vec<String>>,
    ) -> Result<ArunaTokenClaims> {
        let header = decode_header(token)?;
        let alg = header.alg;
        let mut validation = jsonwebtoken::Validation::new(alg);
        if let Some(aud) = audiences {
            validation.set_audience(aud)
        };
        let tokendata = jsonwebtoken::decode::<ArunaTokenClaims>(token, decoding_key, &validation)?;
        Ok(tokendata.claims)
    }
}

pub async fn convert_to_pubkeys_issuers(pubkeys: &Vec<(i16, PubKeyEnum)>) -> Result<Vec<Issuer>> {
    let mut server_encoding_keys = vec![];
    let mut issuers = vec![];

    for (id, pubkey) in pubkeys {
        match pubkey {
            PubKeyEnum::DataProxy((_, dec_key, key)) => {
                let issuer = Issuer::new_with_keys(
                    key.to_string(),
                    vec![(id.to_string(), dec_key.clone())],
                    Some(vec!["aruna".to_string()]),
                    IssuerType::DATAPROXY,
                )
                .await?;
                issuers.push(issuer);
            }
            PubKeyEnum::Server((_, dec_key)) => {
                server_encoding_keys.push((id.to_string(), dec_key.clone()));
            }
        }
    }
    issuers.push(
        Issuer::new_with_keys(
            "aruna".to_string(),
            server_encoding_keys,
            Some(vec!["aruna".to_string()]),
            IssuerType::ARUNA,
        )
        .await?,
    );
    Ok(issuers)
}
