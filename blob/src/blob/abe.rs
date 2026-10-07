//! Executes ABE cryptography and admits object keys without installing bucket keys.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::BlobHandler;
use aruna_core::errors::BlobError;
use aruna_core::events::BlobEvent;
use aruna_core::structs::storage::abe::{
    AbeEffect, AbeEvent, check_object, create_envelope, create_parameters,
};

impl BlobHandler {
    pub(super) async fn abe_effect(&self, effect: AbeEffect) -> BlobEvent {
        match effect {
            AbeEffect::Issue(context) => {
                let secret = match self.unlocks.lock() {
                    Ok(mut registry) => registry
                        .unlocked_key(context.request.parameters.key, std::time::Instant::now()),
                    Err(_) => return BlobEvent::Error(BlobError::HandleMissing),
                };
                let (secret, _) = match secret {
                    Ok(value) => value,
                    Err(error) => return BlobEvent::Error(error.into()),
                };
                let result = (|| {
                    let r = &context.request;
                    let parameters = r.parameters.public()?;
                    let master = r.parameters.recompute(secret.bytes())?;
                    let policy = r.scope.policy(&r.parameters, &r.epochs)?;
                    let key = aruna_kpabe::issue(
                        &parameters,
                        &master,
                        &policy,
                        &mut aruna_core::structs::storage::abe::SysRng,
                    )?;
                    let aad = context.bytes()?;
                    let recipient = r
                        .recipient_public
                        .ok_or(aruna_core::structs::storage::abe::AbeError::Stale)?;
                    let sealed = key.seal(|plain| {
                        aruna_core::key_seal::seal_to(
                            &recipient,
                            aruna_core::structs::storage::abe::GRANT_PURPOSE,
                            &aad,
                            plain,
                        )
                        .map_err(|_| aruna_kpabe::Error)
                    })?;
                    Ok::<_, aruna_core::structs::storage::abe::AbeError>(
                        aruna_core::structs::storage::abe_access::KeyGrant {
                            context,
                            enc: sealed.enc,
                            ciphertext: sealed.ciphertext,
                        },
                    )
                })();
                match result {
                    Ok(grant) => BlobEvent::Abe(Box::new(AbeEvent::Grant(grant))),
                    Err(error) => BlobEvent::Error(error.into()),
                }
            }
            AbeEffect::Parameters {
                realm,
                node,
                key,
                private,
            } => match create_parameters(private.bytes(), realm, node, key) {
                Ok(value) => BlobEvent::Abe(Box::new(AbeEvent::Parameters(value))),
                Err(error) => BlobEvent::Error(error.into()),
            },
            AbeEffect::Write {
                plan,
                resolved,
                bucket,
                key,
                created_by,
                blob,
                size,
            } => {
                let (envelope, private) = match create_envelope(plan) {
                    Ok(value) => value,
                    Err(error) => return BlobEvent::Error(error.into()),
                };
                drop(private);
                let object = envelope.context.public_key;
                match self
                    .write_granted_blob(
                        (&bucket, &key),
                        resolved,
                        created_by,
                        blob,
                        size,
                        (None, Some(object)),
                    )
                    .await
                {
                    BlobEvent::WriteFinished { location } => {
                        BlobEvent::Abe(Box::new(AbeEvent::Written { location, envelope }))
                    }
                    event => event,
                }
            }
            AbeEffect::Envelope(plan) => match create_envelope(plan) {
                Ok((envelope, _)) => BlobEvent::Abe(Box::new(AbeEvent::Envelope(envelope))),
                Err(error) => BlobEvent::Error(error.into()),
            },
            AbeEffect::Admit {
                envelope,
                archive,
                private,
            } => {
                if let Err(error) = check_object(private.bytes(), &envelope) {
                    return BlobEvent::Error(error.into());
                }
                let slots = match self.unlocks.lock() {
                    Ok(registry) => registry.lease_slots(),
                    Err(_) => return BlobEvent::Error(BlobError::HandleMissing),
                };
                let Ok(slot) = slots.acquire_owned().await else {
                    return BlobEvent::Error(BlobError::Closed);
                };
                let lease = match self.unlocks.lock() {
                    Ok(registry) => registry.token_lease(
                        envelope.context.parameters.key,
                        archive,
                        private,
                        slot,
                    ),
                    Err(_) => Err(BlobError::HandleMissing),
                };
                match lease {
                    Ok(lease) => BlobEvent::ReadAdmitted { lease },
                    Err(error) => BlobEvent::Error(error),
                }
            }
        }
    }
}
