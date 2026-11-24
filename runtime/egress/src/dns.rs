//! Reads the TXT values and addresses a custom domain check compares.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::outbound::{DnsAnswer, DnsQuery};
use iroh::dns::DnsResolver;

/// How long one lookup may take before its name counts as not resolving.
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(5);

/// Asks the system resolver; a name that fails to resolve answers an empty list.
pub(crate) async fn lookup(resolver: &DnsResolver, query: DnsQuery) -> DnsAnswer {
    let txt = match resolver.lookup_txt(&query.txt, LOOKUP_TIMEOUT).await {
        Ok(records) => records
            .map(|record| {
                let parts: Vec<u8> = record.iter().flatten().copied().collect();
                String::from_utf8_lossy(&parts).into_owned()
            })
            .collect(),
        Err(_) => Vec::new(),
    };
    let mut addresses = Vec::with_capacity(query.hosts.len());
    for host in &query.hosts {
        let found = resolver.lookup_ipv4_ipv6(host, LOOKUP_TIMEOUT).await;
        addresses.push(found.map(Iterator::collect).unwrap_or_default());
    }
    DnsAnswer { txt, addresses }
}
