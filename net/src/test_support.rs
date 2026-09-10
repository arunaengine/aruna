use aruna_core::alpn::Alpn;
use aruna_core::id::NodeId;
use iroh::SecretKey;

pub(crate) fn make_secret(seed: u8) -> SecretKey {
    let mut seed_bytes = [0u8; 32];
    seed_bytes[0] = seed;
    SecretKey::from_bytes(&seed_bytes)
}

pub(crate) fn make_node(seed: u8) -> NodeId {
    make_secret(seed).public()
}

pub(crate) fn make_repeated_node(seed: u8) -> NodeId {
    SecretKey::from_bytes(&[seed; 32]).public()
}

pub(crate) async fn test_endpoint(seed: u8) -> iroh::Endpoint {
    iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
        .secret_key(SecretKey::from_bytes(&[seed; 32]))
        .relay_mode(iroh::RelayMode::Disabled)
        .alpns(vec![Alpn::DocumentSync.as_bytes().to_vec()])
        .bind_addr(
            "127.0.0.1:0"
                .parse::<std::net::SocketAddr>()
                .expect("valid bind address"),
        )
        .expect("endpoint bind address configures")
        .bind()
        .await
        .expect("endpoint binds")
}

pub(crate) async fn unseeded_endpoint() -> iroh::Endpoint {
    iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
        .relay_mode(iroh::RelayMode::Disabled)
        .bind_addr(
            "127.0.0.1:0"
                .parse::<std::net::SocketAddr>()
                .expect("valid bind addr"),
        )
        .expect("valid bind addr")
        .bind()
        .await
        .expect("endpoint binds")
}
