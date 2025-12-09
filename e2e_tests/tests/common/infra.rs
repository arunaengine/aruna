use crate::common::init::{
    init_keycloak_container_static, init_meili_container_static, init_minio_container_static,
    init_nats_container_static, init_postgres_container_static,
};
use maybe_once::tokio::{Data, MaybeOnceAsync};
use std::sync::OnceLock;
use testcontainers::{ContainerAsync, GenericImage};

/// A function that holds a static reference to the
/// `MaybeOnceAsync` object and returns a `Data`
/// object.
pub async fn keycloak(serial: bool) -> Data<'static, ContainerAsync<GenericImage>> {
    static DATA: OnceLock<MaybeOnceAsync<ContainerAsync<GenericImage>>> = OnceLock::new();

    DATA.get_or_init(|| MaybeOnceAsync::new(|| Box::pin(init_keycloak_container_static())))
        .data(serial)
        .await
}

/// A function that holds a static reference to the
/// `MaybeOnceAsync` object and returns a `Data`
/// object.
pub async fn postgres(serial: bool) -> Data<'static, ContainerAsync<GenericImage>> {
    static DATA: OnceLock<MaybeOnceAsync<ContainerAsync<GenericImage>>> = OnceLock::new();

    DATA.get_or_init(|| MaybeOnceAsync::new(|| Box::pin(init_postgres_container_static())))
        .data(serial)
        .await
}

/// A function that holds a static reference to the
/// `MaybeOnceAsync` object and returns a `Data`
/// object.
pub async fn minio(serial: bool) -> Data<'static, ContainerAsync<GenericImage>> {
    static DATA: OnceLock<MaybeOnceAsync<ContainerAsync<GenericImage>>> = OnceLock::new();

    DATA.get_or_init(|| MaybeOnceAsync::new(|| Box::pin(init_minio_container_static())))
        .data(serial)
        .await
}

/// A function that holds a static reference to the
/// `MaybeOnceAsync` object and returns a `Data`
/// object.
pub async fn nats(serial: bool) -> Data<'static, ContainerAsync<GenericImage>> {
    static DATA: OnceLock<MaybeOnceAsync<ContainerAsync<GenericImage>>> = OnceLock::new();

    DATA.get_or_init(|| MaybeOnceAsync::new(|| Box::pin(init_nats_container_static())))
        .data(serial)
        .await
}

/// A function that holds a static reference to the
/// `MaybeOnceAsync` object and returns a `Data`
/// object.
pub async fn meili(serial: bool) -> Data<'static, ContainerAsync<GenericImage>> {
    static DATA: OnceLock<MaybeOnceAsync<ContainerAsync<GenericImage>>> = OnceLock::new();

    DATA.get_or_init(|| MaybeOnceAsync::new(|| Box::pin(init_meili_container_static())))
        .data(serial)
        .await
}
