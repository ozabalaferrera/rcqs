use rcqs::{Catalog, CatalogItem, Expiration};
use redis::Client;
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;
use uuid::Uuid;

pub fn redis_client() -> Client {
    let url = format!("redis://{}:{}/", env!("REDIS_HOST"), env!("REDIS_PORT"));
    redis::Client::open(url).expect("valid redis url")
}

pub fn random_catalog<T>() -> Catalog<T>
where
    T: Debug + Serialize + DeserializeOwned,
{
    Catalog::new(
        "rcqs:testing".to_owned(),
        Uuid::new_v4().to_string(),
        Expiration::Ttl(60),
        Expiration::Ttl(30),
    )
}

pub fn random_item() -> CatalogItem<String> {
    CatalogItem::new(Uuid::new_v4().to_string())
}

pub fn random_item_with_expiration(expiration: Expiration) -> CatalogItem<String> {
    CatalogItem::new_with_expiration(expiration, Uuid::new_v4().to_string())
}
