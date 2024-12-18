use rcqs::{Catalog, CatalogItem, Expiration};
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;
use uuid::Uuid;

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
