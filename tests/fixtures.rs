use rcqs::{Catalog, Expiration};
use std::sync::LazyLock;

pub static CATALOG_USE: LazyLock<Catalog<u32>> = LazyLock::new(catalog_for_using);
pub static CATALOG_DEL: LazyLock<Catalog<u32>> = LazyLock::new(catalog_for_deleting);

fn catalog_for_using() -> Catalog<u32> {
    Catalog::new(
        "rcqs:testing".to_owned(),
        "for-using".to_owned(),
        Expiration::Ttl(60),
        Expiration::Ttl(30),
    )
}

fn catalog_for_deleting() -> Catalog<u32> {
    Catalog::new(
        "rcqs:testing".to_owned(),
        "for-deleting".to_owned(),
        Expiration::Ttl(60),
        Expiration::Ttl(30),
    )
}
