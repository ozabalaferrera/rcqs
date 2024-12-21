use rcqs::{Catalog, Expiration};

#[test]
fn getters() {
    const ROOT_NAMESPACE: &str = "rcqs:testing";
    const NAME: &str = "catalog-getters";
    const EXPIRATION: Expiration = Expiration::Never;
    const TIMEOUT: Expiration = Expiration::Ttl(30);

    const CATALOG_KEY: &str = constcat::concat!(ROOT_NAMESPACE, ":", NAME);

    let catalog: Catalog<u32> = Catalog::new(
        ROOT_NAMESPACE.to_owned(),
        NAME.to_owned(),
        EXPIRATION,
        TIMEOUT,
    );

    assert_eq!(catalog.root_namespace(), ROOT_NAMESPACE);
    assert_eq!(catalog.name(), NAME);
    assert!(catalog.catalog_key().starts_with(CATALOG_KEY));
    assert!(catalog.catalog_expirations_key().starts_with(CATALOG_KEY));
    assert!(catalog.checkouts_expirations_key().starts_with(CATALOG_KEY));
    assert_eq!(catalog.default_ttl(), EXPIRATION);
    assert_eq!(catalog.default_timeout(), TIMEOUT);
}
